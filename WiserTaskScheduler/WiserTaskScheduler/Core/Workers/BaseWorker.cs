﻿using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;
using WiserTaskScheduler.Modules.RunSchemes.Models;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Core.Workers;

/// <summary>
/// The <see cref="BaseWorker"/> performs shared functionality for all workers.
/// </summary>
public abstract class BaseWorker(IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : BackgroundService
{
    /// <summary>
    /// Gets the name of the worker.
    /// </summary>
    protected string Name { get; private set; }

    /// <summary>
    /// Gets the delay between two runs of the worker.
    /// </summary>
    public RunSchemeModel RunScheme { get; private set; }

    private bool RunImmediately { get; set; }

    private bool SingleRun { get; set; }

    private string ConfigurationName { get; set; }

    private readonly ILogService logService = baseWorkerDependencyAggregate.LogService;
    private readonly ILogger<BaseWorker> logger = baseWorkerDependencyAggregate.Logger;
    private readonly IRunSchemesService runSchemesService = baseWorkerDependencyAggregate.RunSchemesService;
    private readonly IWiserDashboardService wiserDashboardService = baseWorkerDependencyAggregate.WiserDashboardService;
    private readonly IErrorNotificationService errorNotificationService = baseWorkerDependencyAggregate.ErrorNotificationService;
    private readonly WtsSettings wtsSettings = baseWorkerDependencyAggregate.WtsSettings;

    private bool defaultServiceIsCreated;
    private string serviceFailedNotificationEmails;

    /// <summary>
    /// Assigns the base values from a derived class.
    /// </summary>
    /// <param name="name">The name of the worker.</param>
    /// <param name="runScheme">The run scheme of the worker.</param>
    /// <param name="serviceFailedNotificationEmailsValue">The e-mail addresses to send error notifications to.</param>
    /// <param name="runImmediately">True to run the action immediately, false to run at first delayed time.</param>
    /// <param name="configurationName">The name of the configuration, default <see langword="null"/>. If set it will update the service information.</param>
    /// <param name="singleRun">The configuration is only run once, ignoring paused state and run time.</param>
    protected void Initialize(string name, RunSchemeModel runScheme, string serviceFailedNotificationEmailsValue, bool runImmediately = false, string configurationName = null, bool singleRun = false)
    {
        if (!String.IsNullOrWhiteSpace(Name))
        {
            return;
        }

        Name = $"{name} ({Environment.MachineName} - {wtsSettings.Name})";
        RunScheme = runScheme;
        RunImmediately = runImmediately;
        ConfigurationName = configurationName;
        SingleRun = singleRun;
        serviceFailedNotificationEmails = serviceFailedNotificationEmailsValue;
    }

    /// <summary>
    /// Handles when the worker is allowed to execute its action.
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            if (String.IsNullOrWhiteSpace(ConfigurationName) && !defaultServiceIsCreated)
            {
                var existingService = await wiserDashboardService.GetServiceAsync(Name, 0);
                if (existingService == null)
                {
                    await wiserDashboardService.CreateServiceAsync(Name, 0);
                    await wiserDashboardService.UpdateServiceAsync(Name, 0, state: "active", templateId: 0);
                    defaultServiceIsCreated = true;
                }
            }

            await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} started, first run on: {runSchemesService.GetDateTimeTillNextRun(RunScheme)}", ConfigurationName ?? Name, RunScheme.TimeId);
            await wiserDashboardService.UpdateServiceAsync(ConfigurationName ?? Name, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme));

            if (!RunImmediately && !SingleRun)
            {
                await WaitTillNextRun(stoppingToken);
            }

            var updateSucceeded = true;
            while (!stoppingToken.IsCancellationRequested)
            {
                var state = "success";
                var stopWatch = new Stopwatch();
                bool? paused = false;
                bool? alreadyRunning = false;

                // Only check running and pause state for custom configurations because the default services are always running for each instance of the WTS.
                if (!String.IsNullOrWhiteSpace(ConfigurationName))
                {
                    alreadyRunning = await wiserDashboardService.IsServiceRunning(ConfigurationName, RunScheme.TimeId);
                    paused = await wiserDashboardService.IsServicePaused(ConfigurationName, RunScheme.TimeId);
                    // Only save paused state if we know for sure it is paused and not currently running (if it is running, it will be set to paused by that thread after the execution).
                    if (paused.HasValue && paused.Value && alreadyRunning.HasValue && !alreadyRunning.Value)
                    {
                        await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, state: "paused");
                    }
                }

                // Skip if already running or we don't know if it's running, except when we know the previous state sync failed.
                if ((alreadyRunning.HasValue && !alreadyRunning.Value) || !updateSucceeded)
                {
                    // Also skip if paused, or we don't know if it's paused, except when it's a manual run.
                    if ((paused.HasValue && !paused.Value) || SingleRun)
                    {
                        await logService.LogInformation(logger, LogScopes.RunStartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} started at: {DateTime.Now}", ConfigurationName ?? Name, RunScheme.TimeId);
                        await wiserDashboardService.UpdateServiceAsync(ConfigurationName ?? Name, RunScheme.TimeId, state: "running");

                        var runStartTime = DateTime.Now;
                        stopWatch.Start();

                        await ExecuteActionAsync(stoppingToken);

                        stopWatch.Stop();

                        await logService.LogInformation(logger, LogScopes.RunStartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} finished at: {DateTime.Now}, time taken: {stopWatch.Elapsed}", ConfigurationName ?? Name, RunScheme.TimeId);

                        paused = await wiserDashboardService.IsServicePaused(ConfigurationName ?? Name, RunScheme.TimeId);
                        // Only set state to paused if we are sure it's paused.
                        if (paused.HasValue && paused.Value)
                        {
                            state = "paused";
                        }
                        else
                        {
                            var logStateFromLastRun = logService.GetLogLevelOfService(ConfigurationName ?? Name, RunScheme.TimeId).ToString();
                            if (logStateFromLastRun.Equals("None", StringComparison.OrdinalIgnoreCase))
                            {
                                state = "unknown";
                            }
                            else if (logStateFromLastRun.Equals("Critical", StringComparison.OrdinalIgnoreCase) || logStateFromLastRun.Equals("Error", StringComparison.OrdinalIgnoreCase))
                            {
                                state = "failed";
                            }
                            else if (logStateFromLastRun.Equals("Warning", StringComparison.OrdinalIgnoreCase))
                            {
                                state = "warning";
                            }

                            // Clear the log level of the service after the run to prevent an old state from being used.
                            logService.ClearLogLevelOfService(ConfigurationName ?? Name, RunScheme.TimeId);
                        }

                        // If storing the state of this run fails, the state will stay on "running", which will prevent any future runs, so keep track of the success.
                        updateSucceeded = await wiserDashboardService.UpdateServiceAsync(ConfigurationName ?? Name, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme), lastRun: DateTime.Now, runTime: stopWatch.Elapsed, state: state, extraRun: false);
                        if (!updateSucceeded)
                        {
                            await errorNotificationService.NotifyOfErrorByEmailAsync(serviceFailedNotificationEmails, $"Service '{ConfigurationName ?? Name}'{(RunScheme.TimeId > 0 ? $" with time ID '{RunScheme.TimeId}'" : "")} of Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' status save failed.", $"Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' failed to save the service '{ConfigurationName ?? Name}'{(RunScheme.TimeId > 0 ? $" with time ID '{RunScheme.TimeId}'" : "")} state of the last run, which is '{state}', to the database. The service will continue running and the next run will attempt a new state save. Until then, the service state in the database will be incorrect.", RunScheme.LogSettings, LogScopes.RunBody, ConfigurationName ?? Name);
                        }
                    }
                    else
                    {
                        await wiserDashboardService.UpdateServiceAsync(ConfigurationName ?? Name, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme));
                    }
                }

                // If the configuration only needs to be run once break out of the while loop. State will not be set on stopped because the normal configuration is still active.
                if (SingleRun)
                {
                    // If storing the result of this run failed the first time, try again now, otherwise the state will stay on running and it will never run again.
                    if (!updateSucceeded)
                    {
                        updateSucceeded = await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme), lastRun: DateTime.Now, runTime: stopWatch.Elapsed, state: state, extraRun: false);
                        if (!updateSucceeded)
                        {
                            await errorNotificationService.NotifyOfErrorByEmailAsync(serviceFailedNotificationEmails, $"Service '{ConfigurationName ?? Name}'{(RunScheme.TimeId > 0 ? $" with time ID '{RunScheme.TimeId}'" : "")} of Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' status save failed.", $"Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' failed to save the service '{ConfigurationName ?? Name}'{(RunScheme.TimeId > 0 ? $" with time ID '{RunScheme.TimeId}'" : "")} state of the last run, which is '{state}', to the database a second time. Since this was a single run configuration, no more attempts will be made to correct this. The service state will now permanently be incorrect until it is manually updated.", RunScheme.LogSettings, LogScopes.RunStartAndStop, ConfigurationName ?? Name);
                        }
                    }

                    break;
                }

                await WaitTillNextRun(stoppingToken);
            }
        }
        catch (TaskCanceledException)
        {
            await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} has been stopped after cancel was called.", ConfigurationName ?? Name, RunScheme.TimeId);
            await wiserDashboardService.UpdateServiceAsync(ConfigurationName ?? Name, RunScheme.TimeId, state: "stopped");
        }
        catch (Exception exception)
        {
            await logService.LogCritical(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} stopped with exception {exception}", ConfigurationName ?? Name, RunScheme.TimeId);
            await wiserDashboardService.UpdateServiceAsync(ConfigurationName ?? Name, RunScheme.TimeId, state: "crashed");
            await errorNotificationService.NotifyOfErrorByEmailAsync(serviceFailedNotificationEmails, $"Service '{ConfigurationName ?? Name}'{(RunScheme.TimeId > 0 ? $" with time ID '{RunScheme.TimeId}'" : "")} of Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' crashed.", $"Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' crashed while executing the service '{ConfigurationName ?? Name}'{(RunScheme.TimeId > 0 ? $" with time ID '{RunScheme.TimeId}'" : "")} and is therefore shutdown. Please check the logs for more details. A restart is required to start the service again.", RunScheme.LogSettings, LogScopes.StartAndStop, ConfigurationName ?? Name);
        }
    }

    /// <summary>
    /// Wait till the next run, if the time to wait is longer than the allowed delay the time is split and will wait in sections until the full time has completed.
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    private async Task WaitTillNextRun(CancellationToken stoppingToken)
    {
        await TaskHelpers.WaitAsync(runSchemesService.GetTimeTillNextRun(RunScheme), stoppingToken);
    }

    /// <summary>
    /// Execute the action of the derived worker.
    /// </summary>
    /// <param name="stoppingToken">The <see cref="CancellationToken"/> used for the current background service to indicate if it is being stopped.</param>
    /// <returns></returns>
    protected abstract Task ExecuteActionAsync(CancellationToken stoppingToken);
}