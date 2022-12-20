﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;
using WiserTaskScheduler.Modules.RunSchemes.Models;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Core.Workers
{
    /// <summary>
    /// The <see cref="BaseWorker"/> performs shared functionality for all workers.
    /// </summary>
    public abstract class BaseWorker : BackgroundService
    {
        /// <summary>
        /// Gets the name of the worker.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the delay between two runs of the worker.
        /// </summary>
        public RunSchemeModel RunScheme { get; private set; }

        private bool RunImmediately { get; set; }

        private bool SingleRun { get; set; }

        private string ConfigurationName { get; set; }

        private readonly ILogService logService;
        private readonly ILogger<BaseWorker> logger;
        private readonly IRunSchemesService runSchemesService;
        private readonly IWiserDashboardService wiserDashboardService;

        /// <summary>
        /// Creates a new instance of <see cref="BaseWorker"/>.
        /// </summary>
        /// <param name="baseWorkerDependencyAggregate"></param>
        protected BaseWorker(IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate)
        {
            logService = baseWorkerDependencyAggregate.LogService;
            logger = baseWorkerDependencyAggregate.Logger;
            runSchemesService = baseWorkerDependencyAggregate.RunSchemesService;
            wiserDashboardService = baseWorkerDependencyAggregate.WiserDashboardService;
        }

        /// <summary>
        /// Assigns the base values from a derived class.
        /// </summary>
        /// <param name="name">The name of the worker.</param>
        /// <param name="runScheme">The run scheme of the worker.</param>
        /// <param name="runImmediately">True to run the action immediately, false to run at first delayed time.</param>
        /// <param name="configurationName">The name of the configuration, default <see langword="null"/>. If set it will update the service information.</param>
        /// <param name="singleRun">The configuration is only run once, ignoring paused state and run time.</param>
        public void Initialize(string name, RunSchemeModel runScheme, bool runImmediately = false, string configurationName = null, bool singleRun = false)
        {
            if (!String.IsNullOrWhiteSpace(Name))
                return;

            Name = name;
            RunScheme = runScheme;
            RunImmediately = runImmediately;
            ConfigurationName = configurationName;
            SingleRun = singleRun;
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
                await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{Name} started, first run on: {runSchemesService.GetDateTimeTillNextRun(RunScheme)}", Name, RunScheme.TimeId);
                
                if (!String.IsNullOrWhiteSpace(ConfigurationName))
                {
                    await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme));
                }
                
                if (!RunImmediately && !SingleRun)
                {
                    await WaitTillNextRun(stoppingToken);
                }

                while (!stoppingToken.IsCancellationRequested)
                {
                    var paused = false;
                    var alreadyRunning = false;
                    
                    if (!String.IsNullOrWhiteSpace(ConfigurationName))
                    {
                        alreadyRunning = await wiserDashboardService.IsServiceRunning(ConfigurationName, RunScheme.TimeId);
                        paused = await wiserDashboardService.IsServicePaused(ConfigurationName, RunScheme.TimeId);
                        if (paused && !alreadyRunning)
                        {
                            await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, state: "paused");
                        }
                    }

                    if (!alreadyRunning)
                    {
                        if (!paused || SingleRun)
                        {
                            await logService.LogInformation(logger, LogScopes.RunStartAndStop, RunScheme.LogSettings, $"{Name} started at: {DateTime.Now}", Name, RunScheme.TimeId);
                            if (!String.IsNullOrWhiteSpace(ConfigurationName))
                            {
                                await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, state: "running");
                            }

                            var runStartTime = DateTime.Now;
                            var stopWatch = new Stopwatch();
                            stopWatch.Start();

                            await ExecuteActionAsync();

                            stopWatch.Stop();

                            await logService.LogInformation(logger, LogScopes.RunStartAndStop, RunScheme.LogSettings, $"{Name} finished at: {DateTime.Now}, time taken: {stopWatch.Elapsed}", Name, RunScheme.TimeId);

                            if (!String.IsNullOrWhiteSpace(ConfigurationName))
                            {
                                var states = await wiserDashboardService.GetLogStatesFromLastRun(ConfigurationName, RunScheme.TimeId, runStartTime);

                                var state = "success";
                                if (await wiserDashboardService.IsServicePaused(ConfigurationName, RunScheme.TimeId))
                                {
                                    state = "paused";
                                }
                                else if (states.Contains("Critical", StringComparer.OrdinalIgnoreCase) || states.Contains("Error", StringComparer.OrdinalIgnoreCase))
                                {
                                    state = "failed";
                                }
                                else if (states.Contains("Warning", StringComparer.OrdinalIgnoreCase))
                                {
                                    state = "warning";
                                }

                                await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme), lastRun: DateTime.Now, runTime: stopWatch.Elapsed, state: state, extraRun: false);
                            }
                        }
                        else
                        {
                            await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, nextRun: runSchemesService.GetDateTimeTillNextRun(RunScheme));
                        }
                    }

                    // If the configuration only needs to be run once break out of the while loop. State will not be set on stopped because the normal configuration is still active.
                    if (SingleRun)
                    {
                        break;
                    }

                    await WaitTillNextRun(stoppingToken);
                }
            }
            catch (TaskCanceledException)
            {
                await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} has been stopped after cancel was called.", ConfigurationName ?? Name, RunScheme.TimeId);
                if (!String.IsNullOrWhiteSpace(ConfigurationName))
                {
                    await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, state: "stopped");
                }
            }
            catch (Exception e)
            {
                await logService.LogCritical(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{ConfigurationName ?? Name} stopped with exception {e}", ConfigurationName ?? Name, RunScheme.TimeId);
                if (!String.IsNullOrWhiteSpace(ConfigurationName))
                {
                    await wiserDashboardService.UpdateServiceAsync(ConfigurationName, RunScheme.TimeId, state: "crashed");
                }
            }
        }

        /// <summary>
        /// Wait till the next run, if the time to wait is longer than the allowed delay the time is split and will wait in sections until the full time has completed.
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        private async Task WaitTillNextRun(CancellationToken stoppingToken)
        {
            bool timeSplit;

            do
            {
                timeSplit = false;
                var timeTillNextRun = runSchemesService.GetTimeTillNextRun(RunScheme);

                if (timeTillNextRun.TotalMilliseconds > Int32.MaxValue)
                {
                    timeTillNextRun = new TimeSpan(0, 0, 0, 0, Int32.MaxValue);
                    timeSplit = true;
                }

                await Task.Delay(timeTillNextRun, stoppingToken);

            } while (timeSplit);
        }

        /// <summary>
        /// Execute the action of the derived worker.
        /// </summary>
        /// <returns></returns>
        protected abstract Task ExecuteActionAsync();
    }
}
