using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Modules.RunSchemes.Interfaces;
using AutoImportServiceCore.Modules.RunSchemes.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AutoImportServiceCore.Core.Workers
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

        private readonly ILogService logService;
        private readonly ILogger<BaseWorker> logger;
        private readonly IRunSchemesService runSchemesService;

        /// <summary>
        /// Creates a new instance of <see cref="BaseWorker"/>.
        /// </summary>
        /// <param name="baseWorkerDependencyAggregate"></param>
        protected BaseWorker(IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate)
        {
            logService = baseWorkerDependencyAggregate.LogService;
            logger = baseWorkerDependencyAggregate.Logger;
            runSchemesService = baseWorkerDependencyAggregate.RunSchemesService;
        }

        /// <summary>
        /// Assigns the base values from a derived class.
        /// </summary>
        /// <param name="name">The name of the worker.</param>
        /// <param name="runScheme">The run scheme of the worker.</param>
        /// <param name="runImmediately">True to run the action immediately, false to run at first delayed time.</param>
        public void Initialize(string name, RunSchemeModel runScheme, bool runImmediately = false)
        {
            if (!String.IsNullOrWhiteSpace(Name))
                return;

            Name = name;
            RunScheme = runScheme;
            RunImmediately = runImmediately;
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

                if (!RunImmediately)
                {
                    await WaitTillNextRun(stoppingToken);
                }

                while (!stoppingToken.IsCancellationRequested)
                {
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, RunScheme.LogSettings, $"{Name} started at: {DateTime.Now}", Name, RunScheme.TimeId);

                    var stopWatch = new Stopwatch();
                    stopWatch.Start();

                    await ExecuteActionAsync();

                    stopWatch.Stop();

                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, RunScheme.LogSettings, $"{Name} finished at: {DateTime.Now}, time taken: {stopWatch.Elapsed}", Name, RunScheme.TimeId);

                    await WaitTillNextRun(stoppingToken);
                }
            }
            catch (TaskCanceledException)
            {
                await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{Name} has been stopped after cancel was called.", Name, RunScheme.TimeId);
            }
            catch (Exception e)
            {
                await logService.LogCritical(logger, LogScopes.StartAndStop, RunScheme.LogSettings, $"{Name} stopped with exception {e}", Name, RunScheme.TimeId);
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
