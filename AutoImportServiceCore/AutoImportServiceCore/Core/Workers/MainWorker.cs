using System.Threading;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Helpers;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AutoImportServiceCore.Core.Workers
{
    /// <summary>
    /// The <see cref="MainWorker"/> manages all AIS configurations that are provided by Wiser.
    /// </summary>
    public class MainWorker : BaseWorker
    {
        private const string LogName = "MainService";

        private readonly IMainService mainService;
        private readonly ILogService logService;
        private readonly ILogger<MainWorker> logger;

        /// <summary>
        /// Creates a new instance of <see cref="MainWorker"/>.
        /// </summary>
        /// <param name="aisSettings">The settings of the AIS for the run scheme.</param>
        /// <param name="mainService"></param>
        /// <param name="logService">The service to use for logging.</param>
        /// <param name="logger"></param>
        /// <param name="baseWorkerDependencyAggregate"></param>
        public MainWorker(IOptions<AisSettings> aisSettings, IMainService mainService, ILogService logService, ILogger<MainWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            Initialize(LogName, aisSettings.Value.MainService.RunScheme, true);
            RunScheme.LogSettings ??= new LogSettings();

            this.mainService = mainService;
            this.logService = logService;
            this.logger = logger;

            this.mainService.LogSettings = RunScheme.LogSettings;
        }

        /// <inheritdoc />
        protected override async Task ExecuteActionAsync()
        {
            await mainService.ManageConfigurations();
        }

        /// <inheritdoc />
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, "Main worker needs to stop, stopping all configuration workers.", Name, RunScheme.TimeId);
            await mainService.StopAllConfigurations();
            await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, "All configuration workers have stopped, stopping main worker.", Name, RunScheme.TimeId);
            await base.StopAsync(cancellationToken);
        }
    }
}
