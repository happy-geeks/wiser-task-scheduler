using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Services;
using Environment = System.Environment;

namespace WiserTaskScheduler.Core.Workers
{
    /// <summary>
    /// The <see cref="MainWorker"/> manages all WTS configurations that are provided by Wiser.
    /// </summary>
    public class MainWorker : BaseWorker
    {
        private const string LogName = "MainService";

        private readonly IMainService mainService;
        private readonly ILogService logService;
        private readonly ILogger<MainWorker> logger;
        private readonly ISlackChatService slackChatService;

        /// <summary>
        /// Creates a new instance of <see cref="MainWorker"/>.
        /// </summary>
        /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
        /// <param name="mainService"></param>
        /// <param name="logService">The service to use for logging.</param>
        /// <param name="logger"></param>
        /// <param name="baseWorkerDependencyAggregate"></param>
        public MainWorker(IOptions<WtsSettings> wtsSettings, IMainService mainService, ISlackChatService slackChatService, ILogService logService, ILogger<MainWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            Initialize(LogName, wtsSettings.Value.MainService.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
            RunScheme.LogSettings ??= new LogSettings();

            this.mainService = mainService;
            this.logService = logService;
            this.logger = logger;
            this.slackChatService = slackChatService;

            this.mainService.LogSettings = RunScheme.LogSettings;

            slackChatService.SendChannelMessageAsync($"*Wiser Task Scheduler has started ({Environment.MachineName})*");
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
            await slackChatService.SendChannelMessageAsync($"*Wiser Task Scheduler was shut down ({Environment.MachineName})*");
            await mainService.StopAllConfigurationsAsync();
            await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, "All configuration workers have stopped, stopping main worker.", Name, RunScheme.TimeId);
            await base.StopAsync(cancellationToken);
        }
    }
}