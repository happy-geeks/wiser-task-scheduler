using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Workers
{
    public class CleanupWorker : BaseWorker
    {
        private const string LogName = "CleanupService";

        private readonly ICleanupService cleanupService;

        /// <summary>
        /// Creates a new instance of <see cref="CleanupWorker"/>.
        /// </summary>
        /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
        /// <param name="cleanupService">The service to handle the clean up for the WTS.</param>
        /// <param name="baseWorkerDependencyAggregate">The aggregate containing the dependencies needed by the <see cref="BaseWorker"/>.</param>
        public CleanupWorker(IOptions<WtsSettings> wtsSettings, ICleanupService cleanupService, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            Initialize(LogName, wtsSettings.Value.CleanupService.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
            RunScheme.LogSettings ??= new LogSettings();

            this.cleanupService = cleanupService;

            this.cleanupService.LogSettings = RunScheme.LogSettings;
        }

        /// <inheritdoc />
        protected override async Task ExecuteActionAsync(CancellationToken stoppingToken)
        {
            await cleanupService.CleanupAsync();
        }
    }
}
