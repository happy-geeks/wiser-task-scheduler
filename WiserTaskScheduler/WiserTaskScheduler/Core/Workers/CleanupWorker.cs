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
        private readonly ILogger<CleanupWorker> logger;

        /// <summary>
        /// Creates a new instance of <see cref="CleanupWorker"/>.
        /// </summary>
        /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
        /// <param name="cleanupService"></param>
        /// <param name="logger"></param>
        /// <param name="baseWorkerDependencyAggregate"></param>
        public CleanupWorker(IOptions<WtsSettings> wtsSettings, ICleanupService cleanupService, ILogger<CleanupWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            Initialize(LogName, wtsSettings.Value.CleanupService.RunScheme, true);
            RunScheme.LogSettings ??= new LogSettings();

            this.cleanupService = cleanupService;
            this.logger = logger;

            this.cleanupService.LogSettings = RunScheme.LogSettings;
        }

        /// <inheritdoc />
        protected override async Task ExecuteActionAsync()
        {
            await cleanupService.CleanupAsync();
        }
    }
}
