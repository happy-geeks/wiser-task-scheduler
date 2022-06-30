using System.Threading.Tasks;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AutoImportServiceCore.Core.Workers
{
    public class CleanupWorker : BaseWorker
    {
        private const string LogName = "CleanupService";

        private readonly ICleanupService cleanupService;
        private readonly ILogger<CleanupWorker> logger;

        /// <summary>
        /// Creates a new instance of <see cref="CleanupWorker"/>.
        /// </summary>
        /// <param name="aisSettings">The settings of the AIS for the run scheme.</param>
        /// <param name="cleanupService"></param>
        /// <param name="logger"></param>
        /// <param name="baseWorkerDependencyAggregate"></param>
        public CleanupWorker(IOptions<AisSettings> aisSettings, ICleanupService cleanupService, ILogger<CleanupWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            Initialize(LogName, aisSettings.Value.CleanupService.RunScheme, true);
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
