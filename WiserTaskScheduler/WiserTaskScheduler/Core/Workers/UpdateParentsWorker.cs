using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Workers
{
    public class UpdateParentsWorker : BaseWorker
    {
        private const string LogName = "ParentUpdateService";

        private readonly IParentUpdateService parentUpdateService;
        private readonly ILogger<UpdateParentsWorker> logger;

        /// <summary>
        /// Creates a new instance of <see cref="UpdateParentsWorker"/>.
        /// </summary>
        /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
        /// <param name="parentUpdateService"></param>
        /// <param name="logger"></param>
        /// <param name="baseWorkerDependencyAggregate"></param>
        public UpdateParentsWorker(IOptions<WtsSettings> wtsSettings, IParentUpdateService parentUpdateService, ILogger<UpdateParentsWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            Initialize(LogName, wtsSettings.Value.ParentsUpdateService.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
            RunScheme.LogSettings = wtsSettings.Value.ParentsUpdateService.LogSettings;

            this.parentUpdateService = parentUpdateService;
            this.logger = logger;

            this.parentUpdateService.LogSettings = RunScheme.LogSettings;
        }

        /// <inheritdoc />
        protected override async Task ExecuteActionAsync()
        {
            await parentUpdateService.ParentsUpdateAsync();
        }
    }
}
