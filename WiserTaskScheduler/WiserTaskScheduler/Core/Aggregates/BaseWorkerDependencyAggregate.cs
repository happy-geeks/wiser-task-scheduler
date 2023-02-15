using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;
using WiserTaskScheduler.Modules.Wiser.Interfaces;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Aggregates
{
    /// <summary>
    /// An aggregate for the dependencies of the <see cref="BaseWorker"/>.
    /// </summary>
    public class BaseWorkerDependencyAggregate : IBaseWorkerDependencyAggregate, IScopedService, ISingletonService
    {
        /// <inheritdoc />
        public ILogService LogService { get; }

        /// <inheritdoc />
        public ILogger<BaseWorker> Logger { get; }

        /// <inheritdoc />
        public IRunSchemesService RunSchemesService { get; }

        /// <inheritdoc />
        public IWiserDashboardService WiserDashboardService { get; }

        /// <inheritdoc />
        public IErrorNotificationService ErrorNotificationService { get; }
        
        /// <inheritdoc />
        public WtsSettings WtsSettings { get; }

        /// <summary>
        /// Creates a new instance of <see cref="BaseWorkerDependencyAggregate"/>.
        /// </summary>
        /// <param name="logService"></param>
        /// <param name="logger"></param>
        /// <param name="runSchemesService"></param>
        /// <param name="wiserDashboardService"></param>
        /// <param name="errorNotificationService"></param>
        /// <param name="wtsSettings"></param>
        public BaseWorkerDependencyAggregate(ILogService logService, ILogger<BaseWorker> logger, IRunSchemesService runSchemesService, IWiserDashboardService wiserDashboardService, IErrorNotificationService errorNotificationService, IOptions<WtsSettings> wtsSettings)
        {
            LogService = logService;
            Logger = logger;
            RunSchemesService = runSchemesService;
            WiserDashboardService = wiserDashboardService;
            ErrorNotificationService = errorNotificationService;
            WtsSettings = wtsSettings.Value;
        }
    }
}
