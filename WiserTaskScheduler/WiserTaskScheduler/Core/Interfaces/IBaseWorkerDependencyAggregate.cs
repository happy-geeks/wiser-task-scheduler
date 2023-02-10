using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;
using WiserTaskScheduler.Modules.Wiser.Interfaces;
using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces
{
    /// <summary>
    /// An aggregate for the dependencies of the <see cref="BaseWorker"/>.
    /// </summary>
    public interface IBaseWorkerDependencyAggregate
    {
        /// <summary>
        /// Gets the service to use for logging.
        /// </summary>
        ILogService LogService { get; }

        /// <summary>
        /// Gets the <see cref="ILogger"/>.
        /// </summary>
        ILogger<BaseWorker> Logger { get; }

        /// <summary>
        /// Gets the <see cref="IRunSchemesService"/>.
        /// </summary>
        IRunSchemesService RunSchemesService { get; }
        
        /// <summary>
        /// Gets the <see cref="IWiserDashboardService"/>.
        /// </summary>
        IWiserDashboardService WiserDashboardService { get; }
        
        /// <summary>
        /// Gets the <see cref="IErrorNotificationService"/>.
        /// </summary>
        IErrorNotificationService ErrorNotificationService { get; }

        /// <summary>
        /// Gets or sets the <see cref="WtsSettings"/>.
        /// </summary>
        WtsSettings WtsSettings { get; }
    }
}
