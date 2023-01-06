using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;
using WiserTaskScheduler.Modules.Wiser.Interfaces;
using Microsoft.Extensions.Logging;

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
    }
}
