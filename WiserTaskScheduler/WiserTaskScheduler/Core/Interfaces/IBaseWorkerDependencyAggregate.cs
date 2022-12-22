using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;

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
        /// Gets the <see cref="RunSchemesService"/>.
        /// </summary>
        IRunSchemesService RunSchemesService { get; }
    }
}
