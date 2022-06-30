using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Workers;
using AutoImportServiceCore.Modules.RunSchemes.Interfaces;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;

namespace AutoImportServiceCore.Core.Aggregates
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

        /// <summary>
        /// Creates a new instance of <see cref="BaseWorkerDependencyAggregate"/>.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="runSchemesService"></param>
        public BaseWorkerDependencyAggregate(ILogService logService, ILogger<BaseWorker> logger, IRunSchemesService runSchemesService)
        {
            LogService = logService;
            Logger = logger;
            RunSchemesService = runSchemesService;
        }
    }
}
