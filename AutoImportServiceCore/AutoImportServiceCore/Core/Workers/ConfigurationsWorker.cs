using System.Threading.Tasks;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.RunSchemes.Models;
using Microsoft.Extensions.Logging;

namespace AutoImportServiceCore.Core.Workers
{
    /// <summary>
    /// The <see cref="ConfigurationsWorker"/> is used to run a run scheme from a configuration from Wiser.
    /// </summary>
    public class ConfigurationsWorker : BaseWorker
    {
        private readonly ILogger<ConfigurationsWorker> logger;
        private readonly IConfigurationsService configurationsService;
        
        public ConfigurationModel Configuration { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="ConfigurationsWorker"/>.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="configurationsService"></param>
        /// <param name="baseWorkerDependencyAggregate"></param>
        public ConfigurationsWorker(ILogger<ConfigurationsWorker> logger, IConfigurationsService configurationsService, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
        {
            this.logger = logger;
            this.configurationsService = configurationsService;
        }

        /// <summary>
        /// Assigns the base values from a derived class.
        /// </summary>
        /// <param name="configuration">The configuration to retrieve the correct information from.</param>
        /// <param name="name">The name of the worker.</param>
        /// <param name="runScheme">The run scheme of the worker.</param>
        /// <param name="singleRun">The configuration is only run once, ignoring paused state and run time.</param>
        public void Initialize(ConfigurationModel configuration, string name, RunSchemeModel runScheme, bool singleRun = false)
        {
            Initialize(name, runScheme, runScheme.RunImmediately, configuration.ServiceName, singleRun);
            Configuration = configuration;

            configurationsService.Name = Name;
            configurationsService.LogSettings = RunScheme.LogSettings;

            configurationsService.ExtractActionsFromConfiguration(RunScheme.TimeId, configuration);
        }

        /// <inheritdoc />
        protected override async Task ExecuteActionAsync()
        {
            await configurationsService.ExecuteAsync();
        }
    }
}
