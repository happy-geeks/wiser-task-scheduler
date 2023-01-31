using System.Threading.Tasks;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces
{
    /// <summary>
    /// A service for a configuration.
    /// </summary>
    public interface  IConfigurationsService
    {
        /// <summary>
        /// Gets or sets the log settings that needs to be used.
        /// </summary>
        LogSettings LogSettings { get; set; }

        /// <summary>
        /// Gets or sets the name of the configuration run scheme.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets if the configuration service has any action to execute.
        /// </summary>
        public bool HasAction { get; }

        /// <summary>
        /// Get all actions from the configuration that are associated with the time id.
        /// </summary>
        /// <param name="timeId"></param>
        /// <param name="configuration"></param>
        Task ExtractActionsFromConfigurationAsync(int timeId, ConfigurationModel configuration);

        /// <summary>
        /// Check if a configuration is valid, if there are conflicts in the configuration it will be invalid.
        /// </summary>
        /// <param name="configuration">The configuration to validate.</param>
        /// <returns></returns>
        Task<bool> IsValidConfigurationAsync(ConfigurationModel configuration);

        /// <summary>
        /// Execute all actions that have been extracted from the configuration from the time id.
        /// </summary>
        /// <returns></returns>
        Task ExecuteAsync();
    }
}
