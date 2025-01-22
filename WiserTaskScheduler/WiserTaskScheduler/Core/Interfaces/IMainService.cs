using System.Threading.Tasks;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces;

/// <summary>
/// A service to manage all WTS configurations that are provided by Wiser.
/// </summary>
public interface IMainService
{
    /// <summary>
    /// Gets or sets the log settings that the Main service needs to use.
    /// </summary>
    LogSettings LogSettings { get; set; }

    /// <summary>
    /// Manage the WTS configurations.
    /// </summary>
    /// <returns></returns>
    Task ManageConfigurations();

    /// <summary>
    /// Stops all configurations that are currently running.
    /// </summary>
    /// <returns></returns>
    Task StopAllConfigurationsAsync();
}