using System.Threading;
using System.Threading.Tasks;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces;

public interface IAutoProjectDeployService
{
    /// <summary>
    /// Gets or sets the log settings that the Auto Project Deploy service needs to use.
    /// </summary>
    LogSettings LogSettings { get; set; }

    /// <summary>
    /// Manages the auto project deploy.
    /// </summary>
    /// <param name="stoppingToken">The <see cref="CancellationToken"/> used for the current background service to indicate if it is being stopped.</param>
    /// <returns></returns>
    Task ManageAutoProjectDeployAsync(CancellationToken stoppingToken);
}