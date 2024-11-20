using System.Threading.Tasks;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces;

public interface IAutoProjectDeployService
{
    /// <summary>
    /// Gets or sets the log settings that the Auto Project Deploy service needs to use.
    /// </summary>
    LogSettings LogSettings { get; set; }

    Task ManageAutoProjectDeploy();
}