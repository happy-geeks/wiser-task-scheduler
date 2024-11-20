using Microsoft.Extensions.Logging;

namespace WiserTaskScheduler.Core.Models.AutoProjectDeploy;

public class AutoProjectDeploySettings
{
    public bool IsEnabled { get; set; }

    /// <summary>
    /// Gets or sets the log settings for the auto project deploy service.
    /// </summary>
    public LogSettings LogSettings { get; set; } = new LogSettings
    {
        LogMinimumLevel = LogLevel.None
    };
}