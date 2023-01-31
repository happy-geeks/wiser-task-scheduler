using Microsoft.Extensions.Logging;

namespace WiserTaskScheduler.Core.Models
{
    /// <summary>
    /// A model for the settings used to know what information to log.
    /// </summary>
    public class LogSettings
    {
        /// <summary>
        /// Gets or sets if only errors need to be logged, overrules all other settings.
        /// </summary>
        public LogLevel LogMinimumLevel { get; set; } = LogLevel.Information;

        /// <summary>
        /// Gets or sets if only Critical Errors sent to slack.
        /// </summary>
        public LogLevel SlackLogLevel { get; set; } = LogLevel.Critical;

        /// <summary>
        /// Gets or sets if the start and stop needs to be logged.
        /// </summary>
        public bool LogStartAndStop { get; set; } = true;

        /// <summary>
        /// Gets or sets if the start and stop of a single run needs to be logged.
        /// </summary>
        public bool LogRunStartAndStop { get; set; } = true;

        /// <summary>
        /// Gets or sets if the body of the run needs to be logged.
        /// </summary>
        public bool LogRunBody { get; set; } = true;
    }
}
