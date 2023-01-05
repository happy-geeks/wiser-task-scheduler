using WiserTaskScheduler.Core.Models.Cleanup;
using WiserTaskScheduler.Modules.Wiser.Models;

namespace WiserTaskScheduler.Core.Models
{
    /// <summary>
    /// The settings for the WTS.
    /// </summary>
    public class WtsSettings
    {
        /// <summary>
        /// Gets or sets the settings of the <see cref="MainService"/>.
        /// </summary>
        public MainServiceSettings MainService { get; set; }

        /// <summary>
        /// Gets or sets the settings of the <see cref="CleanupService"/>.
        /// </summary>
        public CleanupServiceSettings CleanupService { get; set; } = new CleanupServiceSettings();

        /// <summary>
        /// Gets or sets the settings for the connection to Wiser 3.
        /// </summary>
        public WiserSettings Wiser { get; set; }
        
        /// <summary>
        /// A semicolon (;) seperated list of email addresses to notify when a core service failed during execution.
        /// </summary>
        public string ServiceFailedNotificationEmails { get; set; }
    }
}
