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
    }
}
