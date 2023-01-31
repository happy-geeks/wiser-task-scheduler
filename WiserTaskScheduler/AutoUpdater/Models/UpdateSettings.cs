using GeeksCoreLibrary.Modules.Communication.Models;

namespace AutoUpdater.Models;

public class UpdateSettings
{
    /// <summary>
    /// The URL to the JSON file containing the version information.
    /// </summary>
    public string VersionListUrl { get; set; } = "https://github.com/happy-geeks/wiser-task-scheduler/raw/main/update/versions.json";

    /// <summary>
    /// The URL to the ZIP file containing the latest version of the WTS.
    /// </summary>
    public string VersionDownloadUrl { get; set; } = "https://github.com/happy-geeks/wiser-task-scheduler/raw/main/update/update.zip";

    /// <summary>
    /// The settings to send mails.
    /// </summary>
    public SmtpSettings MailSettings { get; set; }

    /// <summary>
    /// The information of the multiple WTS instances to update.
    /// </summary>
    public List<WtsModel> WtsInstancesToUpdate { get; set; }
}