using GeeksCoreLibrary.Modules.Communication.Models;

namespace AutoUpdater.Models;

public class UpdateSettings
{
    /// <summary>
    /// The URL to the JSON file containing the version information.
    /// </summary>
    public string VersionListUrl { get; set; } = "https://raw.githubusercontent.com/happy-geeks/wiser-task-scheduler/main/Update/versions.json";

    /// <summary>
    /// The URL to the ZIP file containing the latest version of the WTS.
    /// </summary>
    public string VersionDownloadUrl { get; set; } = "https://github.com/happy-geeks/wiser-task-scheduler/raw/main/Update/Update.zip";

    /// <summary>
    /// The information of the multiple WTS instances to update.
    /// </summary>
    public List<WtsModel> WtsInstancesToUpdate { get; set; }
}