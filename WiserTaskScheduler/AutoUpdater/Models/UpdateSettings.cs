using GeeksCoreLibrary.Modules.Communication.Models;

namespace AutoUpdater.Models;

public class UpdateSettings
{
    /// <summary>
    /// The URL to the JSON file containing the version information.
    /// </summary>
    public string VersionListUrl { get; set; } = "https://raw.githubusercontent.com/happy-geeks/wiser-task-scheduler/main/Update/versions.json";

    /// <summary>
    /// The URL to the location where the ZIP file containing the latest version of the WTS is located.
    /// </summary>
    public string VersionDownloadUrl { get; set; } = "https://wts.happyhorizon.dev/";

    /// <summary>
    /// The information of the multiple WTS instances to update.
    /// </summary>
    public List<WtsModel> WtsInstancesToUpdate { get; set; }
}