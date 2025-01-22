using System.ComponentModel.DataAnnotations;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Models;

namespace WiserTaskScheduler.Core.Models;

public class MainServiceSettings
{
    /// <summary>
    /// Gets or sets a configuration that needs to be run from the local disk instead of loading configurations from Wiser.
    /// </summary>
    public string LocalConfiguration { get; set; }

    /// <summary>
    /// Gets or sets a configuration that needs to be used for OAuth from the local disk instead of loading it from Wiser.
    /// </summary>
    public string LocalOAuthConfiguration { get; set; }

    /// <summary>
    /// Gets or sets the run scheme for the <see cref="MainWorker"/>.
    /// </summary>
    [Required]
    public RunSchemeModel RunScheme { get; set; }
}