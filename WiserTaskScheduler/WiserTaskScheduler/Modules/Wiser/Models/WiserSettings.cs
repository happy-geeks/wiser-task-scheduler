using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Wiser.Services;

namespace WiserTaskScheduler.Modules.Wiser.Models;

/// <summary>
/// The settings for the connection to the Wiser API.
/// </summary>
public class WiserSettings
{
    /// <summary>
    /// Gets or sets the username of the Wiser account.
    /// </summary>
    public string Username { get; set; }

    /// <summary>
    /// Gets or sets the password of the Wiser account.
    /// </summary>
    public string Password { get; set; }

    /// <summary>
    /// Gets or sets the subdomain of the Wiser account.
    /// </summary>
    public string Subdomain { get; set; }

    /// <summary>
    /// Gets or sets the url for the Wiser API.
    /// </summary>
    public string WiserApiUrl { get; set; }

    /// <summary>
    /// Gets or sets the id of the client.
    /// </summary>
    public string ClientId { get; set; }

    /// <summary>
    /// Gets or sets the secret of the client.
    /// </summary>
    public string ClientSecret { get; set; }

    /// <summary>
    /// Gets or sets if the WTS is running on a test environment.
    /// </summary>
    public bool TestEnvironment { get; set; }

    /// <summary>
    /// Gets or sets the path within the WTS folder from which the configurations are loaded.
    /// </summary>
    public string ConfigurationPath { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="LogSettings"/> used by the <see cref="WiserService"/>.
    /// </summary>
    public LogSettings LogSettings { get; set; }
}