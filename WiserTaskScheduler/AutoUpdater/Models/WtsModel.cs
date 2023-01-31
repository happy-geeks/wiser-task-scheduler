namespace AutoUpdater.Models;

public class WtsModel
{
    /// <summary>
    /// The name of the WTS service that needs tobe updated.
    /// </summary>
    public string ServiceName { get; set; }

    /// <summary>
    /// The path to the folder the WTS is placed in that needs to be updated.
    /// </summary>
    public string PathToFolder { get; set; }

    /// <summary>
    /// The email to contact when something went wrong or a manual action needs to be performed.
    /// </summary>
    public string ContactEmail { get; set; }

    /// <summary>
    /// Send a email if the WTS has been updated.
    /// </summary>
    public bool SendEmailOnUpdateComplete { get; set; }
}