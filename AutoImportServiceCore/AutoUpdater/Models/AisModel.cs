namespace AutoUpdater.Models;

public class AisModel
{
    /// <summary>
    /// The name of the AIS service that needs tobe updated.
    /// </summary>
    public string ServiceName { get; set; }

    /// <summary>
    /// The path to the folder the AIS is placed in that needs to be updated.
    /// </summary>
    public string PathToFolder { get; set; }

    /// <summary>
    /// The email to contact when something went wrong or a manual action needs to be performed.
    /// </summary>
    public string ContactEmail { get; set; }

    /// <summary>
    /// Send a mail if the AIS has been updated.
    /// </summary>
    public bool SendMailOnUpdateComplete { get; set; }
}