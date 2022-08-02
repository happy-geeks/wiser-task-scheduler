using GeeksCoreLibrary.Modules.Communication.Models;

namespace AutoUpdater.Models;

public class UpdateSettings
{
    /// <summary>
    /// The settings to send mails.
    /// </summary>
    public SmtpSettings MailSettings { get; set; }

    /// <summary>
    /// The information of the multiple AIS instances to update.
    /// </summary>
    public List<AisModel> AisInstancesToUpdate { get; set; }
}