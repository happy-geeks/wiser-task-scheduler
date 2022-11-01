﻿using GeeksCoreLibrary.Modules.Communication.Models;

namespace AutoUpdater.Models;

public class UpdateSettings
{
    /// <summary>
    /// The URL to the JSON file containing the version information.
    /// </summary>
    public string VersionListUrl { get; set; }

    /// <summary>
    /// The URL to the ZIP file containing the latest version of the AIS.
    /// </summary>
    public string VersionDownloadUrl { get; set; }

    /// <summary>
    /// The settings to send mails.
    /// </summary>
    public SmtpSettings MailSettings { get; set; }

    /// <summary>
    /// The information of the multiple AIS instances to update.
    /// </summary>
    public List<AisModel> AisInstancesToUpdate { get; set; }
}