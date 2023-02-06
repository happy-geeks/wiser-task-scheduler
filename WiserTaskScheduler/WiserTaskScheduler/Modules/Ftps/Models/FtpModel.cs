using System.Xml.Serialization;
using GeeksCoreLibrary.Modules.Ftps.Enums;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Models;
using WiserTaskScheduler.Modules.Ftps.Enums;

namespace WiserTaskScheduler.Modules.Ftps.Models;

[XmlType("Ftp")]
public class FtpModel : ActionModel
{
    /// <summary>
    /// Gets or sets the type of FTP that needs to be used.
    /// </summary>
    public FtpTypes Type { get; set; } = FtpTypes.Ftps;

    /// <summary>
    /// Gets or sets the action that needs to be performed.
    /// </summary>
    public FtpActionTypes Action { get; set; } = FtpActionTypes.Download;

    /// <summary>
    /// Gets or sets the encryption mode.
    /// </summary>
    public EncryptionModes EncryptionMode { get; set; } = EncryptionModes.Auto;

    /// <summary>
    /// Gets or sets the host of the FTP.
    /// </summary>
    public string Host { get; set; }

    /// <summary>
    /// Gets or sets the port of the FTP.
    /// </summary>
    public int Port { get; set; }

    /// <summary>
    /// Gets or sets the user to login with.
    /// </summary>
    public string User { get; set; }

    /// <summary>
    /// Gets or sets the password to login with.
    /// </summary>
    public string Password { get; set; }

    /// <summary>
    /// Gets or sets the location to get the file from. If left null or empty during upload the body will be used to create the file during the action.
    /// </summary>
    public string From { get; set; }
    
    /// <summary>
    /// Gets or sets the body to use for the file.
    /// </summary>
    public BodyModel Body { get; set; }

    /// <summary>
    /// Gets or sets the location to write the file to.
    /// </summary>
    public string To { get; set; }

    /// <summary>
    /// Gets or sets if the action needs to be performed on all files in the folder.
    /// </summary>
    public bool AllFilesInFolder { get; set; }

    /// <summary>
    /// Gets or sets if the file(s) need to be deleted after the action has successfully completed.
    /// </summary>
    public bool DeleteFileAfterAction { get; set; }

    /// <summary>
    /// Gets or sets if a passive connection needs to be used.
    /// </summary>
    public bool UsePassive { get; set; } = true;

    /// <summary>
    /// Gets or sets the path to the SSH private key.
    /// </summary>
    public string SshPrivateKeyPath { get; set; }

    /// <summary>
    /// Gets or sets the passphrase for the SSH private key.
    /// </summary>
    public string SshPrivateKeyPassphrase { get; set; }

    /// <summary>
    /// Gets or sets if it is a single action that needs to be executed.
    /// </summary>
    public bool SingleAction { get; set; } = true;
}