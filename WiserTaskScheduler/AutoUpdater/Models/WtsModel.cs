using System.ComponentModel;
using System.Xml;
using System.Xml.Serialization;

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
    
    /// <summary>
    /// Gets or sets the time the WTS needs to be updated. If no value has been provided or the time has already been passed the WTS will be updated immediately.
    /// </summary>
    [XmlIgnore]
    public TimeSpan UpdateTime { get; set; }

    /// <summary>
    /// Gets or sets <see cref="UpdateTime"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("UpdateTime")]
    public string UpdateTimeString
    {
        get => XmlConvert.ToString(UpdateTime);
        set => UpdateTime = String.IsNullOrWhiteSpace(value) ? TimeSpan.Zero : value.StartsWith("P") ? XmlConvert.ToTimeSpan(value) : TimeSpan.Parse(value);
    }
}