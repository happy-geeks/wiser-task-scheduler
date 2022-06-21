using System.Xml.Serialization;
using AutoImportServiceCore.Core.Models;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Communication.Enums;

namespace AutoImportServiceCore.Modules.Communications.Models;

[XmlType("Communication")]
public class CommunicationModel : ActionModel
{
    public CommunicationTypes Type { get; set; } = CommunicationTypes.Email;

    public int MaxNumberOfCommunicationAttempts { get; set; } = 5;

    public int MaxDelayInHours { get; set; } = 0;

    public string ConnectionString { get; set; }

    public SmtpSettings SmtpSettings { get; set; }
}