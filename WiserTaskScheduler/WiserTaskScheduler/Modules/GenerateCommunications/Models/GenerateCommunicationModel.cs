using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Models;

namespace WiserTaskScheduler.Modules.GenerateCommunications.Models;

[XmlType("GenerateCommunication")]
public class GenerateCommunicationModel : ActionModel
{
    /// <summary>
    /// Gets or sets the type of communication to generate.
    /// </summary>
    public string CommunicationType { get; set; }

    /// <summary>
    /// Gets or sets the name of the receiver. Semi-colon separated list of names when multiple receivers.
    /// </summary>
    public string ReceiverName { get; set; }
    
    /// <summary>
    /// Gets or sets the receiver. Semi-colon separated list when multiple receivers.
    /// </summary>
    public string Receiver { get; set; }
    
    /// <summary>
    /// Gets or sets the name of the sender.
    /// </summary>
    public string SenderName { get; set; }
    
    /// <summary>
    /// Gets or sets the sender.
    /// </summary>
    public string Sender { get; set; }
    
    /// <summary>
    /// Gets or sets the reply to email address.
    /// </summary>
    public string ReplyToEmail { get; set; }
    
    /// <summary>
    /// Gets or sets the subject of the email.
    /// </summary>
    public string Subject { get; set; }
    
    /// <summary>
    /// Gets or sets the body of the communication.
    /// </summary>
    public BodyModel Body { get; set; }
}