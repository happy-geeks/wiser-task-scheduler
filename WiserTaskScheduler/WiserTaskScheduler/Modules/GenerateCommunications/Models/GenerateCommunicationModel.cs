using System;
using System.Xml.Serialization;
using GeeksCoreLibrary.Modules.Communication.Enums;
using GeeksCoreLibrary.Modules.Communication.Models;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Models;

namespace WiserTaskScheduler.Modules.GenerateCommunications.Models;

[XmlType("GenerateCommunication")]
public class GenerateCommunicationModel : ActionModel
{
    /// <summary>
    /// Gets or sets the type of communication to generate.
    /// </summary>
    public CommunicationTypes CommunicationType { get; set; }

    /// <summary>
    /// Gets or sets a value to be used to identify the communication in the created result set.
    /// </summary>
    public string Identifier { get; set; } = String.Empty;
    
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
    /// Gets or sets the reply to information such as an email.
    /// </summary>
    public string ReplyTo { get; set; }
    
    /// <summary>
    /// Gets or sets the subject of the email.
    /// </summary>
    public string Subject { get; set; }
    
    /// <summary>
    /// Gets or sets the body of the communication.
    /// </summary>
    public BodyModel Body { get; set; }

    /// <summary>
    /// Gets or sets if the communication is a single communication or a batch. When it is a batch it expects an array for the result set.
    /// </summary>
    public bool SingleCommunication { get; set; } = true;
    
    /// <summary>
    /// Gets or sets if the queue needs to be skipped and the communication needs to be sent immediately. (Note that if it fails to send it will not be retried.)
    /// </summary>
    public bool SkipQueue { get; set; }

    /// <summary>
    /// Gets or sets the settings for the SMTP if the <see cref="Type"/> is Email and <see cref="SkipQueue"/> is true.
    /// </summary>
    public SmtpSettings SmtpSettings { get; set; }

    /// <summary>
    /// Gets or sets the settings for SMS if the <see cref="Type"/> is SMS or WhatsApp and <see cref="SkipQueue"/> is true.
    /// </summary>
    public SmsSettings SmsSettings { get; set; }
}