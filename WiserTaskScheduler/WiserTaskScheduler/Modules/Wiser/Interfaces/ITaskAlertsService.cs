using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.Models;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.Wiser.Interfaces;

/// <summary>
/// Service for sending task alerts / messages to users in Wiser, or doing other things with those messages.
/// </summary>
public interface ITaskAlertsService
{
    /// <summary>
    /// Send a task alert to a user. A task alert is a message that the user will see in Wiser, it's like a to do list for the user, or can be used as a notification.
    /// The user will also receive a notification in Wiser with a sound, that is sent via Pusher.
    /// </summary>
    /// <param name="receiverId">The ID of the Wiser user that should receive the message.</param>
    /// <param name="receiverName">The name of the Wiser user that should receive the message.</param>
    /// <param name="message">The message to send to the user.</param>
    /// <param name="action">The WTS action from which the message is being sent. This is required for logging.</param>
    /// <param name="configurationServiceName">The WTS configuration name from which the message is being sent. This is required for logging.</param>
    /// <param name="replaceData">The data to be used for replacements.</param>
    /// <param name="senderId">Optional: The ID of the Wiser user that sends the message. If no value is given, the receiver ID will be used (so that it looks like the user sent the message to themselves).</param>
    /// <param name="senderName">Optional: The name of the Wiser user that sends the message. Default value is "WTS".</param>
    /// <returns>The <see cref="WiserItemModel">WiserItemModel</see> of the task alert that was sent.</returns>
    Task<WiserItemModel> SendMessageToUserAsync(ulong receiverId, string receiverName, string message, ActionModel action, string configurationServiceName, IDictionary<string, object> replaceData, ulong senderId = 0, string senderName = "WTS");

    /// <summary>
    /// Notify the user that placed the import using an email about the status of the import.
    /// </summary>
    /// <param name="receiverId">The ID of the Wiser user that should receive the message.</param>
    /// <param name="receiverName">The name of the Wiser user that should receive the message.</param>
    /// <param name="action">The WTS information for handling the imports.</param>
    /// <param name="configurationServiceName">The name of the configuration the import is executed within.</param>
    /// <param name="subject">The subject of the email.</param>
    /// <param name="content">The body of the email.</param>
    /// <param name="replaceData">The data to be used for replacements.</param>
    /// <param name="senderEmail">Optional: The e-mail address of the sender. Default value is whatever is set as default sender for e-mails in the app settings.</param>
    /// <param name="senderName">Optional: The name of the sender. Default value is whatever is set as default sender for e-mails in the app settings.</param>
    Task NotifyUserByEmailAsync(ulong receiverId, string receiverName, ActionModel action, string configurationServiceName, string subject, string content, IDictionary<string, object> replaceData, string senderEmail = null, string senderName = null);
}