using System.Threading.Tasks;

namespace WiserTaskScheduler.Core.Interfaces
{
    /// <summary>
    /// A service to handle all things related to the slack chat service
    /// </summary>
    public interface ISlackChatService
    {
        /// <summary>
        /// Function to send a message to the specified recipient
        /// </summary>
        /// <param name="message">The message to be send.</param>
        /// <param name="replies">(Optional) An array of messages that will be added to the message thread.</param>
        /// <param name="recipient">(Optional) The target for the message, If not provided the one from the settings file will be used</param>
        /// <param name="messageHash">(Optional) A hash to check if the message was already send within a given time. If not provided the message will always be sent.</param>
        /// <returns></returns>
        Task SendChannelMessageAsync(string message, string[] replies = null, string recipient = null, string messageHash = null);
    }
}