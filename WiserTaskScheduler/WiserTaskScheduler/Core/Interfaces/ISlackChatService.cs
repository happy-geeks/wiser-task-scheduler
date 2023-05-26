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
        /// <param name="replies">(Optional)An array of messages that will be added to the message thread.</param>
        /// <param name="recipient">(Optional)The target for the message, If not provided the one from the settings file will be used</param>
        /// <returns></returns>
        Task SendChannelMessageAsync(string message, string[] replies = null, string recipient = null);
    }
}