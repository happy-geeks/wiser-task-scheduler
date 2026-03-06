namespace WiserTaskScheduler.Core.Models.GoogleChat;

public class GoogleChatRequest
{
    /// <summary>
    /// The text content of the message
    /// </summary>
    public string Text { get; set; }

    /// <summary>
    /// Optional thread object to post the message into an existing thread
    /// </summary>
    public GoogleChatThread Thread { get; set; }
    
}

