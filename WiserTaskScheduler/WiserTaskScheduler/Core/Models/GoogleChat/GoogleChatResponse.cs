namespace WiserTaskScheduler.Core.Models.GoogleChat;

public class GoogleChatResponse
{
    /// <summary>
    /// The resource name of the created message or thread
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Thread information when available
    /// </summary>
    public GoogleChatThread Thread { get; set; }
}

