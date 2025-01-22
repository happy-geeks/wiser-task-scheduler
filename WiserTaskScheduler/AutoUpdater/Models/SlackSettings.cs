namespace AutoUpdater.Models;

public class SlackSettings
{
    /// <summary>
    /// Gets or sets the name of the SlackChannel.
    /// </summary>
    public string Channel { get; init; }

    /// <summary>
    /// Gets or sets the token of the bot that is being used.
    /// </summary>
    public string BotToken { get; init; }
}