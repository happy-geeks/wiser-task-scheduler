namespace WiserTaskScheduler.Modules.Slack.modules;

public class SlackSettings
{ 
    /// <summary>
    /// Gets or sets the name of the SlackChannel.
    /// </summary>
    public  string Channel { get; set; }
    
    /// <summary>
    /// Gets or sets the token of the bot that is being used.
    /// </summary>
    public string BotToken { get; set; }
}