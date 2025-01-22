using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using WiserTaskScheduler.Core.Interfaces;
#if !DEBUG
using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using SlackNet;
using SlackNet.WebApi;
using WiserTaskScheduler.Core.Models;
#endif

namespace WiserTaskScheduler.Core.Services;

#if DEBUG
public class SlackChatService : ISlackChatService, ISingletonService
{
    /// <inheritdoc />
    public Task SendChannelMessageAsync(string message, string[] replies = null, string recipient = null, string messageHash = null)
    {
        // Only send messages to Slack for production Wiser Task Schedulers to prevent exceptions during developing/testing to trigger it.
        return Task.CompletedTask;
    }
}
#else
public class SlackChatService(IServiceProvider serviceProvider, IOptions<WtsSettings> wtsSettings) : ISlackChatService, ISingletonService
{
    private readonly WtsSettings wtsSettings = wtsSettings.Value;
    private readonly SlackSettings slackSettings = wtsSettings.Value.SlackSettings;

    private readonly ConcurrentDictionary<string, DateTime> sendMessages = new();

    /// <inheritdoc />
    public async Task SendChannelMessageAsync(string message, string[] replies = null, string recipient = null, string messageHash = null)
    {
        if (slackSettings != null && !String.IsNullOrWhiteSpace(slackSettings.BotToken))
        {
            // If a hash is provided and the message has been sent within the set interval time, don't send it again. 30 seconds are added as a buffer.
            if (!String.IsNullOrWhiteSpace(messageHash))
            {
                if (sendMessages.TryGetValue(messageHash, out var lastSendDate) && lastSendDate > DateTime.Now.AddMinutes(-wtsSettings.ErrorNotificationsIntervalInMinutes).AddSeconds(30))
                {
                    return;
                }

                sendMessages.AddOrUpdate(messageHash, DateTime.Now, (key, oldValue) => DateTime.Now);
            }

            var slackMessage = new Message
            {
                Text = message,
                Channel = recipient ?? (slackSettings.Channel ?? "")
            };

            using var scope = serviceProvider.CreateScope();
            var slack = scope.ServiceProvider.GetRequiredService<ISlackApiClient>();

            var mainMessageSend = await slack.Chat.PostMessage(slackMessage);

            if (replies != null)
            {
                foreach (var reply in replies)
                {
                    var replyMessage = new Message
                    {
                        Text = reply,
                        Channel = recipient ?? (slackSettings.Channel ?? ""),
                        ThreadTs = mainMessageSend.Ts
                    };

                    await slack.Chat.PostMessage(replyMessage);
                }
            }
        }
    }
}
#endif