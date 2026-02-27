using System;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Models.GoogleChat;

namespace WiserTaskScheduler.Core.Services;

public class GoogleChatService(IOptions<WtsSettings> wtsSettings, ILogger<GoogleChatService> logger) : IGoogleChatService
{
    private readonly WtsSettings wtsSettings = wtsSettings.Value;
    private readonly GoogleChatSettings googleChatSettings = wtsSettings.Value.GoogleChatSettings;

    private readonly ConcurrentDictionary<string, DateTime> sendMessages = new();

    public async Task SendChannelMessageAsync(string message, string[] replies = null, string recipient = null, string messageHash = null)
    {
        var webHookUrl = recipient ?? (googleChatSettings.WebhookUrl);

        if (String.IsNullOrWhiteSpace(webHookUrl))
        {
            logger.LogDebug("No Google Chat Webhook URL provided so no message will be sent.");
            return;
        }

        if (!webHookUrl.StartsWith("https://chat.googleapis.com/v1/spaces/"))
        {
            logger.LogError("The provided Google Chat Webhook URL is not valid.");
            return;
        }

        // If a hash is provided and the message has been sent within the set interval time, don't send it again. 30 seconds are added as a buffer.
        if (!String.IsNullOrWhiteSpace(messageHash))
        {
            if (sendMessages.TryGetValue(messageHash, out var lastSendDate) && lastSendDate > DateTime.Now.AddMinutes(-wtsSettings.ErrorNotificationsIntervalInMinutes).AddSeconds(30))
            {
                return;
            }

            sendMessages.AddOrUpdate(messageHash, DateTime.Now, (key, oldValue) => DateTime.Now);
        }

        // send message via webhook
        string threadName;
        var serializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver()
        };
        using var httpClient = new HttpClient();

        try
        {
            var payload = new GoogleChatRequest { Text = message };
            var json = JsonConvert.SerializeObject(payload, serializerSettings);
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");

            var response = await httpClient.PostAsync(webHookUrl, content);
            if (!response.IsSuccessStatusCode)
            {
                var resp = await response.Content.ReadAsStringAsync();
                logger.LogError("Failed to send Google Chat message. Status: {Status}. Response: {Response}", response.StatusCode, resp);
                return;
            }

            if (replies is null || replies.Length == 0)
                return;

            threadName = await ExtractThreadName(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Exception while sending message to Google Chat webhook.");
            return;
        }

        var replyWebHook = webHookUrl + "&messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";

        // send replies to thread
        // If we couldn't get a thread name from the response, we still attempt to post replies to the same webhook; Google will treat them as separate messages if thread is not provided.
        foreach (var reply in replies)
        {
            try
            {
                var replyPayload = new GoogleChatRequest
                {
                    Text = reply,
                    Thread = threadName != null ? new GoogleChatThread { Name = threadName } : null
                };
                var replyJson = JsonConvert.SerializeObject(replyPayload, serializerSettings);
                using var content = new StringContent(replyJson, System.Text.Encoding.UTF8, "application/json");

                var replyResponse = await httpClient.PostAsync(replyWebHook, content);
                if (replyResponse.IsSuccessStatusCode) 
                    continue;
                
                var responseText = await replyResponse.Content.ReadAsStringAsync();
                logger.LogError("Failed to send Google Chat reply. Status: {Status}. Response: {Response}", replyResponse.StatusCode, responseText);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Exception while sending reply to Google Chat webhook.");
            }
        }
    }

    private async Task<string> ExtractThreadName(HttpResponseMessage response)
    {
        var responseContent = await response.Content.ReadAsStringAsync();
        if (String.IsNullOrWhiteSpace(responseContent)) 
            return null;
        
        try
        {
            var googleChatResponse = JsonConvert.DeserializeObject<GoogleChatResponse>(responseContent);
            if (googleChatResponse != null)
            {
                if (googleChatResponse.Thread != null && !String.IsNullOrWhiteSpace(googleChatResponse.Thread.Name))
                {
                    return googleChatResponse.Thread.Name;
                }
                    
                if (!String.IsNullOrWhiteSpace(googleChatResponse.Name))
                {
                    return googleChatResponse.Name;
                }
            }
        }
        catch (JsonException)
        {
            // Not JSON or unexpected format - ignore parsing, replies can't be threaded then.
            logger.LogDebug("Google Chat webhook returned non-JSON or unexpected payload when creating message: {Response}", responseContent);
            throw;
        }

        return null;
    }
}