﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.SlackMessages.Interfaces;
using WiserTaskScheduler.Modules.SlackMessages.Models;

namespace WiserTaskScheduler.Modules.SlackMessages.Services;

/// <summary>
/// A service for a slack message action
/// </summary>
public class SlackMessageService(ISlackChatService slackChatService) : ISlackMessageService, IActionsService, IScopedService
{
    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var slackMessage = (SlackMessageModel) action;
        var useResultSet = slackMessage.UseResultSet;
        var msg = slackMessage.Message;

        if (!String.IsNullOrWhiteSpace(useResultSet))
        {
            var keyParts = useResultSet.Split('.');
            var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, ReplacementHelper.EmptyRows, resultSets);
            var remainingKey = keyParts.Length > 1 ? useResultSet[(keyParts[0].Length + 1)..] : "";
            var toPathTuple = ReplacementHelper.PrepareText(slackMessage.Message, usingResultSet, remainingKey, slackMessage.HashSettings);

            msg = ReplacementHelper.ReplaceText(toPathTuple.Item1, ReplacementHelper.EmptyRows, toPathTuple.Item2, usingResultSet, slackMessage.HashSettings);
        }

        await slackChatService.SendChannelMessageAsync(msg, [], slackMessage.Recipient);

        return new JObject
        {
            {"Results", new JArray()}
        };
    }
}