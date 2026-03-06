using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.GoogleChat.Interfaces;
using WiserTaskScheduler.Modules.GoogleChat.Models;

namespace WiserTaskScheduler.Modules.GoogleChat.Services;

public class GoogleMessageService(IGoogleChatService googleChatService) : IGoogleMessageService, IActionsService, IScopedService
{
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        return Task.CompletedTask;
    }

    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var googleMessageAction = (GoogleChatMessageModel) action;
        var useResultSet = googleMessageAction.UseResultSet;
        var message = googleMessageAction.Message;

        if (!String.IsNullOrWhiteSpace(useResultSet))
        {
            var keyParts = useResultSet.Split('.');
            var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, ReplacementHelper.EmptyRows, resultSets);
            var remainingKey = keyParts.Length > 1 ? useResultSet[(keyParts[0].Length + 1)..] : "";
            var toPathTuple = ReplacementHelper.PrepareText(googleMessageAction.Message, usingResultSet, remainingKey, googleMessageAction.HashSettings);

            message = ReplacementHelper.ReplaceText(toPathTuple.Item1, ReplacementHelper.EmptyRows, toPathTuple.Item2, usingResultSet, googleMessageAction.HashSettings);
        }

        await googleChatService.SendChannelMessageAsync(message, [], googleMessageAction.WebHookUrl);

        return new JObject
        {
            {"Results", new JArray()}
        };
    }
}