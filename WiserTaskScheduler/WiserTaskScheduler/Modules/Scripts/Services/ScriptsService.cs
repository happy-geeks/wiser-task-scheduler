using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Modules.ScriptInterpreters.JavaScript.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Scripts.Enums;
using WiserTaskScheduler.Modules.Scripts.Interfaces;
using WiserTaskScheduler.Modules.Scripts.Models;

namespace WiserTaskScheduler.Modules.Scripts.Services;

public class ScriptsService : IScriptsService, IActionsService, IScopedService
{
    private readonly ILogService logService;
    private readonly ILogger<ScriptsService> logger;
    private readonly IServiceProvider serviceProvider;

    /// <summary>
    /// Creates a new instance of <see cref="ScriptsService"/>.
    /// </summary>
    public ScriptsService(ILogService logService, ILogger<ScriptsService> logger, IServiceProvider serviceProvider)
    {
        this.logService = logService;
        this.logger = logger;
        this.serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        return Task.CompletedTask;
    }

    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        if (String.IsNullOrWhiteSpace(action.UseResultSet))
        {
            await logService.LogWarning(logger, LogScopes.RunBody, action.LogSettings, "It is pointless to execute a script without a result set.", configurationServiceName, action.TimeId, action.Order);
            return null;
        }

        var script = (ScriptModel)action;
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, script.LogSettings, $"Executing script in time id: {script.TimeId}, order: {script.Order}", configurationServiceName, script.TimeId, script.Order);

        var keyParts = script.UseResultSet.Split('.');
        var remainingKey = keyParts.Length > 1 ? script.UseResultSet.Substring(keyParts[0].Length + 1) : "";
        var (scriptText, _, _) = ReplacementHelper.PrepareText(script.Script, (JObject) resultSets[keyParts[0]], remainingKey, script.HashSettings);

        var usingResultSet = ResultSetHelper.GetCorrectObject<JArray>(action.UseResultSet, ReplacementHelper.EmptyRows, resultSets);

        var result = script.Interpreter switch
        {
            Interpreters.JavaScript => await ExecuteJavaScript(script, scriptText, usingResultSet, configurationServiceName),
            _ => throw new NotImplementedException($"Interpreter '{script.Interpreter}' is invalid or is not yet implemented.")
        };

        return result;
    }

    private async Task<JObject> ExecuteJavaScript(ScriptModel script, string scriptText, JArray usingResultSet, string configurationServiceName)
    {
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, script.LogSettings, $"Executing JavaScript in time id: {script.TimeId}, order: {script.Order}", configurationServiceName, script.TimeId, script.Order);

        using var scope = serviceProvider.CreateScope();

        var scriptService = scope.ServiceProvider.GetRequiredService<IJavaScriptService>();

        scriptService.SetValue("WtsResultSet", usingResultSet);
        var result = scriptService.ExecuteScript(scriptText);
        return new JObject
        {
            { "Results", JArray.FromObject(result) }
        };
    }
}