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

/// <summary>
/// A service for a script action.
/// </summary>
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

    /// <inheritdoc />
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
        var remainingKey = keyParts.Length > 1 ? script.UseResultSet[(keyParts[0].Length + 1)..] : "";
        var (scriptText, _, _) = ReplacementHelper.PrepareText(script.Script, (JObject) resultSets[keyParts[0]], remainingKey, script.HashSettings);

        var usingResultSet = ResultSetHelper.GetCorrectObject<JArray>(action.UseResultSet, ReplacementHelper.EmptyRows, resultSets);

        var result = script.Interpreter switch
        {
            Interpreters.JavaScript => await ExecuteJavaScript(script, scriptText, usingResultSet, configurationServiceName),
            _ => throw new NotImplementedException($"Interpreter '{script.Interpreter}' is invalid or is not yet implemented.")
        };

        return result;
    }

    /// <summary>
    /// Executes a JavaScript script. The result of the script is returned as a <see cref="JArray"/> in a <see cref="JObject"/> property called "<c>Results</c>".
    /// </summary>
    /// <param name="script">The <c>&lt;Script&gt;</c> tag that initiated the action.</param>
    /// <param name="scriptText">The actual script text to execute.</param>
    /// <param name="usingResultSet">The result set that will be added to the JavaScript engine with the name <c>WtsResultSet</c>.</param>
    /// <param name="configurationServiceName">The service name of the configuration that this action is part of. Only used for logging purposes.</param>
    /// <returns>A <see cref="JObject"/> with the JavaScript result as a <see cref="JArray"/> property called "<c>Results</c>".</returns>
    private async Task<JObject> ExecuteJavaScript(ScriptModel script, string scriptText, JArray usingResultSet, string configurationServiceName)
    {
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, script.LogSettings, $"Executing JavaScript in time id: {script.TimeId}, order: {script.Order}", configurationServiceName, script.TimeId, script.Order);

        // Create scope and get the JavaScript service.
        using var scope = serviceProvider.CreateScope();
        using var scriptService = scope.ServiceProvider.GetRequiredService<IJavaScriptService>();

        // Expose the result set to the JavaScript engine.
        scriptService.SetValue("WtsResultSet", usingResultSet);
        var result = scriptService.Evaluate<object[]>(scriptText);
        return new JObject
        {
            { "Results", JArray.FromObject(result) }
        };
    }
}