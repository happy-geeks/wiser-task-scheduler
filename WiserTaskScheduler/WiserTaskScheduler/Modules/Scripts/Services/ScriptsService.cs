using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Modules.ScriptInterpreters.Lua.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
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
        var script = (ScriptModel)action;
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, script.LogSettings, $"Executing script in time id: {script.TimeId}, order: {script.Order}", configurationServiceName, script.TimeId, script.Order);

        var result = script.Interpreter switch
        {
            Interpreters.Lua => await ExecuteLuaScript(script, resultSets, configurationServiceName),
            _ => throw new ArgumentOutOfRangeException(nameof(script.Interpreter), script.Interpreter, null)
        };

        return result;
    }

    private async Task<JObject> ExecuteLuaScript(ScriptModel script, JObject resultSets, string configurationServiceName)
    {
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, script.LogSettings, $"Executing Lua script in time id: {script.TimeId}, order: {script.Order}", configurationServiceName, script.TimeId, script.Order);

        using var scope = serviceProvider.CreateScope();

        var keyParts = script.UseResultSet.Split('.');

        var luaService = scope.ServiceProvider.GetRequiredService<ILuaService>();
        var result = luaService.ExecuteScript(script.Script);
        return new JObject
        {
            { "Result", JArray.FromObject(result) }
        };
    }
}