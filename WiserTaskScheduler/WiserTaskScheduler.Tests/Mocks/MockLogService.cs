using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Tests.Mocks;

public class MockLogService : ILogService
{
    public LogLevel GetLogLevelOfService(string configurationName, int timeId)
    {
        throw new NotImplementedException();
    }

    public void ClearLogLevelOfService(string configurationName, int timeId)
    {
        throw new NotImplementedException();
    }

    public async Task LogDebug<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Debug, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    public async Task LogInformation<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Information, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    public async Task LogWarning<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Warning, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    public async Task LogError<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Error, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    public async Task LogCritical<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Critical, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    public async Task Log<T>(ILogger<T> logger, LogLevel logLevel, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Task.CompletedTask;
    }
}