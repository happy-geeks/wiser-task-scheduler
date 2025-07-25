﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Services;

public class LogService(IServiceProvider serviceProvider, ISlackChatService slackChatService, IOptions<WtsSettings> settings) : ILogService, ISingletonService
{
    private readonly WtsSettings settings = settings.Value;
    private readonly ConcurrentDictionary<string, LogLevel> highestLogLevelOfServices = new();

    /// <inheritdoc />
    public LogLevel GetLogLevelOfService(string configurationName, int timeId)
    {
        return highestLogLevelOfServices.TryGetValue($"{configurationName}-{timeId}", out var logLevel) ? logLevel : LogLevel.None;
    }

    /// <inheritdoc />
    public void ClearLogLevelOfService(string configurationName, int timeId)
    {
        highestLogLevelOfServices.TryRemove($"{configurationName}-{timeId}", out _);
    }

    /// <inheritdoc />
    public async Task LogDebug<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Debug, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    /// <inheritdoc />
    public async Task LogInformation<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Information, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    /// <inheritdoc />
    public async Task LogWarning<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Warning, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    /// <inheritdoc />
    public async Task LogError<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Error, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    /// <inheritdoc />
    public async Task LogCritical<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        await Log(logger, LogLevel.Critical, logScope, logSettings, message, configurationName, timeId, order, extraValuesToObfuscate);
    }

    /// <inheritdoc />
    public async Task Log<T>(ILogger<T> logger, LogLevel logLevel, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null)
    {
        highestLogLevelOfServices.AddOrUpdate($"{configurationName}-{timeId}", logLevel, (key, value) => value >= logLevel ? value : logLevel);

        logSettings ??= new LogSettings();
        if (logLevel < logSettings.LogMinimumLevel)
        {
            return;
        }

        message = ObfuscateText(message, extraValuesToObfuscate);

        switch (logScope)
        {
            // Log the message if the scope is allowed to log or if log is at least a warning.
            case LogScopes.StartAndStop when logSettings.LogStartAndStop || logLevel > LogLevel.Information:
            case LogScopes.RunStartAndStop when logSettings.LogRunStartAndStop || logLevel > LogLevel.Information:
            case LogScopes.RunBody when logSettings.LogRunBody || logLevel > LogLevel.Information:
            {
                try
                {
                    using var scope = serviceProvider.CreateScope();
                    // Try writing the log to the database.
                    try
                    {
                        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

                        databaseConnection.ClearParameters();
                        databaseConnection.AddParameter("message", message);
                        databaseConnection.AddParameter("level", logLevel.ToString());
                        databaseConnection.AddParameter("scope", logScope.ToString());
                        databaseConnection.AddParameter("source", typeof(T).Name);
                        databaseConnection.AddParameter("configuration", configurationName);
                        databaseConnection.AddParameter("timeId", timeId);
                        databaseConnection.AddParameter("order", order);
                        databaseConnection.AddParameter("addedOn", DateTime.Now);
#if DEBUG
                        databaseConnection.AddParameter("isTest", 1);
#else
                        databaseConnection.AddParameter("isTest", 0);
#endif

                        await databaseConnection.ExecuteAsync($"""
                                                               INSERT INTO {WiserTableNames.WtsLogs} (message, level, scope, source, configuration, time_id, `order`, added_on, is_test)
                                                               VALUES(?message, ?level, ?scope, ?source, ?configuration, ?timeId, ?order, ?addedOn, ?isTest)
                                                               """);
                    }
                    catch (Exception exception)
                    {
                        // If writing to the database fails log its error.
                        logger.Log(logLevel, exception, "Failed to write log to database due to exception.");
                    }

                    logger.Log(logLevel, message);

                    // Log to Slack chat service, if Slack logging is enabled.
                    if (logLevel >= logSettings.SlackLogLevel)
                    {
                        var linebreakIndex = message.IndexOf(Environment.NewLine, StringComparison.InvariantCulture);
                        var title = linebreakIndex >= 0 ? message[..linebreakIndex] : message;
                        var slackMessage = $"""
                                            Server: {Environment.MachineName} - {settings.Name}
                                            Log level: {logLevel}
                                            Configuration: '{configurationName}'
                                            Time ID: '{timeId}'
                                            Order: '{order}'
                                            Message:
                                            {title}
                                            """;

                        // Generate SHA 256 based on configuration name, time id, order id and message
                        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{configurationName}{timeId}{order}{message}"));
                        var messageHash = String.Join("", hash.Select(b => b.ToString("x2")));

                        await slackChatService.SendChannelMessageAsync(slackMessage, [message], messageHash: messageHash);
                    }
                }
                catch
                {
                    // If writing to the log file fails ignore it. We can't write it somewhere else and the application needs to continue.
                }

                break;
            }

            // Stop when the scope is evaluated above but is not allowed to log, to prevent the default exception to be thrown.
            case LogScopes.StartAndStop:
            case LogScopes.RunStartAndStop:
            case LogScopes.RunBody:
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(logScope), logScope.ToString(), null);
        }
    }

    /// <summary>
    /// Obfuscate the text based on the credentials in the settings.
    /// </summary>
    /// <param name="text">The text to obfuscate.</param>
    /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
    /// <returns>Returns the given text with all credentials obfuscated.</returns>
    private string ObfuscateText(string text, List<string> extraValuesToObfuscate)
    {
        if (String.IsNullOrWhiteSpace(text) || (settings.Credentials == null && extraValuesToObfuscate == null))
        {
            return text;
        }

        if (extraValuesToObfuscate != null)
        {
            foreach (var value in extraValuesToObfuscate)
            {
                text = text.Replace(value, "*****");
            }
        }

        if (settings.Credentials == null)
        {
            return text;
        }

        foreach (var credential in settings.Credentials)
        {
            text = text.Replace(credential.Value, "*****");
        }

        return text;
    }
}