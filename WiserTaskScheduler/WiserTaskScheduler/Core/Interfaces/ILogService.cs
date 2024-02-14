using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces
{
    public interface ILogService
    {
        /// <summary>
        /// Log as debug message.
        /// </summary>
        /// <typeparam name="T">The type of the caller.</typeparam>
        /// <param name="logger">The logger to use.</param>
        /// <param name="logScope">The scope of the message of log.</param>
        /// <param name="logSettings">The log settings of the caller.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="configurationName">The name of the configuration from which is being logged.</param>
        /// <param name="timeId">The time id in the configuration from which is being logged.</param>
        /// <param name="order">The order in the time id in the configuration which is being logged.</param>
        /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
        Task LogDebug<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null);

        /// <summary>
        /// Log information.
        /// </summary>
        /// <typeparam name="T">The type of the caller.</typeparam>
        /// <param name="logger">The logger to use.</param>
        /// <param name="logScope">The scope of the message of log.</param>
        /// <param name="logSettings">The log settings of the caller.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="configurationName">The name of the configuration from which is being logged.</param>
        /// <param name="timeId">The time id in the configuration from which is being logged.</param>
        /// <param name="order">The order in the time id in the configuration which is being logged.</param>
        /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
        Task LogInformation<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null);

        /// <summary>
        /// Log a warning.
        /// </summary>
        /// <typeparam name="T">The type of the caller.</typeparam>
        /// <param name="logger">The logger to use.</param>
        /// <param name="logScope">The scope of the message of log.</param>
        /// <param name="logSettings">The log settings of the caller.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="configurationName">The name of the configuration from which is being logged.</param>
        /// <param name="timeId">The time id in the configuration from which is being logged.</param>
        /// <param name="order">The order in the time id in the configuration which is being logged.</param>
        /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
        Task LogWarning<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null);

        /// <summary>
        /// Log an error.
        /// </summary>
        /// <typeparam name="T">The type of the caller.</typeparam>
        /// <param name="logger">The logger to use.</param>
        /// <param name="logScope">The scope of the message of log.</param>
        /// <param name="logSettings">The log settings of the caller.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="configurationName">The name of the configuration from which is being logged.</param>
        /// <param name="timeId">The time id in the configuration from which is being logged.</param>
        /// <param name="order">The order in the time id in the configuration which is being logged.</param>
        /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
        Task LogError<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null);

        /// <summary>
        /// Log critical.
        /// </summary>
        /// <typeparam name="T">The type of the caller.</typeparam>
        /// <param name="logger">The logger to use.</param>
        /// <param name="logScope">The scope of the message of log.</param>
        /// <param name="logSettings">The log settings of the caller.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="configurationName">The name of the configuration from which is being logged.</param>
        /// <param name="timeId">The time id in the configuration from which is being logged.</param>
        /// <param name="order">The order in the time id in the configuration which is being logged.</param>
        /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
        Task LogCritical<T>(ILogger<T> logger, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null);

        /// <summary>
        /// Log a message.
        /// </summary>
        /// <typeparam name="T">The type of the caller.</typeparam>
        /// <param name="logger">The logger to use.</param>
        /// <param name="logLevel">The level of the log to write to.</param>
        /// <param name="logScope">The scope of the message of log.</param>
        /// <param name="logSettings">The log settings of the caller.</param>
        /// <param name="message">The message to log.</param>
        /// <param name="configurationName">The name of the configuration from which is being logged.</param>
        /// <param name="timeId">The time id in the configuration from which is being logged.</param>
        /// <param name="order">The order in the time id in the configuration which is being logged.</param>
        /// <param name="extraValuesToObfuscate">A list of extra values to obfuscate while writing logs. Used to obfuscate dynamic content such as authorization headers.</param>
        Task Log<T>(ILogger<T> logger, LogLevel logLevel, LogScopes logScope, LogSettings logSettings, string message, string configurationName, int timeId = 0, int order = 0, List<string> extraValuesToObfuscate = null);
    }
}
