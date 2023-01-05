using System.Collections.Generic;
using System.Threading.Tasks;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces;

public interface IErrorNotificationService
{
    /// <summary>
    /// Send an email to notify people about an error.
    /// </summary>
    /// <param name="emails">The email addresses to send the email to, seperated by semicolon (;).</param>
    /// <param name="subject">The subject of the email.</param>
    /// <param name="content">The content of the email.</param>
    /// <param name="logSettings">The log settings to use if the method fails.</param>
    /// <param name="logScope">The scope the logs needs to be written to if the method fails.</param>
    /// <param name="configurationName">The name of the configuration that is trying to send the notification to be used in the logs when the method fails.</param>
    /// <returns></returns>
    Task NotifyOfErrorByEmailAsync(string emails, string subject, string content, LogSettings logSettings, LogScopes logScope, string configurationName);
    
    /// <summary>
    /// Send an email to notify people about an error.
    /// </summary>
    /// <param name="emails">A list of email addresses to send the email to.</param>
    /// <param name="subject">The subject of the email.</param>
    /// <param name="content">The content of the email.</param>
    /// <param name="logSettings">The log settings to use if the method fails.</param>
    /// <param name="logScope">The scope the logs needs to be written to if the method fails.</param>
    /// <param name="configurationName">The name of the configuration that is trying to send the notification to be used in the logs when the method fails.</param>
    /// <returns></returns>
    Task NotifyOfErrorByEmailAsync(List<string> emails, string subject, string content, LogSettings logSettings, LogScopes logScope, string configurationName);
}