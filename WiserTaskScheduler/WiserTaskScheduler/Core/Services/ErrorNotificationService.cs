using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Communication.Enums;
using GeeksCoreLibrary.Modules.Communication.Interfaces;
using GeeksCoreLibrary.Modules.Communication.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Services;

public class ErrorNotificationService : IErrorNotificationService, ISingletonService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<ErrorNotificationService> logger;

    public ErrorNotificationService(IServiceProvider serviceProvider, ILogService logService, ILogger<ErrorNotificationService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;
    }
    
    /// <inheritdoc />
#pragma warning disable CS1998
    public async Task NotifyOfErrorByEmailAsync(string emails, string subject, string content, LogSettings logSettings, LogScopes logScope, string configurationName)
    {
#if !DEBUG
        // Only send mails for production Wiser Task Schedulers to prevent exceptions during developing/testing to trigger it.
        if (String.IsNullOrWhiteSpace(emails))
        {
            return;
        }

        var emailList = emails.Split(";").ToList();
        await NotifyOfErrorByEmailAsync(emailList, subject, content, logSettings, logScope, configurationName);
#endif
    }
#pragma warning restore CS1998

    /// <inheritdoc />
    public async Task NotifyOfErrorByEmailAsync(List<string> emails, string subject, string content, LogSettings logSettings, LogScopes logScope, string configurationName)
    {
        if (!emails.Any())
        {
            return;
        }
        
        var receivers = emails.Select(email => new CommunicationReceiverModel() {Address = email}).ToList();

        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        
        // If there are no settings provided to send an email abort.
        var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        if (gclSettings.Value.SmtpSettings == null)
        {
            await logService.LogWarning(logger, logScope, logSettings, $"Service '{configurationName}' has email addresses declared to receive error notifications but not SMTP settings have been provided.", "Core");
            return;
        }
        
        var communicationsService = scope.ServiceProvider.GetRequiredService<ICommunicationsService>();

        try
        {
            var email = new SingleCommunicationModel()
            {
                Type = CommunicationTypes.Email,
                Receivers = receivers,
                Cc = new List<string>(),
                Bcc = new List<string>(),
                Subject = subject,
                Content = content,
                Sender = gclSettings.Value.SmtpSettings.SenderEmailAddress,
                SenderName = gclSettings.Value.SmtpSettings.SenderName
            };

            await communicationsService.SendEmailDirectlyAsync(email);
        }
        catch (Exception e)
        {
            await logService.LogError(logger, logScope, logSettings, $"Failed to send an error notification to emails '{string.Join(';', emails)}'.{Environment.NewLine}Exception: {e}", configurationName);
        }
    }
}