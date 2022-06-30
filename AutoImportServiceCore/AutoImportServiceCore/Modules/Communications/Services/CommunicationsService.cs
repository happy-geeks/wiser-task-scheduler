using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Mail;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Communications.Interfaces;
using AutoImportServiceCore.Modules.Communications.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Communication.Enums;
using GeeksCoreLibrary.Modules.Communication.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using GclCommunicationsService = GeeksCoreLibrary.Modules.Communication.Services.CommunicationsService;

namespace AutoImportServiceCore.Modules.Communications.Services;

public class CommunicationsService : ICommunicationsService, IActionsService, IScopedService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<CommunicationsService> logger;

    private const string EmailSubjectForCommunicationError = "Error while sending communication";
    
    private DateTime lastErrorSent = DateTime.MinValue;
    private string connectionString;

    public CommunicationsService(IServiceProvider serviceProvider, ILogService logService, ILogger<CommunicationsService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;
    }

    /// <inheritdoc />
    public async Task Initialize(ConfigurationModel configuration)
    {
	    connectionString = configuration.ConnectionString;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var communication = (CommunicationModel) action;
        
        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        
        var connectionStringToUse = communication.ConnectionString ?? connectionString;
        await databaseConnection.ChangeConnectionStringsAsync(connectionStringToUse, connectionStringToUse);
        
        // Wiser Items Service requires dependency injection that results in the need of MVC services that are unavailable.
        // Get all other services and create the Wiser Items Service with one of the services missing.
        var objectService = scope.ServiceProvider.GetRequiredService<IObjectsService>();
        var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        var wiserItemsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<WiserItemsService>>();
        var gclCommunicationsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<GclCommunicationsService>>();
        
        var wiserItemsService = new WiserItemsService(databaseConnection, objectService, stringReplacementsService, null, databaseHelpersService, gclSettings, wiserItemsServiceLogger);
        var gclCommunicationsService = new GclCommunicationsService(gclSettings, gclCommunicationsServiceLogger, wiserItemsService, databaseConnection);

        switch (communication.Type)
        {
            case CommunicationTypes.Email:
                return await ProcessMailsAsync(communication, databaseConnection, gclCommunicationsService, configurationServiceName);
            case CommunicationTypes.Sms:
	            return await ProcessSmsAsync(communication, databaseConnection, gclCommunicationsService, configurationServiceName);
            default:
                throw new ArgumentOutOfRangeException(nameof(communication.Type), communication.Type.ToString());
        }
    }

    /// <summary>
    /// Process the emails that need to be send.
    /// </summary>
    /// <param name="communication">The communication information.</param>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="gclCommunicationsService">The communications service from the GCL to actually send out the emails.</param>
    /// <param name="configurationServiceName">The name of the configuration that is being executed.</param>
    /// <returns></returns>
    private async Task<JObject> ProcessMailsAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, GclCommunicationsService gclCommunicationsService, string configurationServiceName)
    {
	    var emails = await GetCommunicationsOfTypeAsync(communication, databaseConnection, configurationServiceName);

	    if (!emails.Any())
	    {
		    await logService.LogInformation(logger, LogScopes.RunStartAndStop, communication.LogSettings, "No emails found to be send.", configurationServiceName, communication.TimeId, communication.Order);
		    return new JObject()
		    {
			    {"Type", "Email"},
			    {"Processed", 0},
			    {"Failed", 0},
			    {"Total", 0}
		    };
	    }

	    var processed = 0;
	    var failed = 0;

	    foreach (var email in emails)
	    {
		    if (ShouldDelay(email) || email.AttemptCount >= communication.MaxNumberOfCommunicationAttempts)
		    {
			    continue;
		    }

		    string statusCode = null;
		    string statusMessage = null;
		    var sendErrorNotification = false;
		    
		    try
		    {
			    email.AttemptCount++;
			    await gclCommunicationsService.SendEmailDirectlyAsync(email, communication.SmtpSettings);
			    processed++;
			    databaseConnection.ClearParameters();
			    databaseConnection.AddParameter("processed_date", DateTime.Now);
		    }
		    catch (SmtpException smtpException)
		    {
			    failed++;
			    databaseConnection.ClearParameters();
			    statusCode = smtpException.StatusCode.ToString();
			    statusMessage = $"Attempt #{email.AttemptCount}:{Environment.NewLine}{smtpException}";
			    await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to send email for communication ID {email.Id} due to SMTP error:\n{smtpException}", configurationServiceName, communication.TimeId, communication.Order);

			    switch (smtpException.StatusCode)
			    {
				    case SmtpStatusCode.ServiceClosingTransmissionChannel:
				    case SmtpStatusCode.CannotVerifyUserWillAttemptDelivery:
				    case SmtpStatusCode.ServiceNotAvailable:
				    case SmtpStatusCode.MailboxBusy:
				    case SmtpStatusCode.LocalErrorInProcessing:
				    case SmtpStatusCode.InsufficientStorage:
				    case SmtpStatusCode.MailboxUnavailable:
				    case SmtpStatusCode.UserNotLocalTryAlternatePath:
				    case SmtpStatusCode.ExceededStorageAllocation:
				    case SmtpStatusCode.TransactionFailed:
				    case SmtpStatusCode.GeneralFailure:
					    sendErrorNotification = true;
					    break;
				    default:
					    // If another error has occured it will most likely not work other times.
					    email.AttemptCount = communication.MaxNumberOfCommunicationAttempts;
					    break;
			    }
		    }
		    catch (Exception e)
		    {
			    failed++;
			    databaseConnection.ClearParameters();
			    statusCode = "General exception";
			    statusMessage = $"Attempt #{email.AttemptCount}:{Environment.NewLine}{e}";
			    await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to send email for communication ID {email.Id} due to general error:\n{e}", configurationServiceName, communication.TimeId, communication.Order);

			    if (!e.Message.Contains("Mail API error", StringComparison.OrdinalIgnoreCase))
			    {
				    // If another error has occured it will most likely not work other times.
				    email.AttemptCount = communication.MaxNumberOfCommunicationAttempts;
			    }
		    }
		    
		    databaseConnection.AddParameter("attempt_count", email.AttemptCount);
			databaseConnection.AddParameter("status_code", statusCode);
			databaseConnection.AddParameter("status_message", statusMessage);
		    await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, email.Id);

		    if (sendErrorNotification)
		    {
			    await SendErrorNotification(communication, databaseConnection, email.AttemptCount, email.Subject, String.Join(';', email.Receivers.Select(x => x.Address)), statusMessage, CommunicationTypes.Email);
		    }
	    }
        
	    return new JObject()
	    {
		    {"Type", "Email"},
		    {"Processed", processed},
		    {"Failed", failed},
		    {"Total", processed + failed}
	    };
    }

    private async Task<JObject> ProcessSmsAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, GclCommunicationsService gclCommunicationsService, string configurationServiceName)
    {
	    var smsList = await GetCommunicationsOfTypeAsync(communication, databaseConnection, configurationServiceName);

	    if (!smsList.Any())
	    {
		    await logService.LogInformation(logger, LogScopes.RunStartAndStop, communication.LogSettings, "No text messages found to be send.", configurationServiceName, communication.TimeId, communication.Order);
		    return new JObject()
		    {
			    {"Type", "Sms"},
			    {"Processed", 0},
			    {"Failed", 0},
			    {"Total", 0}
		    };
	    }

	    var processed = 0;
	    var failed = 0;

	    foreach (var sms in smsList)
	    {
		    if (ShouldDelay(sms) || sms.AttemptCount >= communication.MaxNumberOfCommunicationAttempts)
		    {
			    continue;
		    }

		    string statusCode = null;
		    string statusMessage = null;

		    try
		    {
			    sms.AttemptCount++;
			    await gclCommunicationsService.SendSmsDirectlyAsync(sms, communication.SmsSettings);
			    processed++;
			    databaseConnection.ClearParameters();
			    databaseConnection.AddParameter("processed_date", DateTime.Now);
		    }
		    catch (Exception e)
		    {
			    failed++;
			    databaseConnection.ClearParameters();
			    statusCode = "General exception";
			    statusMessage = $"Attempt #{sms.AttemptCount}:{Environment.NewLine}{e}";
			    await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to send sms for communication ID {sms.Id} due to general error:\n{e}", configurationServiceName, communication.TimeId, communication.Order);

			    sms.AttemptCount = communication.MaxNumberOfCommunicationAttempts;
		    }

		    databaseConnection.AddParameter("attempt_count", sms.AttemptCount);
		    databaseConnection.AddParameter("status_code", statusCode);
		    databaseConnection.AddParameter("status_message", statusMessage);
		    await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, sms.Id);
	    }
	    
	    return new JObject()
	    {
		    {"Type", "Sms"},
		    {"Processed", processed},
		    {"Failed", failed},
		    {"Total", processed + failed}
	    };
    }

    /// <summary>
    /// Get all communications that need to be send.
    /// </summary>
    /// <param name="communication">The communication information.</param>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="configurationServiceName">The name of the configuration that is being executed.</param>
    /// <returns></returns>
    private async Task<List<SingleCommunicationModel>> GetCommunicationsOfTypeAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, string configurationServiceName)
    {
        databaseConnection.AddParameter("communicationType", communication.Type.ToString());
        databaseConnection.AddParameter("now", DateTime.Now);
        databaseConnection.AddParameter("maxDelayInHours", communication.MaxDelayInHours);
        databaseConnection.AddParameter("maxNumberOfCommunicationAttempts", communication.MaxNumberOfCommunicationAttempts);
	    
        var dataTable = await databaseConnection.GetAsync($@"SELECT
	id,
	communication_id,
	receiver,
	receiver_name,
	cc,
	bcc,
	reply_to,
	reply_to_name,
	sender,
	sender_name,
	subject,
	content,
	uploaded_file,
	uploaded_filename,
	attachment_urls,
	wiser_item_files,
	communicationtype,
	send_date,
	attempt_count,
	last_attempt
FROM {WiserTableNames.WiserCommunicationGenerated}
WHERE
	communicationtype = ?communicationType
	AND send_date <= ?now
	AND IF(?maxDelayInHours > 0, DATE_ADD(send_date, INTERVAL ?maxDelayInHours HOUR), '2199-01-01 00:00:00') >= ?now
	AND processed_date IS NULL
	AND attempt_count < ?maxNumberOfCommunicationAttempts");

        var communications = new List<SingleCommunicationModel>();
        
        foreach (DataRow row in dataTable.Rows)
        {
	        try
	        {
		        communications.Add(GetModel(row));
	        }
	        catch (Exception e)
	        {
		        await logService.LogInformation(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to create model for communication ID '{Convert.ToInt32(row["id"])}' with exception:\n{e}", configurationServiceName, communication.TimeId, communication.Order);
	        }
        }

        return communications;
    }

    /// <summary>
    /// Get the model of a single communication from a row in the database.
    /// </summary>
    /// <param name="row">The row to get the model from.</param>
    /// <returns>Returns a single communication model.</returns>
    private SingleCommunicationModel GetModel(DataRow row)
    {
	    var receiverAddresses = new List<CommunicationReceiverModel>();
        var rawReceiverAddresses = row.Field<string>("receiver");
        var rawReceiverNames = row.Field<string>("receiver_name");
        if (!String.IsNullOrWhiteSpace(rawReceiverAddresses))
        {
	        var addresses = rawReceiverAddresses.Split(new[] {',', ';'});
	        var names = String.IsNullOrWhiteSpace(rawReceiverNames) ? new string[addresses.Length] : rawReceiverNames.Split(new[] {',', ';'});

	        for (var i = 0; i < addresses.Length; i++)
	        {
		        if (String.IsNullOrWhiteSpace(addresses[i]))
		        {
			        continue;
		        }
		        
		        receiverAddresses.Add(new CommunicationReceiverModel()
		        {
			        Address = addresses[i],
			        DisplayName = names[i]
		        });
	        }
        }
        
        var bccAddresses = new List<string>();
        var rawBccValue = row.Field<string>("bcc");
        if (!String.IsNullOrWhiteSpace(rawBccValue))
        {
	        bccAddresses.AddRange(rawBccValue.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries));
        }

        var ccAddresses = new List<string>();
        var rawCcValue = row.Field<string>("cc");
        if (!String.IsNullOrWhiteSpace(rawCcValue))
        {
	        ccAddresses.AddRange(rawCcValue.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries));
        }

        var attachmentUrls = new List<string>();
        var rawAttachmentUrls = row.Field<string>("attachment_urls");
        if (!String.IsNullOrWhiteSpace(rawAttachmentUrls))
        {
	        attachmentUrls.AddRange(rawAttachmentUrls.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries));
        }

        var wiserItemFiles = new List<ulong>();
        var rawWiserItemFiles = row.Field<string>("wiser_item_files");
        if (!String.IsNullOrWhiteSpace(rawWiserItemFiles))
        {
	        wiserItemFiles.AddRange(rawWiserItemFiles.Split(new[] {',', ';'}, StringSplitOptions.RemoveEmptyEntries).Select(UInt64.Parse).ToList());
        }

        var singleCommunication = new SingleCommunicationModel()
        {
	        Id = Convert.ToInt32(row["id"]),
	        CommunicationId = Convert.ToInt32(row["communication_id"]),
	        Receivers = receiverAddresses,
	        Cc = ccAddresses,
	        Bcc = bccAddresses,
	        ReplyTo = row.Field<string>("reply_to"),
	        ReplyToName = row.Field<string>("reply_to_name"),
	        Sender = row.Field<string>("sender"),
	        SenderName = row.Field<string>("sender_name"),
	        Subject = row.Field<string>("subject"),
	        Content = row.Field<string>("content"),
	        UploadedFile = row.Field<byte[]>("uploaded_file"),
	        UploadedFileName = row.Field<string>("uploaded_filename"),
	        AttachmentUrls = attachmentUrls,
	        WiserItemFiles = wiserItemFiles,
	        Type = Enum.Parse<CommunicationTypes>(row.Field<string>("communicationtype"), true),
	        SendDate = row.Field<DateTime>("send_date"),
	        AttemptCount = row.Field<int>("attempt_count"),
	        LastAttempt = row.Field<DateTime?>("last_attempt")
        };
        
        //TODO attachments

        return singleCommunication;
    }

    /// <summary>
    /// Check if the communication needs to be delayed.
    /// </summary>
    /// <param name="singleCommunication">The communication that needs to be checked.</param>
    /// <returns>Returns true if the communication needs to be delayed, otherwise false.</returns>
    private bool ShouldDelay(SingleCommunicationModel singleCommunication)
    {
	    if (singleCommunication.AttemptCount == 0 || !singleCommunication.LastAttempt.HasValue)
	    {
		    return false;
	    }
	    
	    var totalMinutesSinceLastAttempt = (DateTime.Now - singleCommunication.LastAttempt.Value).TotalMinutes;

	    return (singleCommunication.AttemptCount == 1 && totalMinutesSinceLastAttempt < 1)
	           || (singleCommunication.AttemptCount == 2 && totalMinutesSinceLastAttempt < 5)
	           || (singleCommunication.AttemptCount == 3 && totalMinutesSinceLastAttempt < 15)
	           || (singleCommunication.AttemptCount == 4 && totalMinutesSinceLastAttempt < 60)
	           || (singleCommunication.AttemptCount >= 5 && totalMinutesSinceLastAttempt < 1440);
    }

    private async Task SendErrorNotification(CommunicationModel communication, IDatabaseConnection databaseConnection, int attemptCount, string subject, string receivers, string statusMessage, CommunicationTypes type)
    {
	    // Check if an error email needs to be send.
	    if (attemptCount < communication.MaxNumberOfCommunicationAttempts || String.IsNullOrWhiteSpace(communication.EmailAddressForErrorNotifications) || subject == EmailSubjectForCommunicationError || lastErrorSent.Date >= DateTime.Today)
	    {
		    return;
	    }

	    var lastErrorMail = await databaseConnection.GetAsync($"SELECT MAX(send_date) AS lastErrorMail FROM {WiserTableNames.WiserCommunicationGenerated} WHERE is_internal_error_mail = 1");
	    if (lastErrorMail.Rows[0].Field<object>("lastErrorMail") != null && lastErrorMail.Rows[0].Field<DateTime>("lastErrorMail").Date == DateTime.Today)
	    {
		    lastErrorSent = lastErrorMail.Rows[0].Field<DateTime>("lastErrorMail").Date;
		    return;
	    }
			    
	    databaseConnection.ClearParameters();
	    databaseConnection.AddParameter("receiver", communication.EmailAddressForErrorNotifications);
	    databaseConnection.AddParameter("sender", communication.EmailAddressForErrorNotifications);
	    databaseConnection.AddParameter("subject", EmailSubjectForCommunicationError);
	    databaseConnection.AddParameter("content", $"<p>Failed to send {type} to '{receivers}'.</p><p>Error log:</p><pre>{statusMessage}</pre>");
	    databaseConnection.AddParameter("communicationtype", "email");
	    databaseConnection.AddParameter("send_date", DateTime.Now);
	    databaseConnection.AddParameter("is_internal_error_mail", true);
	    await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, false);
	    lastErrorSent = DateTime.Now;
    }
}