﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net.Mail;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Communication.Enums;
using GeeksCoreLibrary.Modules.Communication.Extensions;
using GeeksCoreLibrary.Modules.Communication.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.DataSelector.Interfaces;
using GeeksCoreLibrary.Modules.DataSelector.Models;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Communications.Interfaces;
using WiserTaskScheduler.Modules.Communications.Models;
using IGclCommunicationsService = GeeksCoreLibrary.Modules.Communication.Interfaces.ICommunicationsService;

namespace WiserTaskScheduler.Modules.Communications.Services;

public class CommunicationsService(IServiceProvider serviceProvider, ILogService logService, ILogger<CommunicationsService> logger) : ICommunicationsService, IActionsService, IScopedService
{
    private const string EmailSubjectForCommunicationError = "Error while sending communication";

    private DateTime lastErrorSent = DateTime.MinValue;
    private string connectionString;
    private static readonly char[] Separator = [',', ';'];

    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        connectionString = configuration.ConnectionString;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var communication = (CommunicationModel) action;

        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        var connectionStringToUse = communication.ConnectionString ?? connectionString;
        await databaseConnection.ChangeConnectionStringsAsync(connectionStringToUse, connectionStringToUse);

        var gclCommunicationsService = scope.ServiceProvider.GetRequiredService<IGclCommunicationsService>();
        var dataSelectorsService = scope.ServiceProvider.GetRequiredService<IDataSelectorsService>();
        var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();

        await GenerateCommunicationsAsync(communication, databaseConnection, gclCommunicationsService, dataSelectorsService, stringReplacementsService, configurationServiceName);

        return communication.Type switch
        {
            CommunicationTypes.Email => await ProcessMailsAsync(communication, databaseConnection, gclCommunicationsService, configurationServiceName),
            CommunicationTypes.Sms => await ProcessSmsAsync(communication, databaseConnection, gclCommunicationsService, configurationServiceName),
            CommunicationTypes.WhatsApp => await ProcessWhatsAppAsync(communication, databaseConnection, gclCommunicationsService, configurationServiceName),
            _ => throw new ArgumentOutOfRangeException(nameof(communication.Type), communication.Type.ToString(), null)
        };
    }

    /// <summary>
    /// Generate communications that need to be send.
    /// </summary>
    /// <param name="communication">The communication information.</param>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="gclCommunicationsService">The communications service from the GCL to store the generated communications.</param>
    /// <param name="dataSelectorsService">The data selectors service to use.</param>
    /// <param name="stringReplacementsService">The string replacements service from the GCL to use.</param>
    /// <param name="configurationServiceName">The name of the configuration that is being executed.</param>
    private async Task GenerateCommunicationsAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, IGclCommunicationsService gclCommunicationsService, IDataSelectorsService dataSelectorsService, IStringReplacementsService stringReplacementsService, string configurationServiceName)
    {
        var communicationSettings = await gclCommunicationsService.GetSettingsAsync(communication.Type);

        foreach (var communicationSetting in communicationSettings)
        {
            var contentSettings = communicationSetting.Settings.Single(x => x.Type == communication.Type);
            var lastProcessed = communicationSetting.LastProcessed.SingleOrDefault(x => x.Type == communication.Type);
            if (lastProcessed == null)
            {
                await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"There is no 'LastProcessed' for '{communication.Type}' with ID '{communicationSetting.Id}'. No communication has been generated.", configurationServiceName, communication.TimeId, communication.Order);
                continue;
            }

            // Check if the communication needs to be generated based on it's type and associated settings.
            switch (communicationSetting.SendTriggerType)
            {
                case SendTriggerTypes.Direct:
                case SendTriggerTypes.Fixed:
                    if (!communicationSetting.TriggerStart.HasValue ||
                        communicationSetting.TriggerStart > DateTime.Now ||
                        lastProcessed.DateTime >= communicationSetting.TriggerStart)
                    {
                        continue;
                    }

                    break;
                case SendTriggerTypes.Recurring:
                    var currentDate = DateTime.Now;

                    // Don't send the communication if we don't have all required values or if today is not between the start and end date.
                    if (!communicationSetting.TriggerStart.HasValue ||
                        !communicationSetting.TriggerEnd.HasValue ||
                        !communicationSetting.TriggerTime.HasValue ||
                        communicationSetting.TriggerStart > currentDate ||
                        communicationSetting.TriggerEnd < currentDate)
                    {
                        continue;
                    }

                    // Calculate the next date and time that this should be processed.
                    var nextDateTimeToProcess = new DateTime(lastProcessed.DateTime.Year, lastProcessed.DateTime.Month, lastProcessed.DateTime.Day, communicationSetting.TriggerTime.Value.Hours, communicationSetting.TriggerTime.Value.Minutes, 0);
                    switch (communicationSetting.TriggerPeriodType)
                    {
                        case TriggerPeriodTypes.Week:
                            // Always start at the next day, because we never send communication more than once a day and that makes the calculations below easier.
                            nextDateTimeToProcess = nextDateTimeToProcess.AddDays(1);
                            while (!communicationSetting.TriggerWeekDays.Value.IsWeekday(nextDateTimeToProcess.DayOfWeek))
                            {
                                nextDateTimeToProcess = nextDateTimeToProcess.AddDays(1);
                            }

                            // Now we need to check the week of year. If the next date to process is in the same week, we can always handle that communication because then it's been set to run at multiple days per week.
                            // If it's not the same week, we need to check if it's the correct week, because it's possible to set the communication to be sent every 3 weeks for example.
                            var newWeekNumber = CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(nextDateTimeToProcess, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            var lastProcessedWeekNumber = CultureInfo.CurrentCulture.Calendar.GetWeekOfYear(lastProcessed.DateTime, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
                            if (newWeekNumber != lastProcessedWeekNumber)
                            {
                                // If the next date to process is a new week, add the amount of weeks that the user indicated, minus 1 because we are already 1 week ahead at this point.
                                nextDateTimeToProcess = nextDateTimeToProcess.AddDays(7 * (communicationSetting.TriggerPeriodValue - 1));
                            }

                            break;
                        case TriggerPeriodTypes.Month:
                            nextDateTimeToProcess = nextDateTimeToProcess.AddMonths(communicationSetting.TriggerPeriodValue);
                            nextDateTimeToProcess = new DateTime(nextDateTimeToProcess.Year, nextDateTimeToProcess.Month, Math.Min(DateTime.DaysInMonth(currentDate.Year, currentDate.Month), communicationSetting.TriggerDayOfMonth));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(communicationSetting.TriggerPeriodType), communicationSetting.TriggerPeriodType.ToString(), null);
                    }

                    // Don't send the communication if the next date and time is in the future.
                    if (nextDateTimeToProcess > currentDate)
                    {
                        continue;
                    }

                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(communicationSetting.SendTriggerType), communicationSetting.SendTriggerType.ToString(), null);
            }

            var receivers = new Dictionary<string, JToken>();

            // Retrieve all receivers based on settings.
            if (communicationSetting.ReceiversQueryId > 0 || communicationSetting.ReceiversDataSelectorId > 0)
            {
                if (String.IsNullOrWhiteSpace(contentSettings.Selector))
                {
                    await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"No selector has been provided for communication with ID '{communicationSetting.Id}'. No communication has been generated.", configurationServiceName, communication.TimeId, communication.Order);
                    continue;
                }

                var dataSelectorSettings = new DataSelectorRequestModel
                {
                    DataSelectorId = communicationSetting.ReceiversDataSelectorId,
                    QueryId = communicationSetting.ReceiversQueryId.ToString().EncryptWithAesWithSalt(withDateTime: true)
                };

                var (results, _, _) = await dataSelectorsService.GetJsonResponseAsync(dataSelectorSettings, true);

                foreach (var item in results)
                {
                    var receiver = item.Value<string>(contentSettings.Selector);
                    if (String.IsNullOrWhiteSpace(receiver))
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, communication.LogSettings, $"Could not get receiver from data for communication with ID '{communicationSetting.Id}'. Skipped line: {JsonConvert.SerializeObject(item)}", configurationServiceName, communication.TimeId, communication.Order);
                        continue;
                    }

                    if (receivers.ContainsKey(receiver))
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, communication.LogSettings, $"Duplicate receiver ({receiver}) for communication with ID '{communicationSetting.Id}'. Skipped line: {JsonConvert.SerializeObject(item)}", configurationServiceName, communication.TimeId, communication.Order);
                        continue;
                    }

                    receivers.Add(receiver, item);
                }
            }
            else if (communicationSetting.ReceiversList.Any())
            {
                foreach (var receiver in communicationSetting.ReceiversList)
                {
                    if (receivers.ContainsKey(receiver))
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, communication.LogSettings, $"Duplicate receiver ({receiver}) for communication with ID '{communicationSetting.Id}'.", configurationServiceName, communication.TimeId, communication.Order);
                        continue;
                    }

                    receivers.Add(receiver, null);
                }
            }

            if (!receivers.Any())
            {
                await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, "There are no receivers for the communication with ID communication. No communication has been generated.", configurationServiceName, communication.TimeId, communication.Order);
                continue;
            }

            // Generate communication for each receiver.
            foreach (var receiver in receivers)
            {
                var subject = contentSettings.Subject;
                var content = contentSettings.Content;

                var dataSelectorSettings = new DataSelectorRequestModel
                {
                    DataSelectorId = communicationSetting.ContentDataSelectorId,
                    QueryId = communicationSetting.ContentQueryId.ToString().EncryptWithAesWithSalt(withDateTime: true)
                };

                if (!String.IsNullOrWhiteSpace(subject))
                {
                    if (receiver.Value != null)
                    {
                        subject = stringReplacementsService.DoReplacements(subject, receiver.Value);
                    }

                    // Replace content data selector or query for each receiver.
                    if (communicationSetting.ContentDataSelectorId > 0 || communicationSetting.ContentQueryId > 0)
                    {
                        dataSelectorSettings.OutputTemplate = subject;
                        var (result, _, _) = await dataSelectorsService.ToHtmlAsync(dataSelectorSettings);
                        if (!String.IsNullOrWhiteSpace(result))
                        {
                            subject = result;
                        }
                    }
                }

                if (!String.IsNullOrWhiteSpace(content))
                {
                    if (receiver.Value != null)
                    {
                        content = stringReplacementsService.DoReplacements(content, receiver.Value);
                    }

                    // Replace content data selector or query for each receiver.
                    if (communicationSetting.ContentDataSelectorId > 0 || communicationSetting.ContentQueryId > 0)
                    {
                        dataSelectorSettings.OutputTemplate = content;
                        var (result, _, _) = await dataSelectorsService.ToHtmlAsync(dataSelectorSettings);
                        if (!String.IsNullOrWhiteSpace(result))
                        {
                            content = result;
                        }
                    }
                }

                switch (communication.Type)
                {
                    case CommunicationTypes.Email:
                        await gclCommunicationsService.SendEmailAsync(receiver.Key, subject, content);
                        break;
                    case CommunicationTypes.Sms:
                        await gclCommunicationsService.SendSmsAsync(receiver.Key, content);
                        break;
                    case CommunicationTypes.WhatsApp:
                        await gclCommunicationsService.SendWhatsAppAsync(receiver.Key, content);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(communication.Type), communication.Type.ToString(), null);
                }
            }

            lastProcessed.DateTime = DateTime.Now;
            databaseConnection.AddParameter("id", communicationSetting.Id);
            databaseConnection.AddParameter("lastProcessed", JsonConvert.SerializeObject(communicationSetting.LastProcessed, Formatting.Indented));
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserCommunication} SET last_processed = ?lastProcessed WHERE id = ?id");
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
    private async Task<JObject> ProcessMailsAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, IGclCommunicationsService gclCommunicationsService, string configurationServiceName)
    {
        var emails = await GetCommunicationsOfTypeAsync(communication, databaseConnection, configurationServiceName);

        if (!emails.Any())
        {
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, communication.LogSettings, "No emails found to be send.", configurationServiceName, communication.TimeId, communication.Order);
            return new JObject
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
                statusMessage = email.StatusMessage;
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
                sendErrorNotification = true;

                databaseConnection.ClearParameters();
                statusCode = "General exception";
                statusMessage = $"Attempt #{email.AttemptCount}:{Environment.NewLine}{e}";
                await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to send email for communication ID {email.Id} due to general error:\n{e}", configurationServiceName, communication.TimeId, communication.Order);
            }

            databaseConnection.AddParameter("attempt_count", email.AttemptCount);
            databaseConnection.AddParameter("last_attempt", DateTime.Now);
            databaseConnection.AddParameter("status_code", statusCode);
            databaseConnection.AddParameter("status_message", statusMessage);
            await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, email.Id);

            if (sendErrorNotification)
            {
                await SendErrorNotification(communication, databaseConnection, email, statusMessage);
            }
        }

        return new JObject
        {
            {"Type", "Email"},
            {"Processed", processed},
            {"Failed", failed},
            {"Total", processed + failed}
        };
    }

    private async Task<JObject> ProcessSmsAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, IGclCommunicationsService gclCommunicationsService, string configurationServiceName)
    {
        var smsList = await GetCommunicationsOfTypeAsync(communication, databaseConnection, configurationServiceName);

        if (!smsList.Any())
        {
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, communication.LogSettings, "No text messages found to be send.", configurationServiceName, communication.TimeId, communication.Order);
            return new JObject
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
            databaseConnection.AddParameter("last_attempt", DateTime.Now);
            databaseConnection.AddParameter("status_code", statusCode);
            databaseConnection.AddParameter("status_message", statusMessage);
            await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, sms.Id);
        }

        return new JObject
        {
            {"Type", "Sms"},
            {"Processed", processed},
            {"Failed", failed},
            {"Total", processed + failed}
        };
    }

    private async Task<JObject> ProcessWhatsAppAsync(CommunicationModel communication, IDatabaseConnection databaseConnection, IGclCommunicationsService gclCommunicationsService, string configurationServiceName)
    {
        var whatsAppList = await GetCommunicationsOfTypeAsync(communication, databaseConnection, configurationServiceName);

        if (!whatsAppList.Any())
        {
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, communication.LogSettings, "No text messages found to be send.", configurationServiceName, communication.TimeId, communication.Order);
            return new JObject
            {
                {"Type", "WhatsApp"},
                {"Processed", 0},
                {"Failed", 0},
                {"Total", 0}
            };
        }

        var processed = 0;
        var failed = 0;

        foreach (var whatsApp in whatsAppList)
        {
            if (ShouldDelay(whatsApp) || whatsApp.AttemptCount >= communication.MaxNumberOfCommunicationAttempts)
            {
                continue;
            }

            string statusCode = null;
            string statusMessage = null;

            try
            {
                whatsApp.AttemptCount++;
                await gclCommunicationsService.SendWhatsAppDirectlyAsync(whatsApp, communication.SmsSettings);
                processed++;
                databaseConnection.ClearParameters();
                databaseConnection.AddParameter("processed_date", DateTime.Now);
            }
            catch (Exception e)
            {
                failed++;
                databaseConnection.ClearParameters();
                statusCode = "General exception";
                statusMessage = $"Attempt #{whatsApp.AttemptCount}:{Environment.NewLine}{e}";
                await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to send whatsApp for communication ID {whatsApp.Id} due to general error:\n{e}", configurationServiceName, communication.TimeId, communication.Order);

                whatsApp.AttemptCount = communication.MaxNumberOfCommunicationAttempts;
            }

            databaseConnection.AddParameter("attempt_count", whatsApp.AttemptCount);
            databaseConnection.AddParameter("last_attempt", DateTime.Now);
            databaseConnection.AddParameter("status_code", statusCode);
            databaseConnection.AddParameter("status_message", statusMessage);
            await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, whatsApp.Id);
        }

        return new JObject
        {
            {"Type", "WhatsApp"},
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

        var dataTable = await databaseConnection.GetAsync($"""
                                                           SELECT
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
                                                           	AND attempt_count < ?maxNumberOfCommunicationAttempts
                                                           """);

        var communications = new List<SingleCommunicationModel>();

        foreach (DataRow row in dataTable.Rows)
        {
            try
            {
                communications.Add(GetModel(row));
            }
            catch (Exception exception)
            {
                await logService.LogError(logger, LogScopes.RunBody, communication.LogSettings, $"Failed to create model for communication ID '{Convert.ToInt32(row["id"])}' with exception:\n{exception}", configurationServiceName, communication.TimeId, communication.Order);
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
            var addresses = rawReceiverAddresses.Split(Separator);
            var names = String.IsNullOrWhiteSpace(rawReceiverNames) ? new string[addresses.Length] : rawReceiverNames.Split(Separator);

            for (var i = 0; i < addresses.Length; i++)
            {
                if (String.IsNullOrWhiteSpace(addresses[i]))
                {
                    continue;
                }

                receiverAddresses.Add(new CommunicationReceiverModel
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
            bccAddresses.AddRange(rawBccValue.Split(Separator, StringSplitOptions.RemoveEmptyEntries));
        }

        var ccAddresses = new List<string>();
        var rawCcValue = row.Field<string>("cc");
        if (!String.IsNullOrWhiteSpace(rawCcValue))
        {
            ccAddresses.AddRange(rawCcValue.Split(Separator, StringSplitOptions.RemoveEmptyEntries));
        }

        var attachmentUrls = new List<string>();
        var rawAttachmentUrls = row.Field<string>("attachment_urls");
        if (!String.IsNullOrWhiteSpace(rawAttachmentUrls))
        {
            attachmentUrls.AddRange(rawAttachmentUrls.Split(Separator, StringSplitOptions.RemoveEmptyEntries));
        }

        var wiserItemFiles = new List<ulong>();
        var rawWiserItemFiles = row.Field<string>("wiser_item_files");
        if (!String.IsNullOrWhiteSpace(rawWiserItemFiles))
        {
            wiserItemFiles.AddRange(rawWiserItemFiles.Split(Separator, StringSplitOptions.RemoveEmptyEntries).Select(UInt64.Parse).ToList());
        }

        var singleCommunication = new SingleCommunicationModel
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

    private async Task SendErrorNotification(CommunicationModel communication, IDatabaseConnection databaseConnection, SingleCommunicationModel singleCommunicationModel, string statusMessage)
    {
        // Check if an error email needs to be send.
        if (singleCommunicationModel.AttemptCount < communication.MaxNumberOfCommunicationAttempts || String.IsNullOrWhiteSpace(communication.EmailAddressForErrorNotifications) || singleCommunicationModel.Subject == EmailSubjectForCommunicationError || lastErrorSent.Date >= DateTime.Today)
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
        databaseConnection.AddParameter("sender", singleCommunicationModel.Sender);
        databaseConnection.AddParameter("sender_name", singleCommunicationModel.SenderName);
        databaseConnection.AddParameter("subject", EmailSubjectForCommunicationError);
        databaseConnection.AddParameter("content", $"<p>Failed to send {singleCommunicationModel.Type} to '{String.Join(';', singleCommunicationModel.Receivers.Select(x => x.Address))}'.</p><p>Error log:</p><pre>{statusMessage}</pre>");
        databaseConnection.AddParameter("communicationtype", "email");
        databaseConnection.AddParameter("send_date", DateTime.Now);
        databaseConnection.AddParameter("is_internal_error_mail", true);
        await databaseConnection.InsertOrUpdateRecordBasedOnParametersAsync(WiserTableNames.WiserCommunicationGenerated, false);
        lastErrorSent = DateTime.Now;
    }
}