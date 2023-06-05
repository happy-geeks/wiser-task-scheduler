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
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Interfaces;
using WiserTaskScheduler.Modules.GenerateCommunications.Interfaces;
using WiserTaskScheduler.Modules.GenerateCommunications.Models;

namespace WiserTaskScheduler.Modules.GenerateCommunications.Services;

public class GenerateCommunicationsService : IGenerateCommunicationsService, IActionsService, IScopedService
{
    private readonly IServiceProvider serviceProvider;
    private readonly IBodyService bodyService;
    private readonly ILogger<GenerateCommunicationsService> logger;
    private readonly ILogService logService;

    private string connectionString;
    
    public GenerateCommunicationsService(IServiceProvider serviceProvider, IBodyService bodyService, ILogger<GenerateCommunicationsService> logger, ILogService logService)
    {
        this.serviceProvider = serviceProvider;
        this.bodyService = bodyService;
        this.logger = logger;
        this.logService = logService;
    }
    
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        connectionString = configuration.ConnectionString;
        
        if (String.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException($"Configuration '{configuration.ServiceName}' has no connection string defined but contains active `GenerateCommunication` actions. Please provide a connection string.");
        }
        
        return Task.CompletedTask;
    }

    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var generateCommunication = (GenerateCommunicationModel)action;
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateCommunication.LogSettings, $"Generating communications of type '{generateCommunication.CommunicationType}' in time id: {generateCommunication.TimeId}, order: {generateCommunication.Order}", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);

        using var scope = serviceProvider.CreateScope();
        var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        var communicationsService = scope.ServiceProvider.GetRequiredService<ICommunicationsService>();

        await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);
        databaseConnection.ClearParameters();
        await databaseConnection.EnsureOpenConnectionForWritingAsync();
        await databaseConnection.EnsureOpenConnectionForReadingAsync();

        if (generateCommunication.SingleCommunication)
        {
            try
            {
                return await GenerateCommunicationAsync(generateCommunication, generateCommunication.UseResultSet, resultSets, databaseConnection, communicationsService, ReplacementHelper.EmptyRows, configurationServiceName);
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, generateCommunication.LogSettings, $"Failed to generate single communication due to exception:{Environment.NewLine}{e}", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);
                return new JObject()
                {
                    {"Identifier", ""},
                    {"CommunicationId", -1},
                    {"CommunicationType", generateCommunication.CommunicationType.ToString()},
                    {"SkipQueue", generateCommunication.SkipQueue}
                };
            }
        }
        
        var jArray = new JArray();
        
        var rows = ResultSetHelper.GetCorrectObject<JArray>(generateCommunication.UseResultSet, ReplacementHelper.EmptyRows, resultSets);
        for (var i = 0; i < rows.Count; i++)
        {
            var indexRows = new List<int> {i};
            try
            {
                jArray.Add(await GenerateCommunicationAsync(generateCommunication, $"{generateCommunication.UseResultSet}[{i}]", resultSets, databaseConnection, communicationsService, indexRows, configurationServiceName, forcedIndex: i));
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunBody, generateCommunication.LogSettings, $"Failed to generate communication in loop due to exception:{Environment.NewLine}{e}", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);
            }
        }
        
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateCommunication.LogSettings, $"Generated {rows.Count} communications of type '{generateCommunication.CommunicationType}' in time id: {generateCommunication.TimeId}, order: {generateCommunication.Order}", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);
        
        return new JObject
        {
            {"Results", jArray}
        };
    }

    private async Task<JObject> GenerateCommunicationAsync(GenerateCommunicationModel generateCommunication, string useResultSet, JObject resultSets, IDatabaseConnection databaseConnection, ICommunicationsService communicationsService, List<int> rows, string configurationServiceName, int forcedIndex = -1)
    {
        var identifier = generateCommunication.Identifier;
        var receivers = generateCommunication.Receiver;
        var receiverNames = generateCommunication.ReceiverName;
        var additionalReceivers = generateCommunication.AdditionalReceiver;
        var sender = generateCommunication.Sender;
        var senderName = generateCommunication.SenderName;
        var replyTo = generateCommunication.ReplyTo;
        var subject = generateCommunication.Subject;
        var body = String.Empty;

        // If a result set is used on the main level apply replacements to all properties that have been set.
        if (!String.IsNullOrWhiteSpace(useResultSet))
        {
            var keyParts = useResultSet.Split('.');
            var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, rows, resultSets);
            var remainingKey = keyParts.Length > 1 ? useResultSet.Substring(keyParts[0].Length + 1) : "";
            
            if (!String.IsNullOrWhiteSpace(identifier))
            {
                identifier = HandleReplacements(identifier, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(receivers))
            {
                receivers = HandleReplacements(receivers, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(receiverNames))
            {
                receiverNames = HandleReplacements(receiverNames, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(additionalReceivers))
            {
                additionalReceivers = HandleReplacements(additionalReceivers, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(sender))
            {
                sender = HandleReplacements(sender, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(senderName))
            {
                senderName = HandleReplacements(senderName, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(replyTo))
            {
                replyTo = HandleReplacements(replyTo, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
            
            if (!String.IsNullOrWhiteSpace(subject))
            {
                subject = HandleReplacements(subject, usingResultSet, remainingKey, rows, generateCommunication.HashSettings);
            }
        }

        if (generateCommunication.Body != null)
        {
            body = bodyService.GenerateBody(generateCommunication.Body, rows, resultSets, generateCommunication.HashSettings, forcedIndex);
        }

        // Create the communication object to place in queue or send directly.
        var singleCommunication = new SingleCommunicationModel()
        {
            Type = generateCommunication.CommunicationType
        };
        
        // Add receivers if they are provided.
        if (!String.IsNullOrWhiteSpace(receivers))
        {
            var receiverList = new List<CommunicationReceiverModel>();
            var receiverNamesParts = String.IsNullOrWhiteSpace(receiverNames) ? null : receiverNames.Split(';');
            var receiverParts = receivers.Split(';');
            var includeNames = receiverNamesParts != null && receiverNamesParts.Length == receiverParts.Length;
            
            for (var i = 0; i < receiverParts.Length; i++)
            {
                receiverList.Add(new CommunicationReceiverModel
                {
                    Address = receiverParts[i]
                });
                
                if (includeNames)
                {
                    receiverList.Last().DisplayName = receiverNamesParts[i];
                }
            }

            if (receiverList.Any())
            {
                singleCommunication.Receivers = receiverList;
            }
        }

        if (!String.IsNullOrWhiteSpace(sender))
        {
            singleCommunication.Sender = sender;
        }
        
        if (!String.IsNullOrWhiteSpace(senderName))
        {
            singleCommunication.SenderName = senderName;
        }

        if (!String.IsNullOrWhiteSpace(additionalReceivers))
        {
            singleCommunication.Bcc = additionalReceivers.Split(';').ToList();
        }
        
        if (!String.IsNullOrWhiteSpace(replyTo))
        {
            singleCommunication.ReplyTo = replyTo;
        }
        
        if (!String.IsNullOrWhiteSpace(subject))
        {
            singleCommunication.Subject = subject;
        }

        if (!String.IsNullOrWhiteSpace(body))
        {
            singleCommunication.Content = body;
        }
        
        var communicationId = 0;
        
        if (generateCommunication.SkipQueue)
        {
            communicationId = -1;

            try
            {
                switch (generateCommunication.CommunicationType)
                {
                    case CommunicationTypes.Email:
                        await communicationsService.SendEmailDirectlyAsync(singleCommunication, generateCommunication.SmtpSettings);
                        break;
                    case CommunicationTypes.Sms:
                        await communicationsService.SendSmsDirectlyAsync(singleCommunication, generateCommunication.SmsSettings);
                        break;
                    case CommunicationTypes.WhatsApp:
                        await communicationsService.SendWhatsAppDirectlyAsync(singleCommunication, generateCommunication.SmsSettings);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(generateCommunication.CommunicationType), generateCommunication.CommunicationType.ToString());
                }
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunBody, generateCommunication.LogSettings, $"Failed to directly send communication '{generateCommunication.Identifier}' using '{generateCommunication.CommunicationType}' due to exception:{Environment.NewLine}{e}", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);
            }
        }
        else
        {
            try
            {
                switch (generateCommunication.CommunicationType)
                {
                    case CommunicationTypes.Email:
                        communicationId = await communicationsService.SendEmailAsync(singleCommunication);
                        break;
                    case CommunicationTypes.Sms:
                        communicationId = await communicationsService.SendSmsAsync(singleCommunication);
                        break;
                    case CommunicationTypes.WhatsApp:
                        communicationId = await communicationsService.SendWhatsAppAsync(singleCommunication);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(generateCommunication.CommunicationType), generateCommunication.CommunicationType.ToString());
                }
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunBody, generateCommunication.LogSettings, $"Failed to place communication '{generateCommunication.Identifier}' in the queue using '{generateCommunication.CommunicationType}' due to exception:{Environment.NewLine}{e}", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);
            }
        }

        return new JObject()
        {
            {"Identifier", identifier},
            {"CommunicationId", communicationId},
            {"CommunicationType", generateCommunication.CommunicationType.ToString()},
            {"SkipQueue", generateCommunication.SkipQueue}
        };
    }

    private string HandleReplacements(string originalString, JObject usingResultSet, string remainingKey, List<int> rows, HashSettingsModel hashSettings)
    {
        var tuple = ReplacementHelper.PrepareText(originalString, usingResultSet, remainingKey, hashSettings);
        var result = tuple.Item1;
        var parameterKeys = tuple.Item2;
        result = ReplacementHelper.ReplaceText(result, rows, parameterKeys, usingResultSet, hashSettings);

        return result;
    }
}