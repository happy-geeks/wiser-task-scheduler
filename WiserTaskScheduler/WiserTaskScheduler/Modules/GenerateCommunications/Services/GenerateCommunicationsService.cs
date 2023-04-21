using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Modules.Communication.Interfaces;
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
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateCommunication.LogSettings, $"", configurationServiceName, generateCommunication.TimeId, generateCommunication.Order);

        using var scope = serviceProvider.CreateScope();
        var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        var communicationsService = scope.ServiceProvider.GetRequiredService<ICommunicationsService>();

        await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);
        databaseConnection.ClearParameters();
        await databaseConnection.EnsureOpenConnectionForWritingAsync();
        await databaseConnection.EnsureOpenConnectionForReadingAsync();
        
        if (generateCommunication != null)
        {
            var body = bodyService.GenerateBody(generateCommunication.Body, )
        }
        
        throw new System.NotImplementedException();
    }

    private async Task<JObject> GenerateCommunicationAsync(GenerateCommunicationModel generateCommunication, JObject resultSets, IDatabaseConnection databaseConnection, ICommunicationsService communicationsService, List<int> rows, string configurationServiceName)
    {
        var receivers = generateCommunication.Receiver;
        var receiverNames = generateCommunication.ReceiverName;
        var sender = generateCommunication.Sender;
        var senderName = generateCommunication.SenderName;
        var replyToEmail = generateCommunication.ReplyToEmail;
        var subject = generateCommunication.Subject;

        if (!String.IsNullOrWhiteSpace(generateCommunication.UseResultSet))
        {
            var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(generateCommunication.UseResultSet, ReplacementHelper.EmptyRows, resultSets);
            
            if (!String.IsNullOrWhiteSpace(receivers))
            {
                
            }
        }
    }
}