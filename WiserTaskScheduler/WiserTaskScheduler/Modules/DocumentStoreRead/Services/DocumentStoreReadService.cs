using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Enums;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.CleanupItems.Services;
using WiserTaskScheduler.Modules.DocumentStoreRead.Interfaces;
using WiserTaskScheduler.Modules.DocumentStoreRead.Models;

namespace WiserTaskScheduler.Modules.DocumentStoreRead.Services;

public class DocumentStoreReadService : IDocumentStoreReadService, IScopedService, IActionsService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<CleanupItemsService> logger;
    private const string WiserUsername = "WTS (Document Store Read)";

    public DocumentStoreReadService(IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupItemsService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;
    }

    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var documentStoreReadItem = (DocumentStoreReadModel)action;

        using var scope = serviceProvider.CreateScope();

        var documentStorageService = scope.ServiceProvider.GetRequiredService<IDocumentStorageService>();
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();

        var entitySettings = await wiserItemsService.GetEntityTypeSettingsAsync(documentStoreReadItem.EntityName);
        if (entitySettings.Id == 0)
        {
            entitySettings = null;
        }

        IReadOnlyCollection<(WiserItemModel model, string documentId)> items;
        try
        {

            items = await documentStorageService.GetItems("", new Dictionary<string, object>(), entitySettings);

            if (items.Count == 0)
            {
                return new JObject()
                {
                    { "Success", true },
                    { "ItemsRead", 0 }
                };
            }
        }
        catch (Exception e)
        {
            string message;
            if (String.IsNullOrEmpty(documentStoreReadItem.EntityName))
            {
                message = $"Failed to read wiser items from document store due to exception:\n{e}";
            }
            else
            {
                message = $"Failed to read wiser items of entity {documentStoreReadItem.EntityName} from document store due to exception:\n{e}";
            }
            
            await logService.LogError(
                logger, 
                LogScopes.RunStartAndStop, 
                documentStoreReadItem.LogSettings, 
                message,
                configurationServiceName,
                documentStoreReadItem.TimeId,
                documentStoreReadItem.Order);
            return new JObject()
            {
                { "Success", false },
                { "ItemsRead", 0 },
            };
        }
        
        try
        {
            foreach (var item in items)
            {
                if (!String.IsNullOrEmpty(documentStoreReadItem.EntityName) && item.model.EntityType != documentStoreReadItem.EntityName)
                {
                    continue;
                }
                await wiserItemsService.SaveAsync(item.model, username: documentStoreReadItem.UsernameForLogging ?? WiserUsername, storeTypeOverride: StoreType.Table, userId: documentStoreReadItem.UserId);
                await documentStorageService.DeleteItem(item.documentId, entitySettings);
            }
        }
        catch (Exception e)
        {
            string message;
            if (String.IsNullOrEmpty(documentStoreReadItem.EntityName))
            {
                message = $"Failed to store wiser items due to exception:\n{e}";
            }
            else
            {
                message = $"Failed to store wiser items of entity {documentStoreReadItem.EntityName} due to exception:\n{e}";
            }
            
            await logService.LogError(
                logger, 
                LogScopes.RunStartAndStop, 
                documentStoreReadItem.LogSettings, 
                message, 
                configurationServiceName, 
                documentStoreReadItem.TimeId, 
                documentStoreReadItem.Order);

            return new JObject()
            {
                { "Success", false },
                { "ItemsRead", 0 },
            };;
        }

        return new JObject()
        {
            { "Success", true },
            { "ItemsRead", items.Count },
        };
    }
}