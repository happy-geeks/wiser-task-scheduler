using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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

    private string connectionString;

    public DocumentStoreReadService(IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupItemsService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;
    }

    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        connectionString = configuration.ConnectionString;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var documentStoreReadItem = (DocumentStoreReadModel)action;

        using var scope = serviceProvider.CreateScope();
        var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();

        var prefix = await wiserItemsService.GetTablePrefixForEntityAsync(documentStoreReadItem.EntityName);
        var items = new List<WiserItemModel>();
        
        try
        {
            var dataTable = await databaseConnection.GetAsync($"""
                SELECT id
                FROM {prefix}{WiserTableNames.WiserItem}
                WHERE entity_type = ?entityType
                AND (
	                json_last_processed_date IS NULL
	                OR json_last_processed_date < changed_on
                )
""");

            foreach (DataRow row in dataTable.Rows)
            {
                var item = await wiserItemsService.GetItemDetailsAsync(row.Field<ulong>("id"));
                if (item == null)
                {
                    continue;
                }

                items.Add(item);
            }

            if (!items.Any())
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

                if (documentStoreReadItem.PublishedEnvironmentToSet != null)
                {
                    item.model.PublishedEnvironment = (Environments)documentStoreReadItem.PublishedEnvironmentToSet;
                }
                
                await wiserItemsService.SaveAsync(item.model, username: documentStoreReadItem.UsernameForLogging, storeTypeOverride: StoreType.Table, userId: documentStoreReadItem.UserId);
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
            };
        }

        return new JObject()
        {
            { "Success", true },
            { "ItemsRead", items.Count },
        };
    }
}