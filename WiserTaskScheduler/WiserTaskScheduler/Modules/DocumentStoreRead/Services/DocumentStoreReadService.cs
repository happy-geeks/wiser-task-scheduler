using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
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

public class DocumentStoreReadService(IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupItemsService> logger) : IDocumentStoreReadService, IScopedService, IActionsService
{
    private string connectionString;

    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        connectionString = configuration.ConnectionString;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var documentStoreReadItem = (DocumentStoreReadModel) action;

        var processedItems = 0;
        var successfulItems = 0;
        var failedItems = 0;

        if (String.IsNullOrWhiteSpace(documentStoreReadItem.EntityName))
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, documentStoreReadItem.LogSettings, "Can't process items because no entity name has been provided.", configurationServiceName, documentStoreReadItem.TimeId, documentStoreReadItem.Order);
            return new JObject
            {
                {"ItemsProcessed", processedItems},
                {"ItemsSuccessful", successfulItems},
                {"ItemsFailed", failedItems}
            };
        }

        using var scope = serviceProvider.CreateScope();
        var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();

        var entitiesToProcess = documentStoreReadItem.EntityName.Split(',');
        var tableGroups = new Dictionary<string, List<string>>();
        var settingsPerEntity = new Dictionary<string, EntitySettingsModel>();

        // Get the table prefix for each entity and group them by prefix. Additionally, get the settings for each entity.
        foreach (var entity in entitiesToProcess)
        {
            var prefix = await wiserItemsService.GetTablePrefixForEntityAsync(entity);
            var entityTypeSettings = await wiserItemsService.GetEntityTypeSettingsAsync(entity);
            settingsPerEntity.Add(entity, entityTypeSettings);

            if (!tableGroups.ContainsKey(prefix))
            {
                tableGroups.Add(prefix, new List<string>());
            }

            tableGroups[prefix].Add(entity);
        }

        // Process each group of entities.
        foreach (var tableGroup in tableGroups)
        {
            var entityWherePart = new StringBuilder();

            // If there is only one entity in the group, use an equals comparison. Otherwise, use an IN comparison.
            if (tableGroup.Value.Count == 1)
            {
                databaseConnection.AddParameter("entityType", tableGroup.Value[0]);
                entityWherePart.Append("= ?entityType");
            }
            else
            {
                entityWherePart.Append("IN (");

                // Dynamically add parameters for each entity in the group.
                for (var i = 0; i < tableGroup.Value.Count; i++)
                {
                    databaseConnection.AddParameter($"entityType{i}", tableGroup.Value[i]);
                    entityWherePart.Append($"{(i == 0 ? "" : ", ")}?entityType{i}");
                }

                entityWherePart.Append(')');
            }

            var dataTable = await databaseConnection.GetAsync($"""
                                                                           SELECT {(documentStoreReadItem.NoCache ? "SQL_NO_CACHE" : "")} id
                                                                           FROM {tableGroup.Key}{WiserTableNames.WiserItem}
                                                                           WHERE entity_type {entityWherePart}
                                                                           AND json IS NOT NULL
                                                                           AND (
                                                               	            json_last_processed_date IS NULL
                                                               	            OR json_last_processed_date < changed_on
                                                                           )
                                                               """);

            // Process each item in the group.
            foreach (DataRow row in dataTable.Rows)
            {
                try
                {
                    processedItems++;

                    var itemId = row.Field<ulong>("id");
                    var item = await wiserItemsService.GetItemDetailsAsync(itemId, entityType: documentStoreReadItem.EntityName, skipPermissionsCheck: true);

                    if (item == null)
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, documentStoreReadItem.LogSettings, $"Failed to process wiser item with ID '{itemId}' because it could not be found.", configurationServiceName, documentStoreReadItem.TimeId, documentStoreReadItem.Order);
                        failedItems++;
                        continue;
                    }

                    if (documentStoreReadItem.PublishedEnvironmentToSet != null)
                    {
                        item.PublishedEnvironment = (Environments) documentStoreReadItem.PublishedEnvironmentToSet;
                    }

                    if (settingsPerEntity[tableGroup.Key].StoreType == StoreType.Hybrid)
                    {
                        // Set the json to null to mark the item as processed. Hybrid mode will then no longer load and update by JSON.
                        item.Json = null;
                    }

                    item.JsonLastProcessedDate = DateTime.Now;
                    await wiserItemsService.SaveAsync(item, username: "WTS", storeTypeOverride: StoreType.Table, skipPermissionsCheck: true);
                    successfulItems++;
                }
                catch (Exception e)
                {
                    failedItems++;
                    await logService.LogWarning(logger, LogScopes.RunBody, documentStoreReadItem.LogSettings, $"Failed to process wiser item due to exception:\n{e}", configurationServiceName, documentStoreReadItem.TimeId, documentStoreReadItem.Order);
                }
            }
        }

        return new JObject
        {
            {"ItemsProcessed", processedItems},
            {"ItemsSuccessful", successfulItems},
            {"ItemsFailed", failedItems}
        };
    }
}