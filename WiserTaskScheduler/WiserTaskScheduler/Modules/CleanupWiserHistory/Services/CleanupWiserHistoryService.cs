using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.CleanupWiserHistory.Interfaces;
using WiserTaskScheduler.Modules.CleanupWiserHistory.Models;

namespace WiserTaskScheduler.Modules.CleanupWiserHistory.Services;

public class CleanupWiserHistoryService(IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupWiserHistoryService> logger) : ICleanupWiserHistoryService, IActionsService, IScopedService
{
    private string connectionString;
    private HashSet<string> tablesToOptimize;

    /// <inheritdoc />
    // ReSharper disable once ParameterHidesMember
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        connectionString = configuration.ConnectionString;
        this.tablesToOptimize = tablesToOptimize;

        if (String.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException($"Configuration '{configuration.ServiceName}' has no connection string defined but contains active `CleanupWiserHistory` actions. Please provide a connection string.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var cleanupWiserHistory = (CleanupWiserHistoryModel) action;

        if (String.IsNullOrWhiteSpace(cleanupWiserHistory.EntityName) && !cleanupWiserHistory.CleanupAllEntities)
        {
            await logService.LogWarning(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"No entity provided to clean the history from. Please provide a name of an entity or mark that all entities need to be cleaned in {WiserTableNames.WiserHistory}.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);

            return new JObject
            {
                {"Success", false},
                {"EntityName", cleanupWiserHistory.EntityName},
                {"CleanupDate", DateTime.MinValue},
                {"HistoryRowsDeleted", 0}
            };
        }

        if (String.IsNullOrWhiteSpace(cleanupWiserHistory.TimeToStoreString))
        {
            await logService.LogWarning(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, "No time to store provided to describe how long the history needs to stay stored. Please provide a time to store.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);

            return new JObject
            {
                {"Success", false},
                {"EntityName", cleanupWiserHistory.EntityName},
                {"CleanupDate", DateTime.MinValue},
                {"HistoryRowsDeleted", 0}
            };
        }

        await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"Starting cleanup for history entries{(String.IsNullOrWhiteSpace(cleanupWiserHistory.EntityName) ? "" : $" of entity '{cleanupWiserHistory.EntityName}'")} that are older than '{cleanupWiserHistory.TimeToStore}'.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);

        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        var connectionStringToUse = connectionString;
        await databaseConnection.ChangeConnectionStringsAsync(connectionStringToUse, connectionStringToUse);
        databaseConnection.ClearParameters();

        var cleanupDate = DateTime.Now.Subtract(cleanupWiserHistory.TimeToStore);
        databaseConnection.AddParameter("cleanupDate", cleanupDate);

        var historyEntriesToDelete = new List<ulong>();

        if (!String.IsNullOrWhiteSpace(cleanupWiserHistory.EntityName))
        {
            var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
            var tablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(cleanupWiserHistory.EntityName);

            databaseConnection.AddParameter("entityName", cleanupWiserHistory.EntityName);
            databaseConnection.AddParameter("tableName", $"{tablePrefix}{WiserTableNames.WiserItem}");

            var dataTable = await databaseConnection.GetAsync($"""
                                                               SELECT CAST(history.id AS UNSIGNED) AS id
                                                               FROM {tablePrefix}{WiserTableNames.WiserItem} AS item
                                                               JOIN {WiserTableNames.WiserHistory} AS history ON history.item_id = item.id AND history.tablename LIKE CONCAT(?tableName, '%') AND history.changed_on < ?cleanupDate
                                                               WHERE item.entity_type = ?entityName
                                                               """);

            historyEntriesToDelete.AddRange(dataTable.AsEnumerable().Select(row => row.Field<ulong>("id")));

            dataTable = await databaseConnection.GetAsync($"""
                                                           SELECT CAST(history.id AS UNSIGNED) AS id
                                                           FROM {tablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix} AS item
                                                           JOIN {WiserTableNames.WiserHistory} AS history ON history.item_id = item.id AND history.tablename LIKE CONCAT(?tableName, '%')AND history.changed_on < ?cleanupDate
                                                           WHERE item.entity_type = ?entityName
                                                           """);

            historyEntriesToDelete.AddRange(dataTable.AsEnumerable().Select(row => row.Field<ulong>("id")));
        }
        else if (cleanupWiserHistory.CleanupAllEntities)
        {
            var dataTable = await databaseConnection.GetAsync($"""
                                                               SELECT CAST(history.id AS UNSIGNED) AS id
                                                               FROM {WiserTableNames.WiserHistory} AS history
                                                               WHERE history.changed_on < ?cleanupDate
                                                               """);

            historyEntriesToDelete.AddRange(dataTable.AsEnumerable().Select(row => row.Field<ulong>("id")));
        }

        var historyRowsDeleted = 0;
        if (historyEntriesToDelete.Count > 0)
        {
            databaseConnection.ClearParameters();

            historyRowsDeleted = await databaseConnection.ExecuteAsync($"""
                                                                      DELETE FROM {WiserTableNames.WiserHistory}
                                                                      WHERE id IN ({String.Join(", ", historyEntriesToDelete)})
                                                                      """);
        }

        await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"'{historyRowsDeleted}' {(historyRowsDeleted == 1 ? "row has" : "rows have")} been deleted from the history{(String.IsNullOrWhiteSpace(cleanupWiserHistory.EntityName) ? "" : $" of items of entity '{cleanupWiserHistory.EntityName}'")}.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);

        if (cleanupWiserHistory.OptimizeTablesAfterCleanup && historyRowsDeleted > 0)
        {
            tablesToOptimize.Add(WiserTableNames.WiserHistory);
        }

        return new JObject
        {
            {"Success", true},
            {"EntityName", String.IsNullOrWhiteSpace(cleanupWiserHistory.EntityName) ? "All" : cleanupWiserHistory.EntityName},
            {"CleanupDate", cleanupDate},
            {"HistoryRowsDeleted", historyRowsDeleted}
        };
    }
}