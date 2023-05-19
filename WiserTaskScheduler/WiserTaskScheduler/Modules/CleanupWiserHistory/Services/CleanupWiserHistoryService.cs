﻿using System;
using System.Collections.Generic;
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

public class CleanupWiserHistoryService : ICleanupWiserHistoryService, IActionsService, IScopedService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<CleanupWiserHistoryService> logger;
    
    private string connectionString;
    private HashSet<string> tablesToOptimize;

    public CleanupWiserHistoryService(IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupWiserHistoryService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;
    }
    
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

        if (String.IsNullOrWhiteSpace(cleanupWiserHistory.EntityName))
        {
            await logService.LogWarning(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"No entity provided to clean the history from. Please provide a name of an entity.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);
            
            return new JObject()
            {
                {"Success", false},
                {"EntityName", cleanupWiserHistory.EntityName},
                {"CleanupDate", DateTime.MinValue},
                {"HistoryRowsDeleted", 0}
            };
        }
        
        if (String.IsNullOrWhiteSpace(cleanupWiserHistory.TimeToStoreString))
        {
            await logService.LogWarning(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"No time to store provided to describe how long the history needs to stay stored. Please provide a time to store.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);
            
            return new JObject()
            {
                {"Success", false},
                {"EntityName", cleanupWiserHistory.EntityName},
                {"CleanupDate", DateTime.MinValue},
                {"HistoryRowsDeleted", 0}
            };
        }
        
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"Starting cleanup for history of entity '{cleanupWiserHistory.EntityName}' that are older than '{cleanupWiserHistory.TimeToStore}'.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);
        
        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        var connectionStringToUse = connectionString;
        await databaseConnection.ChangeConnectionStringsAsync(connectionStringToUse, connectionStringToUse);
        databaseConnection.ClearParameters();
        
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
        var tablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(cleanupWiserHistory.EntityName);

        var cleanupDate = DateTime.Now.Subtract(cleanupWiserHistory.TimeToStore);
        databaseConnection.AddParameter("entityName", cleanupWiserHistory.EntityName);
        databaseConnection.AddParameter("cleanupDate", cleanupDate);
        databaseConnection.AddParameter("tableName", $"{tablePrefix}{WiserTableNames.WiserItem}");
        
        var historyRowsDeleted = await databaseConnection.ExecuteAsync($@"
DELETE history.*
FROM {tablePrefix}{WiserTableNames.WiserItem} AS item
JOIN {WiserTableNames.WiserHistory} AS history ON history.item_id = item.id AND history.tablename LIKE CONCAT(?tableName, '%') AND history.changed_on < ?cleanupDate
WHERE item.entity_type = ?entityName");
        
        historyRowsDeleted += await databaseConnection.ExecuteAsync($@"
DELETE history.*
FROM {tablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix} AS item
JOIN {WiserTableNames.WiserHistory} AS history ON history.item_id = item.id AND history.tablename LIKE CONCAT(?tableName, '%') AND history.changed_on < ?cleanupDate
WHERE item.entity_type = ?entityName");

        await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupWiserHistory.LogSettings, $"'{historyRowsDeleted}' {(historyRowsDeleted == 1 ? "row has" : "rows have")} been deleted from the history of items of entity '{cleanupWiserHistory.EntityName}'.", configurationServiceName, cleanupWiserHistory.TimeId, cleanupWiserHistory.Order);

        if (cleanupWiserHistory.OptimizeTablesAfterCleanup && historyRowsDeleted > 0)
        {
            tablesToOptimize.Add(WiserTableNames.WiserHistory);
        }
        
        return new JObject()
        {
            {"Success", true},
            {"EntityName", cleanupWiserHistory.EntityName},
            {"CleanupDate", cleanupDate},
            {"HistoryRowsDeleted", historyRowsDeleted}
        };
    }
}