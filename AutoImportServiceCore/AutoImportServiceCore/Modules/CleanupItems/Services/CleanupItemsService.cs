﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.CleanupItems.Interfaces;
using AutoImportServiceCore.Modules.CleanupItems.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.CleanupItems.Services;

public class CleanupItemsService : ICleanupItemsService, IActionsService, IScopedService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<CleanupItemsService> logger;

    private string connectionString;

    public CleanupItemsService(IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupItemsService> logger)
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
        var cleanupItem = (CleanupItemModel) action;

        await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupItem.LogSettings, $"Starting cleanup for items of entity '{cleanupItem.EntityName}' that are older than '{cleanupItem.TimeToStore}'.", configurationServiceName, cleanupItem.TimeId, cleanupItem.Order);

        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        var connectionStringToUse = cleanupItem.ConnectionString ?? connectionString;
        await databaseConnection.ChangeConnectionStringsAsync(connectionStringToUse, connectionStringToUse);
        databaseConnection.ClearParameters();
        
        // Wiser Items Service requires dependency injection that results in the need of MVC services that are unavailable.
        // Get all other services and create the Wiser Items Service with one of the services missing.
        var objectService = scope.ServiceProvider.GetRequiredService<IObjectsService>();
        var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        var wiserItemsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<WiserItemsService>>();
        
        var wiserItemsService = new WiserItemsService(databaseConnection, objectService, stringReplacementsService, null, databaseHelpersService, gclSettings, wiserItemsServiceLogger);
        var tablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(cleanupItem.EntityName);
        
        // Get the delete action of the entity to show it in the logs.
        databaseConnection.AddParameter("entityName", cleanupItem.EntityName);
        var entityDataTable = await databaseConnection.GetAsync($"SELECT delete_action FROM {WiserTableNames.WiserEntity} WHERE `name` = ?entityName LIMIT 1");
        var deleteAction = entityDataTable.Rows[0].Field<string>("delete_action");
        
        // Get all IDs from items that need to be cleaned.
        var cleanupDate = DateTime.Now.Subtract(cleanupItem.TimeToStore);
        databaseConnection.AddParameter("cleanupDate", cleanupDate);

        var joins = new StringBuilder();
        var wheres = new StringBuilder();

        // Add extra checks if the item is not allowed to be a connected item.
        if (cleanupItem.OnlyWhenNotConnectedItem)
        {
            joins.Append($"LEFT JOIN {WiserTableNames.WiserItemLink} AS itemLink ON itemLink.item_id = item.id {(cleanupItem.OnlyWhenNotDestinationItem ? "OR itemLink.destination_item_id = item.id" : "")}");
            
            wheres.Append(@"AND item.parent_item_id = 0
AND itemLink.id IS NULL");

            // Add checks for dedicated link tables if the item is set as connected item entity.
            var dedicatedLinkTypes = await GetDedicatedLinkTypes(databaseConnection, cleanupItem.EntityName, false);
            for (var i = 0; i < dedicatedLinkTypes.Count; i++)
            {
                joins.Append($@"
LEFT JOIN {dedicatedLinkTypes[i]}_{WiserTableNames.WiserItemLink} AS connectedItemLink{i} ON connectedItemLink{i}.item_id = item.id");

                wheres.Append($@"
AND connectedItemLink{i}.id IS NULL");
            }
        }

        // Add extra checks if the item is not allowed to be a destination item. When combined with OnlyWhenNotConnectedItem the checks need to be added to the existing ones.
        if (cleanupItem.OnlyWhenNotDestinationItem)
        {
            if (!cleanupItem.OnlyWhenNotConnectedItem)
            {
                joins.Append($"LEFT JOIN {WiserTableNames.WiserItemLink} AS itemLink ON itemLink.destination_item_id = item.id");
                wheres.Append(@"AND itemLink.id IS NULL");
            }
            
            joins.Append($@"
LEFT JOIN {tablePrefix}wiser_item AS child ON child.parent_item_id = item.id");
            
            wheres.Append(@"
AND child.id IS NULL");
            
            // Add checks for dedicated link tables if the item is set as destination entity.
            var dedicatedLinkTypes = await GetDedicatedLinkTypes(databaseConnection, cleanupItem.EntityName, true);
            for (var i = 0; i < dedicatedLinkTypes.Count; i++)
            {
                joins.Append($@"
LEFT JOIN {dedicatedLinkTypes[i]}_{WiserTableNames.WiserItemLink} AS destinationItemLink{i} ON destinationItemLink{i}.item_id = item.id");

                wheres.Append($@"
AND destinationItemLink{i}.id IS NULL");
            }
        }

        var query = $@"SELECT item.id
FROM {tablePrefix}{WiserTableNames.WiserItem} AS item
{joins}
WHERE item.entity_type = ?entityName
AND TIMEDIFF(item.{(cleanupItem.SinceLastChange ? "changed_on" : "added_on")}, ?cleanupDate) <= 0
{wheres}";
        
        var itemsDataTable = await databaseConnection.GetAsync(query);

        if (itemsDataTable.Rows.Count == 0)
        {
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupItem.LogSettings, $"Finished cleanup for items of entity '{cleanupItem.EntityName}', no items found to cleanup.", configurationServiceName, cleanupItem.TimeId, cleanupItem.Order);

            return new JObject()
            {
                {"Success", true},
                {"EntityName", cleanupItem.EntityName},
                {"CleanupDate", cleanupDate},
                {"ItemsToCleanup", 0},
                {"DeleteAction", deleteAction}
            };
        }

        var ids = (from DataRow row in itemsDataTable.Rows
                   select row.Field<ulong>("id")).ToList();

        var success = true;
        
        try
        {
            await wiserItemsService.DeleteAsync(ids, username: "AIS Cleanup", saveHistory: cleanupItem.SaveHistory, skipPermissionsCheck: true, entityType: cleanupItem.EntityName);
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, cleanupItem.LogSettings, $"Finished cleanup for items of entity '{cleanupItem.EntityName}', delete action: '{deleteAction}'.", configurationServiceName, cleanupItem.TimeId, cleanupItem.Order);
        }
        catch (Exception e)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, cleanupItem.LogSettings, $"Failed cleanup for items of entity '{cleanupItem.EntityName}' due to exception:\n{e}", configurationServiceName, cleanupItem.TimeId, cleanupItem.Order);
            success = false;
        }
        
        return new JObject()
        {
            {"Success", success},
            {"EntityName", cleanupItem.EntityName},
            {"CleanupDate", cleanupDate},
            {"ItemsToCleanup", itemsDataTable.Rows.Count},
            {"DeleteAction", deleteAction}
        };
    }

    /// <summary>
    /// Get the types of links that have a dedicated table.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="entityName">The name of the entity that the link needs to be for.</param>
    /// <param name="destinationInsteadOfConnectedItem">True to check if entity is destination, false to check if entity is connected item.</param>
    /// <returns>Returns a list with the types.</returns>
    private async Task<List<int>> GetDedicatedLinkTypes(IDatabaseConnection databaseConnection, string entityName, bool destinationInsteadOfConnectedItem)
    {
        databaseConnection.AddParameter("entityName", entityName);
        
        var query = $@"SELECT link.type
FROM {WiserTableNames.WiserLink} AS link
WHERE link.use_dedicated_table = true
AND link.{(destinationInsteadOfConnectedItem ? "destination_entity_type" : "connected_entity_type")} = 'testmark6'";

        var dataTable = await databaseConnection.GetAsync(query);

        var dedicatedLinkTypes = new List<int>();

        if (dataTable.Rows.Count == 0)
        {
            return dedicatedLinkTypes;
        }

        dedicatedLinkTypes.AddRange(from DataRow row in dataTable.Rows
                                    select row.Field<int>("type"));

        return dedicatedLinkTypes;
    }
}