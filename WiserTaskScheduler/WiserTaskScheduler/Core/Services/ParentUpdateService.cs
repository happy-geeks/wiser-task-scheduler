﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Models.ParentsUpdate;

namespace WiserTaskScheduler.Core.Services;

/// <summary>
/// A service to perform updates to parent items that are listed in the wiser_parent_updates table
/// </summary>
public class ParentUpdateService(IOptions<WtsSettings> wtsSettings, IServiceProvider serviceProvider, ILogService logService, ILogger<ParentUpdateService> logger) : IParentUpdateService, ISingletonService
{
    private readonly string logName = $"ParentUpdateService ({Environment.MachineName} - {wtsSettings.Value.Name})";

    private readonly ParentsUpdateServiceSettings parentsUpdateServiceSettings = wtsSettings.Value.ParentsUpdateService;

    private bool updatedParentUpdatesTable;
    private bool updatedTargetDatabaseList;

    // Database strings used to target other dbs in the same cluster.
    private readonly List<ParentUpdateDatabaseStrings> targetDatabases = [];

    private int runCounter;

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    /// <inheritdoc />
    public async Task ParentsUpdateAsync()
    {
        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();

        // Update parent table if it has not already been done since launch. The table definitions can only change when the WTS restarts with a new update.
        if (!updatedParentUpdatesTable)
        {
            await databaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WiserParentUpdates]);
            updatedParentUpdatesTable = true;
        }

        if (!updatedTargetDatabaseList)
        {
            UpdateTargetedDatabases(databaseConnection);
            updatedTargetDatabaseList = true;
        }

        foreach (var database in targetDatabases)
        {
            await ParentsUpdateMainAsync(databaseConnection, databaseHelpersService, database);
        }

        runCounter++;
        if (runCounter > parentsUpdateServiceSettings.PerformOptimizeEveryXtimes && parentsUpdateServiceSettings.PerformOptimizeEveryXtimes > 0)
        {
            foreach (var database in targetDatabases)
            {
                await ParentsUpdateOptimizeTables(databaseConnection, databaseHelpersService, database);
            }

            runCounter = 0;
        }
    }

    /// <summary>
    /// The main parent update routine, checks if there are updates to be performed and performs them, then truncates WiserTableNames.WiserParentUpdates table.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    /// <param name="targetDatabase">The database we are applying the parent updates on.</param>
    private async Task ParentsUpdateMainAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, ParentUpdateDatabaseStrings targetDatabase)
    {
        await databaseConnection.EnsureOpenConnectionForWritingAsync();

        if (databaseConnection.ConnectedDatabaseForWriting == null)
        {
            // No need to log this, If we dont have a database connection just skip it for now.
            // We run the updates once we database connection is back and adding logs here would just flood the logs.
            return;
        }

        if (await databaseHelpersService.DatabaseExistsAsync(targetDatabase.DatabaseName))
        {
            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserParentUpdates, targetDatabase.DatabaseName))
            {
                var dataTable = await databaseConnection.GetAsync(targetDatabase.ListTableQuery);
                var exceptionOccurred = false;

                foreach (DataRow dataRow in dataTable.Rows)
                {
                    var tableName = dataRow.Field<string>("target_table");

                    var query = $"SET @saveHistory := false; UPDATE {targetDatabase.DatabaseName}.{tableName} item INNER JOIN {targetDatabase.DatabaseName}.{WiserTableNames.WiserParentUpdates} `updates` ON `item`.id = `updates`.target_id AND `updates`.target_table = '{tableName}' SET `item`.changed_on = `updates`.changed_on, `item`.changed_by = `updates`.changed_by;";

                    try
                    {
                        await databaseConnection.ExecuteAsync(query);
                    }
                    catch (Exception e)
                    {
                        exceptionOccurred = true;
                        await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Failed to run query ( {query} ) in parent update service due to exception:{Environment.NewLine}{Environment.NewLine}{e}", logName);
                    }
                }

                try
                {
                    if (!exceptionOccurred)
                    {
                        await databaseConnection.ExecuteAsync(targetDatabase.CleanUpQuery);
                    }
                }
                catch (Exception exception)
                {
                    await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Failed to run query ( {targetDatabase.CleanUpQuery} ) in parent update service due to exception:{Environment.NewLine}{Environment.NewLine}{exception}", logName);
                }
            }
        }
    }

    /// <summary>
    /// The optimize routine, performs the optimize query on the targeted databse
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    /// <param name="targetDatabase">The database we are applying the parent updates on.</param>
    private async Task ParentsUpdateOptimizeTables(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, ParentUpdateDatabaseStrings targetDatabase)
    {
        await databaseConnection.EnsureOpenConnectionForWritingAsync();

        if (databaseConnection.ConnectedDatabaseForWriting == null)
        {
            // No need to log this, If we dont have a database connection just skip it for now.
            // We run the updates once we database connection is back and adding logs here would just flood the logs.
            return;
        }

        if (await databaseHelpersService.DatabaseExistsAsync(targetDatabase.DatabaseName))
        {
            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserParentUpdates, targetDatabase.DatabaseName))
            {
                try
                {
                    await databaseConnection.ExecuteAsync(targetDatabase.OptimizeQuery);
                }
                catch (Exception exception)
                {
                    await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Failed to run query ( {targetDatabase.OptimizeQuery} ) in parent update service due to exception:{Environment.NewLine}{Environment.NewLine}{exception}", logName);
                }
            }
        }
    }

    /// <summary>
    /// Helper function to rebuild the targeted database list
    /// </summary>
    private void UpdateTargetedDatabases(IDatabaseConnection databaseConnection)
    {
        targetDatabases.Clear();

        // Add main database.
        var listTablesQuery = $"SELECT DISTINCT `target_table` FROM `{WiserTableNames.WiserParentUpdates}`;";
        var parentsCleanUpQuery = $"DELETE FROM `{WiserTableNames.WiserParentUpdates}` WHERE `id` IS NOT NULL;";
        var resetIncrementQuery = $"ALTER TABLE `{WiserTableNames.WiserParentUpdates}` AUTO_INCREMENT = 1";
        var optimizeQuery = $"OPTIMIZE TABLE `{WiserTableNames.WiserParentUpdates}`;";

        var combinedCleanUpQuery = $"{parentsCleanUpQuery} {resetIncrementQuery}";

        targetDatabases.Add(new ParentUpdateDatabaseStrings(databaseConnection.ConnectedDatabase, listTablesQuery, combinedCleanUpQuery, optimizeQuery));

        if (parentsUpdateServiceSettings.AdditionalDatabases == null)
        {
            return;
        }

        // Add additional databases.
        foreach (var additionalDatabase in parentsUpdateServiceSettings.AdditionalDatabases)
        {
            listTablesQuery = $"SELECT DISTINCT `target_table` FROM `{additionalDatabase}`.`{WiserTableNames.WiserParentUpdates}`;";
            parentsCleanUpQuery = $"DELETE FROM `{additionalDatabase}`.`{WiserTableNames.WiserParentUpdates}` WHERE `id` IS NOT NULL;";
            optimizeQuery = $"OPTIMIZE TABLE `{additionalDatabase}`.`{WiserTableNames.WiserParentUpdates}`;";

            targetDatabases.Add(new ParentUpdateDatabaseStrings(additionalDatabase, listTablesQuery, parentsCleanUpQuery, optimizeQuery));
        }
    }
}