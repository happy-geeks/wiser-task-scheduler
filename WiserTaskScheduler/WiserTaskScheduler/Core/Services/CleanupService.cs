using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Models.Cleanup;

namespace WiserTaskScheduler.Core.Services;

/// <summary>
/// A service to manage all WTS configurations that are provided by Wiser.
/// </summary>
public class CleanupService(
    IOptions<WtsSettings> wtsSettings,
    IServiceProvider serviceProvider,
    ILogService logService,
    ILogger<CleanupService> logger
    ) : ICleanupService, ISingletonService
{
    private readonly string logName = $"CleanupService ({Environment.MachineName} - {wtsSettings.Value.Name})";

    private readonly CleanupServiceSettings cleanupServiceSettings = wtsSettings.Value.CleanupService;

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    /// <inheritdoc />
    public async Task CleanupAsync()
    {
        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
        var linkTypesService = scope.ServiceProvider.GetRequiredService<ILinkTypesService>();

        await CleanupFilesAsync();
        await CleanupDatabaseLogsAsync(databaseConnection, databaseHelpersService);
        await CleanupDatabaseRenderTimesAsync(databaseConnection, databaseHelpersService);
        await CleanupWtsServicesAsync(databaseConnection, databaseHelpersService);
		await CleanupTemporaryWiserFilesAsync(databaseConnection, databaseHelpersService);
        await CleanupFloatingLinksAsync(databaseConnection, databaseHelpersService, wiserItemsService , linkTypesService);
    }

    /// <summary>
    /// Cleanup files older than the set number of days in the given folders.
    /// </summary>
    private async Task CleanupFilesAsync()
    {
        if (cleanupServiceSettings.FileFolderPaths == null || !cleanupServiceSettings.FileFolderPaths.Any())
        {
            return;
        }

        foreach (var folderPath in cleanupServiceSettings.FileFolderPaths)
        {
            try
            {
                var files = Directory.GetFiles(folderPath);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Found {files.Length} files in '{folderPath}' to perform cleanup on.", logName);
                var filesDeleted = 0;

                foreach (var file in files)
                {
                    try
                    {
                        if (File.GetLastWriteTime(file) >= DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStore))
                        {
                            continue;
                        }

                        File.Delete(file);
                        filesDeleted++;
                        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Deleted file: {file}", logName);
                    }
                    catch (Exception exception)
                    {
                        await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Could not delete file: {file} due to exception {exception}", logName);
                    }
                }

                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {filesDeleted} files in '{folderPath}'.", logName);
            }
            catch (Exception exception)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Could not delete files in folder: {folderPath} due to exception {exception}", logName);
            }
        }
    }

    /// <summary>
    /// Cleanup logs in the database older than the set number of days in the WTS logs.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    private async Task CleanupDatabaseLogsAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
    {
        try
        {
            databaseConnection.SetCommandTimeout(cleanupServiceSettings.Timeout);
            databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStore));
            var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WtsLogs} WHERE added_on < ?cleanupDate", cleanUp: true);
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WtsLogs}'.", logName);

            if (cleanupServiceSettings.OptimizeLogsTableAfterCleanup)
            {
                await databaseHelpersService.OptimizeTablesAsync(WiserTableNames.WtsLogs);
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup: {exception}", logName);
        }
    }

    /// <summary>
    /// Cleanup files in the wiser_itemfile table with property_name 'TEMPORARY_FILE_FROM_WISER' older than 24h.
    /// Note: this does not yet support dedicated tables.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    private async Task CleanupTemporaryWiserFilesAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
    {
        try
        {
            // Check if the table exists before trying to delete from it.
            if (!await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserItemFile))
            {
                return;
            }

            // Check if the property name is set and not just whitespace(or is empty).
            if (String.IsNullOrWhiteSpace(cleanupServiceSettings.ProperyNameTemporaryWiserFiles))
            {
                return;
            }

            // Check if the number of days to store is greater than 0.
            if (cleanupServiceSettings.NumberOfDaysToStoreTemporaryWiserFiles <= 0)
            {
                return;
            }

            databaseConnection.AddParameter("propertyName", cleanupServiceSettings.ProperyNameTemporaryWiserFiles);
            databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStoreTemporaryWiserFiles));
            var itemsToDelete = await databaseConnection.GetAsync($"SELECT id FROM {WiserTableNames.WiserItemFile} WHERE property_name = ?propertyName AND added_on < ?cleanupDate");

            // if there is nothing to delete, don't bother running the delete query.
            if (itemsToDelete.Rows.Count == 0)
            {
                return;
            }

            var idsToDelete = itemsToDelete.Rows.Cast<DataRow>().Select(x => Convert.ToUInt64(x["id"])).ToList();

            var deleteQuery = $"DELETE FROM {WiserTableNames.WiserItemFile} WHERE id IN ({String.Join(", ", idsToDelete)})";

            var rowsDeleted = await databaseConnection.ExecuteAsync(deleteQuery, cleanUp: true);
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WiserItemFile}'.", logName);

            if (cleanupServiceSettings.OptimizeLogsTableAfterCleanup)
            {
                await databaseHelpersService.OptimizeTablesAsync(WiserTableNames.WiserItemFile);
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during temp file cleanup: {exception}", logName);
        }
    }

    /// <summary>
    /// Cleanup render times in the database older than the set number of days.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    private async Task CleanupDatabaseRenderTimesAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
    {
        databaseConnection.SetCommandTimeout(cleanupServiceSettings.Timeout);
        databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStoreRenderTimes));
        var optimizeRenderLogTables = new List<string>();

        try
        {
            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserTemplateRenderLog))
            {
                var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WiserTemplateRenderLog} WHERE end < ?cleanupDate", cleanUp: true);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WiserTemplateRenderLog}'.", logName);
                optimizeRenderLogTables.Add(WiserTableNames.WiserTemplateRenderLog);
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup: {exception}", logName);
        }

        try
        {
            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserDynamicContentRenderLog))
            {
                var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WiserDynamicContentRenderLog} WHERE end < ?cleanupDate", cleanUp: true);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WiserDynamicContentRenderLog}'.", logName);
                optimizeRenderLogTables.Add(WiserTableNames.WiserDynamicContentRenderLog);
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup: {exception}", logName);
        }

        try
        {
            if (cleanupServiceSettings.OptimizeRenderTimesTableAfterCleanup && optimizeRenderLogTables.Any())
            {
                await databaseHelpersService.OptimizeTablesAsync(optimizeRenderLogTables.ToArray());
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup: {exception}", logName);
        }
    }

    /// <summary>
    /// Cleanup wts services in the database older than the set number of days in the WTS services.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    private async Task CleanupWtsServicesAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
    {
        try
        {
            databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStoreWtsServices));

            // Use the next_run column to determine if a service is older than the cleanup date to prevent paused and longer run schemes from being deleted.
            var query = $"DELETE FROM {WiserTableNames.WtsServices} WHERE next_run < ?cleanupDate";
            var rowsDeleted = await databaseConnection.ExecuteAsync(query, cleanUp: true);

            await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WtsServices}'.", logName);

            if (cleanupServiceSettings.OptimizeLogsTableAfterCleanup)
            {
                await databaseHelpersService.OptimizeTablesAsync(WiserTableNames.WtsServices);
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup: {exception}", logName);
        }
    }

    /// <summary>
    /// Cleanup floating dead links in the database
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
    private async Task CleanupFloatingLinksAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, IWiserItemsService wiserItemsService, ILinkTypesService linkTypesService)
    {
        var AllLinkTypes = await linkTypesService.GetAllLinkTypeSettingsAsync();

        foreach (var linkType in AllLinkTypes)
        {
            if (!linkType.UseItemParentId)
            {
                await CleanupFloatingLinksAsync(databaseConnection, databaseHelpersService, wiserItemsService, linkTypesService, linkType);
            }
        }
    }

    private async Task CleanupFloatingLinksAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, IWiserItemsService wiserItemsService, ILinkTypesService linkTypesService, LinkSettingsModel linkSettings)
    {

        var itemLinkTablePrefix= linkTypesService.GetTablePrefixForLink(linkSettings);

        var sourceItemTablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(linkSettings.SourceEntityType);
        var destinationItemTablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(linkSettings.DestinationEntityType);

        var retrieveIdsQuery = $"""
                                    SELECT link.id
                                    FROM {itemLinkTablePrefix}{WiserTableNames.WiserItemLink} AS link
                                    LEFT JOIN {sourceItemTablePrefix}{WiserTableNames.WiserItem} AS sourceItem ON sourceItem.id = link.item_id AND sourceItem.entity_type = ?sourceItemEntityType
                                    LEFT JOIN {destinationItemTablePrefix}{WiserTableNames.WiserItem} AS destinationItem ON destinationItem.id = link.destination_item_id AND destinationItem.entity_type = ?destinationItemEntityType
                                    WHERE link.type = ?linkType
                                    AND link.destination_item_id != 0
                                    AND (sourceItem.id IS NULL OR destinationItem.id IS NULL)
                                    AND link.added_on < (CURDATE() - INTERVAL 1 MONTH)
                                 """;
        try
        {
            databaseConnection.ClearParameters();
            databaseConnection.AddParameter("linkType", linkSettings.Type);
            databaseConnection.AddParameter("sourceItemEntityType", linkSettings.SourceEntityType);
            databaseConnection.AddParameter("destinationItemEntityType", linkSettings.DestinationEntityType);

            var retrievedIds = await databaseConnection.GetAsync(retrieveIdsQuery);

            if (retrievedIds.Rows.Count != 0)
            {
                var deleteFloatingLinksQuery = $"""
                    DELETE FROM {itemLinkTablePrefix}{WiserTableNames.WiserItemLink}
                    WHERE id IN ({String.Join(", ", retrievedIds.Rows.Cast<DataRow>().Select(x => x.Field<long>("id")))})
                """;

                /*
                TODO: uncomment this after verifying the correctness of the cleanup query.
                var deleteResults = await databaseConnection.ExecuteAsync(retrieveIdsQuery);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Deleted {deleteResults} floating links for link type '{linkSettings.Type}' in '{itemLinkTablePrefix}{WiserTableNames.WiserItemLink}'.", logName);
                */
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup of {itemLinkTablePrefix}{WiserTableNames.WiserItemLink}: {exception}", logName);
        }
    }
}