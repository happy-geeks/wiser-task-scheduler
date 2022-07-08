using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.WiserImports.Interfaces;
using AutoImportServiceCore.Modules.WiserImports.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.WiserImports.Services;

public class WiserImportsService : IWiserImportsService, IActionsService, IScopedService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<WiserImportsService> logger;
    
    private string connectionString;

    public WiserImportsService(IServiceProvider serviceProvider, ILogService logService, ILogger<WiserImportsService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;
    }
    
    /// <inheritdoc />
    public Task Initialize(ConfigurationModel configuration)
    {
        connectionString = configuration.ConnectionString;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var wiserImport = (WiserImportModel) action;
        var connectionStringToUse = wiserImport.ConnectionString ?? connectionString;
        var databaseName = GetDatabaseNameForLogging(connectionStringToUse);

        await logService.LogInformation(logger, LogScopes.RunStartAndStop, wiserImport.LogSettings, $"Starting the import for '{databaseName}'.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
        
        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        await databaseConnection.ChangeConnectionStringsAsync(connectionStringToUse, connectionStringToUse);
        
        var importDataTable = await GetImportsToProcess(databaseConnection);
        if (importDataTable.Rows.Count == 0)
        {
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, wiserImport.LogSettings, $"Finished the import for '{databaseName}' due to no imports to process.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
            
            return new JObject()
            {
                {"Database", databaseName},
                {"Processed", 0},
                {"Failed", 0},
                {"Total", 0}
            };
        }
        
        var stopwatch = new Stopwatch();
        var fileExtensionContentTypeProvider = new FileExtensionContentTypeProvider();
        
        // Wiser Items Service requires dependency injection that results in the need of MVC services that are unavailable.
        // Get all other services and create the Wiser Items Service with one of the services missing.
        var objectService = scope.ServiceProvider.GetRequiredService<IObjectsService>();
        var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        var wiserItemsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<WiserItemsService>>();
        
        var wiserItemsService = new WiserItemsService(databaseConnection, objectService, stringReplacementsService, null, databaseHelpersService, gclSettings, wiserItemsServiceLogger);

        foreach (DataRow row in importDataTable.Rows)
        {
            var importRow = new ImportRowModel(row);
            var usernameForLogs = $"{importRow.Username} (Import)";

            if (String.IsNullOrWhiteSpace(importRow.RawData))
            {
                await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Import for '{databaseName}' with ID '{importRow.Id}' failed because there is no data to import.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
                continue;
            }

            stopwatch.Restart();
            databaseConnection.AddParameter("id", importRow.Id);
            databaseConnection.AddParameter("startDate", DateTime.Now);
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserImport} SET started_on = ?startDate WHERE id = ?id");

            var errors = new List<string>();
            var destinationLinksToKeep = new Dictionary<int, Dictionary<ulong, List<ulong>>>();
            var itemParentIdsToKeep = new Dictionary<int, Dictionary<ulong, List<ulong>>>();
            var sourceLinksToKeep = new Dictionary<int, Dictionary<ulong, List<ulong>>>();

            var importData = JsonConvert.DeserializeObject<List<ImportDataModel>>(importRow.RawData, new JsonSerializerSettings() {NullValueHandling = NullValueHandling.Ignore});
            foreach (var import in importData)
            {
                try
                {
                    import.Item = await wiserItemsService.SaveAsync(import.Item, import.Item.ParentItemId, username: usernameForLogs, userId: importRow.UserId);
                }
                catch (Exception e)
                {
                    //TODO log error
                    // No need to import links and files if the item failed to be imported.
                    continue;
                }

                foreach (var link in import.Links)
                {
                    try
                    {
                        if (link.ItemId == 0 && link.DestinationItemId == 0)
                        {
                            //TODO log that no items are being linked.
                            continue;
                        }

                        // Set the missing link to the new item.
                        if (link.ItemId == 0)
                        {
                            link.ItemId = import.Item.Id;
                        }
                        else
                        {
                            link.DestinationItemId = import.Item.Id;
                        }

                        // Sorting starts with 1, so set value to 1 if not set.
                        if (link.Ordering == 0)
                        {
                            link.Ordering = 1;
                        }

                        if (!link.UseParentItemId)
                        {
                            databaseConnection.AddParameter("itemId", link.ItemId);
                            databaseConnection.AddParameter("destinationItemId", link.DestinationItemId);
                            databaseConnection.AddParameter("type", link.Type);
                            var linkPrefix = await wiserItemsService.GetTablePrefixForLinkAsync(link.Type);
                            var existingLinkDataTable = await databaseConnection.GetAsync($"SELECT id FROM {linkPrefix}{WiserTableNames.WiserItemLink} WHERE item_id = ?itemId AND destination_item_id = ?destinationItemId AND type = ?type");

                            if (existingLinkDataTable.Rows.Count > 0)
                            {
                                link.Id = existingLinkDataTable.Rows[0].Field<ulong>("id");
                            }
                            else
                            {
                                link.Id = await wiserItemsService.AddItemLinkAsync(link.ItemId, link.DestinationItemId, link.Type, link.Ordering, usernameForLogs, importRow.UserId);
                            }
                        }

                        //TODO delete existing links if set.

                        if (!link.UseParentItemId && link.Details != null && link.Details.Any())
                        {
                            foreach (var linkDetail in link.Details)
                            {
                                linkDetail.IsLinkProperty = true;
                                linkDetail.ItemLinkId = link.Id;
                                linkDetail.Changed = true;
                            }

                            import.Item.Details.AddRange(link.Details);
                            await wiserItemsService.SaveAsync(import.Item, username: usernameForLogs, userId: importRow.UserId);
                        }
                    }
                    catch (Exception e)
                    {
                        //TODO log error
                        errors.Add(e.Message);
                    }
                }

                var basePath = $@"C:\temp\AIS Import\{importRow.CustomerId}\{importRow.Id}\";
                foreach (var file in import.Files)
                {
                    try
                    {
                        var fileLocation = Path.Combine(basePath, file.FileName);
                        if (!File.Exists(fileLocation))
                        {
                            var errorMessage = $"Could not import file '{file.FileName}' for item '{import.Item.Id}' because it was not found on the hard-disk of the server.";
                            errors.Add(errorMessage);
                            //TODO log error
                            
                            continue;
                        }

                        file.ItemId = import.Item.Id;
                        file.Content = await File.ReadAllBytesAsync(fileLocation);
                        if (fileExtensionContentTypeProvider.TryGetContentType(file.FileName, out var contentType))
                        {
                            file.ContentType = contentType;
                        }

                        file.Id = await wiserItemsService.AddItemFileAsync(file, usernameForLogs, importRow.UserId);
                    }
                    catch (Exception e)
                    {
                        //TODO log error
                        errors.Add(e.Message);
                    }
                }
            }

            if (!errors.Any())
            {
                foreach (var destinationLink in destinationLinksToKeep)
                {
                    var linkType = destinationLink.Key;
                    foreach (var destination in destinationLink.Value)
                    {
                        var destinatonItemId = destination.Key;
                        var linkPrefix = await wiserItemsService.GetTablePrefixForLinkAsync(linkType);

                        try
                        {
                            databaseConnection.AddParameter("type", linkType);
                            databaseConnection.AddParameter("destinationItemId", destinatonItemId);
                            await databaseConnection.ExecuteAsync($"DELETE FROM {linkPrefix}{WiserTableNames.WiserItemLink} WHERE type = ?type AND destination_item_id = ?destinationItemId AND item_id NOT IN ({String.Join(",", destination.Value)})");
                        }
                        catch (Exception e)
                        {
                            //TODO log error
                            errors.Add(e.Message);
                        }
                    }
                }

                foreach (var parentItem in itemParentIdsToKeep)
                {
                    var linkType = parentItem.Key;
                    foreach (var parent in parentItem.Value)
                    {
                        var parentItemId = parent.Key;
                        try
                        {
                            databaseConnection.AddParameter("parentItemId", parentItemId);
                            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserItem} SET parent_item_id = 0 WHERE parent_item_id = ?parentItemId AND id NOT IN ({String.Join(",", parent.Value)})");
                        }
                        catch (Exception e)
                        {
                            //TODO log error
                            errors.Add(e.Message);
                        }
                    }
                }

                foreach (var sourceLink in sourceLinksToKeep)
                {
                    var linkType = sourceLink.Key;
                    foreach (var source in sourceLink.Value)
                    {
                        var sourceItemId = source.Key;
                        var linkPrefix = await wiserItemsService.GetTablePrefixForLinkAsync(linkType);
                        
                        try
                        {
                            databaseConnection.AddParameter("type", linkType);
                            databaseConnection.AddParameter("sourceItemId", sourceItemId);
                            await databaseConnection.ExecuteAsync($"DELETE FROM {linkPrefix}{WiserTableNames.WiserItemLink} WHERE type = ?type AND item_id = ?sourceItemId AND destination_item_id NOT IN ({String.Join(",", source.Value)})");
                        }
                        catch (Exception e)
                        {
                            //TODO log error
                            errors.Add(e.Message);
                        }
                    }
                }
            }
            
            stopwatch.Stop();
            
            databaseConnection.AddParameter("id", importRow.Id);
            databaseConnection.AddParameter("finishedOn", DateTime.Now);
            databaseConnection.AddParameter("success", !errors.Any());
            databaseConnection.AddParameter("errors", String.Join(Environment.NewLine, errors));
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserImport} SET finished_on = ?finishedOn, success = ?success, errors = ?errors WHERE id = ?id");
            
            databaseConnection.AddParameter("userId", importRow.UserId);
            var userDataTable = await databaseConnection.GetAsync($"SELECT `value` FROM {WiserTableNames.WiserItemDetail} WHERE item_id = ?userId AND `key` = 'email_address'");
            if (userDataTable.Rows.Count == 0)
            {
                //TODO log error
                continue;
            }
            
            //TODO send email to user.
            
            //TODO send task alert to user.
        }
        
        throw new System.NotImplementedException();
    }

    /// <summary>
    /// Get the name of the database that is used for the imports to include in the logging.
    /// </summary>
    /// <param name="connectionStringToUse">The connection string to get the database name from.</param>
    /// <returns>Returns the name of the database or 'Unknown' if it failed to retrieve a value.</returns>
    private string GetDatabaseNameForLogging(string connectionStringToUse)
    {
        var connectionStringParts = connectionStringToUse.Split(';');
        foreach (var part in connectionStringParts)
        {
            if (part.StartsWith("database"))
            {
                return part.Split('=')[1];
            }
        }

        return "Unknown";
    }

    /// <summary>
    /// Get all imports that need to be processed that have been created by the current machine/server.
    /// Only imports from the same machine/server are processed to ensure any files uploaded to be included are present.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <returns>Returns a <see cref="DataTable"/> containing the rows of the imports that need to be processed.</returns>
    private async Task<DataTable> GetImportsToProcess(IDatabaseConnection databaseConnection)
    {
        databaseConnection.AddParameter("serverName", Environment.MachineName);
        // Ensure import times are based on server time and not database time to prevent difference in time zones to have imports be processed to early/late.
        databaseConnection.AddParameter("now", DateTime.Now);
        return await databaseConnection.GetAsync($@"
SELECT 
    id,
    name,
    user_id,
    customer_id,
    added_by,
    data,
    sub_domain
FROM {WiserTableNames.WiserImport}
WHERE server_name = ?serverName
AND started_on IS NULL
AND start_on <= ?now
ORDER BY added_on ASC");
    }
}