﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Wiser.Interfaces;
using AutoImportServiceCore.Modules.WiserImports.Interfaces;
using AutoImportServiceCore.Modules.WiserImports.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Enums;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using GeeksCoreLibrary.Modules.Imports.Models;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using GclCommunicationsService = GeeksCoreLibrary.Modules.Communication.Services.CommunicationsService;

namespace AutoImportServiceCore.Modules.WiserImports.Services;

public class WiserImportsService : IWiserImportsService, IActionsService, IScopedService
{
    private const string DefaultSubject = "Import[if({name}!)] with the name '{name}'[endif] from {date:DateTime(dddd\\, dd MMMM yyyy,en-US)} did [if({errorCount}=0)]finish successfully[else](partially) go wrong[endif]";
    private const string DefaultContent = "<p>The import started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The import took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the import: {errors}[endif]";
    
    private readonly IServiceProvider serviceProvider;
    private readonly AisSettings aisSettings;
    private readonly IWiserService wiserService;
    private readonly ILogService logService;
    private readonly ILogger<WiserImportsService> logger;
    private readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings() {NullValueHandling = NullValueHandling.Ignore};
    private readonly FileExtensionContentTypeProvider fileExtensionContentTypeProvider = new FileExtensionContentTypeProvider();

    private string connectionString;

    public WiserImportsService(IServiceProvider serviceProvider, IOptions<AisSettings> aisSettings, IWiserService wiserService, ILogService logService, ILogger<WiserImportsService> logger)
    {
        this.serviceProvider = serviceProvider;
        this.aisSettings = aisSettings.Value;
        this.wiserService = wiserService;
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

        var importDataTable = await GetImportsToProcessAsync(databaseConnection);
        if (importDataTable.Rows.Count == 0)
        {
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, wiserImport.LogSettings, $"Finished the import for '{databaseName}' due to no imports to process.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);

            return new JObject()
            {
                {"Database", databaseName},
                {"Success", 0},
                {"WithWarnings", 0},
                {"Total", 0}
            };
        }

        var stopwatch = new Stopwatch();

        // Wiser Items Service requires dependency injection that results in the need of MVC services that are unavailable.
        // Get all other services and create the Wiser Items Service with one of the services missing.
        var objectService = scope.ServiceProvider.GetRequiredService<IObjectsService>();
        var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        var wiserItemsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<WiserItemsService>>();
        var gclCommunicationsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<GclCommunicationsService>>();

        var wiserItemsService = new WiserItemsService(databaseConnection, objectService, stringReplacementsService, null, databaseHelpersService, gclSettings, wiserItemsServiceLogger);
        var gclCommunicationsService = new GclCommunicationsService(gclSettings, gclCommunicationsServiceLogger, wiserItemsService, databaseConnection, databaseHelpersService);

        var successfulImports = 0;
        var importsWithWarnings = 0;

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
            var startDate = DateTime.Now;
            databaseConnection.AddParameter("id", importRow.Id);
            databaseConnection.AddParameter("startDate", startDate);
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserImport} SET started_on = ?startDate WHERE id = ?id");

            var errors = new List<string>();
            var destinationLinksToKeep = new Dictionary<int, Dictionary<ulong, List<ulong>>>();
            var itemParentIdsToKeep = new Dictionary<int, Dictionary<ulong, List<ulong>>>();
            var sourceLinksToKeep = new Dictionary<int, Dictionary<ulong, List<ulong>>>();

            var importData = JsonConvert.DeserializeObject<List<ImportDataModel>>(importRow.RawData, serializerSettings);
            foreach (var import in importData)
            {
                try
                {
                    import.Item = await wiserItemsService.SaveAsync(import.Item, import.Item.ParentItemId, username: usernameForLogs, userId: importRow.UserId);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Failed to import an item due to exception:\n{e}\n\nItem details:\n{JsonConvert.SerializeObject(import.Item, serializerSettings)}", configurationServiceName, wiserImport.TimeId, wiserImport.Order);

                    // No need to import links and files if the item failed to be imported.
                    continue;
                }

                await ImportLinksAsync(wiserImport, import, databaseConnection, wiserItemsService, usernameForLogs, errors, destinationLinksToKeep, itemParentIdsToKeep, sourceLinksToKeep, importRow, configurationServiceName);
                await ImportFilesAsync(wiserImport, import, databaseConnection, wiserItemsService, usernameForLogs, errors, importRow, configurationServiceName);
            }

            // If there have been no errors the import was successful. We can safely cleanup any links that need to be deleted.
            if (!errors.Any())
            {
                await CleanupLinks(wiserImport, databaseConnection, wiserItemsService, errors, destinationLinksToKeep, itemParentIdsToKeep, sourceLinksToKeep, usernameForLogs, importRow, configurationServiceName, importData.First().Item.EntityType);
                successfulImports++;
            }
            else
            {
                importsWithWarnings++;
            }

            var endDate = DateTime.Now;
            stopwatch.Stop();

            databaseConnection.AddParameter("id", importRow.Id);
            databaseConnection.AddParameter("finishedOn", DateTime.Now);
            databaseConnection.AddParameter("success", !errors.Any());
            databaseConnection.AddParameter("errors", String.Join(Environment.NewLine, errors));
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserImport} SET finished_on = ?finishedOn, success = ?success, errors = ?errors WHERE id = ?id");

            var replaceData = new Dictionary<string, object>
            {
                {"name", importRow.ImportName},
                {"date", DateTime.Now},
                {"errorCount", errors.Count},
                {"startDate", startDate},
                {"endDate", endDate},
                {"hours", stopwatch.Elapsed.Hours},
                {"minutes", stopwatch.Elapsed.Minutes},
                {"seconds", stopwatch.Elapsed.Seconds},
                {"errors", $"<ul><li><pre>{String.Join("</pre></li><li><pre>", errors)}</pre></li></ul>"}
            };

            WiserItemModel template = null;
            if (wiserImport.TemplateId > 0)
            {
                template = await wiserItemsService.GetItemDetailsAsync(wiserImport.TemplateId, userId: importRow.UserId);
            }
            
            var subject = await stringReplacementsService.DoAllReplacementsAsync(stringReplacementsService.DoReplacements(String.IsNullOrWhiteSpace(template?.GetDetailValue("subject")) ? DefaultSubject : template.GetDetailValue("subject"), replaceData));
            await NotifyUserByEmailAsync(wiserImport, importRow, databaseConnection, gclCommunicationsService, configurationServiceName, subject, template, replaceData, stringReplacementsService);
            await NotifyUserByTaskAlertAsync(wiserImport, importRow, wiserItemsService, configurationServiceName, usernameForLogs, subject);
            
            // TODO: Get all imports where "rollback_on" is not null and rollback those imports. We want to use wiser_history for rolling back.
            // TODO: Get all item IDs that have been used in the import and get the start date and end date, then get all changes from wiser_history for these items between these two dates and use that data for rolling back.
        }

        return new JObject()
        {
            {"Database", databaseName},
            {"Success", successfulImports},
            {"WithWarnings", importsWithWarnings},
            {"Total", successfulImports + importsWithWarnings}
        };
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
    private async Task<DataTable> GetImportsToProcessAsync(IDatabaseConnection databaseConnection)
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

    /// <summary>
    /// Import the links to Wiser.
    /// </summary>
    /// <param name="wiserImport">The AIS information for handling the imports.</param>
    /// <param name="import">The data to import.</param>
    /// <param name="databaseConnection">The connection to the database.</param>
    /// <param name="wiserItemsService">The WiserItemsService to use to import the data.</param>
    /// <param name="usernameForLogs">The username to use for the logs in Wiser.</param>
    /// <param name="errors">The list of errors.</param>
    /// <param name="destinationLinksToKeep">The list of destination links to keep.</param>
    /// <param name="itemParentIdsToKeep">The list of item parent ids to keep.</param>
    /// <param name="sourceLinksToKeep">The list of source links to keep.</param>
    /// <param name="importRow">The information of the import itself.</param>
    /// <param name="configurationServiceName">The name of the configuration the import is executed within.</param>
    private async Task ImportLinksAsync(WiserImportModel wiserImport, ImportDataModel import, IDatabaseConnection databaseConnection, IWiserItemsService wiserItemsService, string usernameForLogs, List<string> errors, Dictionary<int, Dictionary<ulong, List<ulong>>> destinationLinksToKeep, Dictionary<int, Dictionary<ulong, List<ulong>>> itemParentIdsToKeep, Dictionary<int, Dictionary<ulong, List<ulong>>> sourceLinksToKeep, ImportRowModel importRow, string configurationServiceName)
    {
        foreach (var link in import.Links)
        {
            try
            {
                if (link.ItemId == 0 && link.DestinationItemId == 0)
                {
                    await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Found link with neither an item_id nor a destination_item_id for item '{import.Item.Id}', but it has not data, so there is nothing we can import.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
                    continue;
                }

                // Set the missing link to the new item.
                if (link.ItemId == 0)
                {
                    link.ItemId = import.Item.Id;
                }
                else if (link.DestinationItemId == 0)
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
                        link.Id = Convert.ToUInt64(existingLinkDataTable.Rows[0]["id"]);
                    }
                    else
                    {
                        link.Id = await wiserItemsService.AddItemLinkAsync(link.ItemId, link.DestinationItemId, link.Type, link.Ordering, usernameForLogs, importRow.UserId);
                    }
                }

                // If the user wants to delete existing links, make a list with links that we need to keep, so we can delete the rest at the end of the import.
                if (link.DeleteExistingLinks)
                {
                    // If the current item is the source item, then we want to delete all other links of the given destination, so that only the imported items will remain linked to that destination.
                    if (link.ItemId == import.Item.Id)
                    {
                        var listToUse = link.UseParentItemId ? itemParentIdsToKeep : destinationLinksToKeep;

                        if (!listToUse.ContainsKey(link.Type))
                        {
                            listToUse.Add(link.Type, new Dictionary<ulong, List<ulong>>());
                        }

                        if (!listToUse[link.Type].ContainsKey(link.DestinationItemId))
                        {
                            listToUse[link.Type].Add(link.DestinationItemId, new List<ulong>());
                        }

                        listToUse[link.Type][link.DestinationItemId].Add(link.ItemId);
                    }

                    // Else if the current item is the destination item, then we want to delete all other links of the given source, so that this item will only remain linked the the items from this import.
                    else if (link.DestinationItemId == import.Item.Id)
                    {
                        if (!sourceLinksToKeep.ContainsKey(link.Type))
                        {
                            sourceLinksToKeep.Add(link.Type, new Dictionary<ulong, List<ulong>>());
                        }

                        if (!sourceLinksToKeep[link.Type].ContainsKey(link.ItemId))
                        {
                            sourceLinksToKeep[link.Type].Add(link.ItemId, new List<ulong>());
                        }

                        sourceLinksToKeep[link.Type][link.ItemId].Add(link.DestinationItemId);
                    }
                }

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
                await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Error while trying to import an item link due to exception:\n{e}\n\nItem link details:\n{JsonConvert.SerializeObject(link, serializerSettings)}", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
                errors.Add(e.Message);
            }
        }
    }

    /// <summary>
    /// Import the files to Wiser.
    /// </summary>
    /// <param name="wiserImport">The AIS information for handling the imports.</param>
    /// <param name="import">The data to import.</param>
    /// <param name="databaseConnection">The connection to the database.</param>
    /// <param name="wiserItemsService">The WiserItemsService to use to import the data.</param>
    /// <param name="usernameForLogs">The username to use for the logs in Wiser.</param>
    /// <param name="errors">The list of errors.</param>
    /// <param name="importRow">The information of the import itself.</param>
    /// <param name="configurationServiceName">The name of the configuration the import is executed within.</param>
    private async Task ImportFilesAsync(WiserImportModel wiserImport, ImportDataModel import, IDatabaseConnection databaseConnection, IWiserItemsService wiserItemsService, string usernameForLogs, List<string> errors, ImportRowModel importRow, string configurationServiceName)
    {
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
                    await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, errorMessage, configurationServiceName, wiserImport.TimeId, wiserImport.Order);

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
                await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Error while trying to import an item file:\n{e}\n\nFile details:\n{JsonConvert.SerializeObject(file, serializerSettings)}", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
                errors.Add(e.Message);
            }
        }
    }

    /// <summary>
    /// Cleanup all links that have not been included in this import if it is requested.
    /// </summary>
    /// <param name="wiserImport">The AIS information for handling the imports.</param>
    /// <param name="databaseConnection">The connection to the database.</param>
    /// <param name="wiserItemsService">The WiserItemsService to use to import the data.</param>
    /// <param name="errors">The list of errors.</param>
    /// <param name="destinationLinksToKeep">The list of destination links to keep.</param>
    /// <param name="itemParentIdsToKeep">The list of item parent ids to keep.</param>
    /// <param name="sourceLinksToKeep">The list of source links to keep.</param>
    /// <param name="usernameForLogs">The username to use for the logs in Wiser.</param>
    /// <param name="importRow">The information of the import itself.</param>
    /// <param name="configurationServiceName">The name of the configuration the import is executed within.</param>
    private async Task CleanupLinks(WiserImportModel wiserImport, IDatabaseConnection databaseConnection, IWiserItemsService wiserItemsService, List<string> errors, Dictionary<int, Dictionary<ulong, List<ulong>>> destinationLinksToKeep, Dictionary<int, Dictionary<ulong, List<ulong>>> itemParentIdsToKeep, Dictionary<int, Dictionary<ulong, List<ulong>>> sourceLinksToKeep, string usernameForLogs, ImportRowModel importRow, string configurationServiceName, string entityType)
    {
        foreach (var destinationLink in destinationLinksToKeep)
        {
            var linkType = destinationLink.Key;
            foreach (var destination in destinationLink.Value)
            {
                var destinationItemId = destination.Key;
                var linkSettings = await wiserItemsService.GetLinkTypeSettingsAsync(linkType, sourceEntityType: entityType);
                var linkPrefix = wiserItemsService.GetTablePrefixForLink(linkSettings);

                try
                {
                    databaseConnection.AddParameter("type", linkType);
                    databaseConnection.AddParameter("destinationItemId", destinationItemId);
                    var destinationLinksToDelete = await databaseConnection.GetAsync($"SELECT `id`, item_id AS sourceId, destination_item_id AS destinationId FROM {linkPrefix}{WiserTableNames.WiserItemLink} WHERE type = ?type AND destination_item_id = ?destinationItemId AND item_id NOT IN ({String.Join(",", destination.Value)})");

                    if (destinationLinksToDelete.Rows.Count > 0)
                    {
                        var linkIds = new HashSet<ulong>();
                        var destinationIds = new HashSet<ulong>();
                        var sourceIds = new HashSet<ulong>();

                        foreach (DataRow row in destinationLinksToDelete.Rows)
                        {
                            linkIds.Add(Convert.ToUInt64(row["id"]));
                            destinationIds.Add(Convert.ToUInt64(row["destinationId"]));
                            sourceIds.Add(Convert.ToUInt64(row["sourceId"]));
                        }

                        await wiserItemsService.RemoveItemLinksByIdAsync(linkIds.ToList(), linkSettings.SourceEntityType, sourceIds.ToList(), linkSettings.DestinationEntityType, destinationIds.ToList(), usernameForLogs, importRow.UserId);
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Error while trying to delete item links (destination) of type '{linkType}' due to exception:\n{e}", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
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
                var linkSettings = await wiserItemsService.GetLinkTypeSettingsAsync(linkType, sourceEntityType: entityType);

                try
                {
                    var linkIds = new HashSet<ulong>();
                    var destinationIds = new HashSet<ulong>() {parentItemId};
                    var sourceIds = new HashSet<ulong>();
                    var linkedItems = await wiserItemsService.GetLinkedItemDetailsAsync(parentItemId, linkType, userId: importRow.UserId, entityType: linkSettings.SourceEntityType);

                    foreach (var linkedItem in linkedItems)
                    {
                        if (parent.Value.Contains(linkedItem.Id))
                        {
                            continue;
                        }

                        linkIds.Add(linkedItem.Id);
                        sourceIds.Add(linkedItem.Id);
                    }

                    if (linkIds.Any())
                    {
                        await wiserItemsService.RemoveParentLinkOfItemsAsync(linkIds.ToList(), linkSettings.SourceEntityType, sourceIds.ToList(), linkSettings.DestinationEntityType, destinationIds.ToList(), usernameForLogs, importRow.UserId);
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Error while trying to delete item links (parent) of type '{linkType}' due to exception:\n{e}", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
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
                var linkSettings = await wiserItemsService.GetLinkTypeSettingsAsync(linkType, destinationEntityType: entityType);
                var linkPrefix = wiserItemsService.GetTablePrefixForLink(linkSettings);

                try
                {
                    databaseConnection.AddParameter("type", linkType);
                    databaseConnection.AddParameter("sourceItemId", sourceItemId);
                    var sourceLinksToDelete = await databaseConnection.GetAsync($"SELECT `id`, item_id AS sourceId, destination_item_id AS destinationId FROM {linkPrefix}{WiserTableNames.WiserItemLink} WHERE type = ?type AND item_id = ?sourceItemId AND destination_item_id NOT IN ({String.Join(",", source.Value)})");

                    if (sourceLinksToDelete.Rows.Count > 0)
                    {
                        var linkIds = new HashSet<ulong>();
                        var destinationIds = new HashSet<ulong>();
                        var sourceIds = new HashSet<ulong>();

                        foreach (DataRow row in sourceLinksToDelete.Rows)
                        {
                            linkIds.Add(Convert.ToUInt64(row["id"]));
                            destinationIds.Add(Convert.ToUInt64(row["destinationId"]));
                            sourceIds.Add(Convert.ToUInt64(row["sourceId"]));
                        }

                        await wiserItemsService.RemoveItemLinksByIdAsync(linkIds.ToList(), linkSettings.SourceEntityType, sourceIds.ToList(), linkSettings.DestinationEntityType, destinationIds.ToList(), usernameForLogs, importRow.UserId);
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Error while trying to delete item links (source) of type '{linkType}' due to exception:\n{e}", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
                    errors.Add(e.Message);
                }
            }
        }
    }

    /// <summary>
    /// Notify the user that placed the import using an email about the status of the import.
    /// </summary>
    /// <param name="wiserImport">The AIS information for handling the imports.</param>
    /// <param name="importRow">The information of the import itself.</param>
    /// <param name="databaseConnection">The connection to the database.</param>
    /// <param name="gclCommunicationsService">The communications service from the GCL to add the email to the queue.</param>
    /// <param name="configurationServiceName">The name of the configuration the import is executed within.</param>
    /// <param name="subject">The subject of the email.</param>
    /// <param name="template">An email template to use, if null the default will be used.</param>
    /// <param name="replaceData">The data to be used for replacements.</param>
    /// <param name="stringReplacementsService">The string replacements service to handle the replacements.</param>
    private async Task NotifyUserByEmailAsync(WiserImportModel wiserImport, ImportRowModel importRow, IDatabaseConnection databaseConnection, GclCommunicationsService gclCommunicationsService, string configurationServiceName, string subject, WiserItemModel template, Dictionary<string, object> replaceData, IStringReplacementsService stringReplacementsService)
    {
        databaseConnection.AddParameter("userId", importRow.UserId);
        var userDataTable = await databaseConnection.GetAsync($"SELECT `value` AS receiver FROM {WiserTableNames.WiserItemDetail} WHERE item_id = ?userId AND `key` = 'email_address'");

        if (userDataTable.Rows.Count == 0)
        {
            await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Could not find email address for user '{importRow.UserId}' and import '{importRow.Id}'", configurationServiceName, wiserImport.TimeId, wiserImport.Order);

            // If there is no email address to send the notification to skip it.
            return;
        }

        var receiver = userDataTable.Rows[0].Field<string>("receiver");
        var content = await stringReplacementsService.DoAllReplacementsAsync(stringReplacementsService.DoReplacements(String.IsNullOrWhiteSpace(template?.GetDetailValue("template")) ? DefaultContent : template.GetDetailValue("template"), replaceData));
        await gclCommunicationsService.SendEmailAsync(receiver, subject, content, importRow.Username, sendDate: DateTime.Now, sender: template?.GetDetailValue("sender_email"), senderName: template?.GetDetailValue("sender_name"));
    }

    /// <summary>
    /// Notify the user that placed the import using a task alert about the status of the import.
    /// </summary>
    /// <param name="wiserImport">The AIS information for handling the imports.</param>
    /// <param name="importRow">The information of the import itself.</param>
    /// <param name="wiserItemsService">The Wiser items service to save the task alert.</param>
    /// <param name="configurationServiceName">The name of the configuration the import is executed within.</param>
    /// <param name="usernameForLogs">The username to use for the logs in the database.</param>
    /// <param name="subject">The subject of the task alert.</param>
    private async Task NotifyUserByTaskAlertAsync(WiserImportModel wiserImport, ImportRowModel importRow, IWiserItemsService wiserItemsService, string configurationServiceName, string usernameForLogs, string subject)
    {
        // Create and save the task alert in the database.
        var taskAlert = new WiserItemModel()
        {
            EntityType = "agendering",
            ModuleId = 708,
            PublishedEnvironment = Environments.Live,
            Details = new List<WiserItemDetailModel>()
            {
                new() {Key = "agendering_date", Value = DateTime.Now.ToString("yyyy-MM-dd")},
                new() {Key = "content", Value = subject},
                new() {Key = "userid", Value = importRow.UserId},
                new() {Key = "username", Value = importRow.Username},
                new() {Key = "placed_by", Value = "AIS"},
                new() {Key = "placed_by_id", Value = importRow.UserId}
            }
        };

        await wiserItemsService.SaveAsync(taskAlert, username: usernameForLogs, userId: importRow.UserId);

        // Push the task alert to the user to give a signal within Wiser if it is open.
        var accessToken = await Task.Run(() => wiserService.AccessToken);

        try
        {
            var client = new HttpClient();
            var request = new HttpRequestMessage(HttpMethod.Post, $"{aisSettings.Wiser.WiserApiUrl}api/v3/pusher/message");
            request.Headers.Add("Authorization", $"Bearer {accessToken}");
            request.Content = JsonContent.Create(new {userId = importRow.UserId});
            var response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Failed to notify the user of the import through the task alert pusher, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
            }
        }
        catch (Exception e)
        {
            await logService.LogError(logger, LogScopes.RunBody, wiserImport.LogSettings, $"Failed to notify the user of the import through the task alert pusher due to exception:\n{e}.", configurationServiceName, wiserImport.TimeId, wiserImport.Order);
        }
    }
}