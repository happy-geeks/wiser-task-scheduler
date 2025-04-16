using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Branches.Enumerations;
using GeeksCoreLibrary.Modules.Branches.Helpers;
using GeeksCoreLibrary.Modules.Branches.Models;
using GeeksCoreLibrary.Modules.Databases.Helpers;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.DataSelector.Interfaces;
using GeeksCoreLibrary.Modules.DataSelector.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Branches.Enums;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Branches.Models;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Modules.Branches.Services;

/// <inheritdoc cref="IBranchQueueService" />
public class BranchQueueService(ILogService logService, ILogger<BranchQueueService> logger, IServiceProvider serviceProvider, IOptions<GclSettings> gclSettings, IBranchBatchLoggerService branchBatchLoggerService) : IBranchQueueService, IActionsService, IScopedService
{
    private const string CreateBranchSubject = "Branch with the name '{name}' [if({errorCount}=0)]has been created successfully[else]could not be created[endif] on {date:DateTime(dddd\\, dd MMMM yyyy,en-US)}";
    private const string CreateBranchTemplate = "<p>The branch creation started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The creation took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the creation of the branch: {errors:Raw}[endif]";
    private const string MergeBranchSubject = "Branch with the name '{name}' [if({errorCount}=0)]has been merged successfully[else]could not be merged[endif] on {date:DateTime(dddd\\, dd MMMM yyyy,en-US)}";
    private const string MergeBranchTemplate = "<p>The branch merge started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The merge took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the merge of the branch: {errors:Raw}[endif]";
    private const string DeleteBranchSubject = "Branch with the name '{name}' [if({errorCount}=0)]has been deleted successfully[else]could not be deleted[endif] on {date:DateTime(dddd\\, dd MMMM yyyy,en-US)}";
    private const string DeleteBranchTemplate = "<p>The branch deletion started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The deletion took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the deletion of the branch: {errors:Raw}[endif]";

    private const int BatchSize = 1000;

    private readonly GclSettings gclSettings = gclSettings.Value;

    private string connectionString;

    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        var connectionStringBuilder = new MySqlConnectionStringBuilder(configuration.ConnectionString)
        {
            IgnoreCommandTransaction = true,
            AllowLoadLocalInfile = true,
            ConvertZeroDateTime = true
        };
        connectionString = connectionStringBuilder.ConnectionString;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        // Create a scope for dependency injection.
        using var scope = serviceProvider.CreateScope();
        var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
        var taskAlertsService = scope.ServiceProvider.GetRequiredService<ITaskAlertsService>();
        var branchQueue = (BranchQueueModel) action;

        // Make sure we connect to the correct database.
        await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);
        databaseConnection.ClearParameters();

        await logService.LogInformation(logger, LogScopes.RunStartAndStop, branchQueue.LogSettings, $"Start handling branches queue in time id: {branchQueue.TimeId}, order: {branchQueue.Order}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

        string query;

        if (branchQueue.AutomaticDeployBranchQueueId > 0)
        {
            databaseConnection.AddParameter("automaticDeployBranchQueueId", branchQueue.AutomaticDeployBranchQueueId);
            query = $"SELECT * FROM {WiserTableNames.WiserBranchesQueue} WHERE id = ?automaticDeployBranchQueueId";
        }
        else
        {
            // Use .NET time and not database time, because we often use DigitalOcean, and they have their timezone set to UTC by default.
            databaseConnection.AddParameter("now", DateTime.Now);

            query = $"""
                     SELECT * 
                     FROM {WiserTableNames.WiserBranchesQueue}
                     WHERE started_on IS NULL
                     AND is_template = 0
                     AND is_for_automatic_deploy = 0
                     AND start_on <= ?now
                     ORDER BY start_on ASC, id ASC
                     """;
        }

        var dataTable = await databaseConnection.GetAsync(query);

        var results = new JArray();
        foreach (DataRow dataRow in dataTable.Rows)
        {
            var branchAction = dataRow.Field<string>("action");
            switch (branchAction)
            {
                case "create":
                    results.Add(await HandleCreateBranchActionAsync(dataRow, branchQueue, configurationServiceName, databaseConnection, databaseHelpersService, wiserItemsService, scope, taskAlertsService));
                    break;
                case "merge":
                    results.Add(await HandleMergeBranchActionAsync(dataRow, branchQueue, configurationServiceName, databaseConnection, databaseHelpersService, wiserItemsService, taskAlertsService));
                    break;
                case "delete":
                    results.Add(await HandleDeleteBranchActionAsync(dataRow, branchQueue, configurationServiceName, databaseConnection, databaseHelpersService, wiserItemsService, taskAlertsService));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(branchAction), branchAction, null);
            }
        }

        return new JObject
        {
            {"Results", results}
        };
    }

    /// <inheritdoc />
    public MySqlConnectionStringBuilder GetConnectionStringBuilderForBranch(BranchActionBaseModel branchActionBaseModel, string database, bool allowLoadLocalInfile = false)
    {
        var connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString)
        {
            IgnoreCommandTransaction = true,
            AllowLoadLocalInfile = allowLoadLocalInfile,
            ConvertZeroDateTime = true,
            Database = database
        };

        if (!String.IsNullOrWhiteSpace(branchActionBaseModel.DatabaseHost))
        {
            connectionStringBuilder.Server = branchActionBaseModel.DatabaseHost.DecryptWithAesWithSalt(gclSettings.DefaultEncryptionKey, useSlowerButMoreSecureMethod: true);
        }

        if (branchActionBaseModel.DatabasePort is > 0)
        {
            connectionStringBuilder.Port = (uint) branchActionBaseModel.DatabasePort.Value;
        }

        if (!String.IsNullOrWhiteSpace(branchActionBaseModel.DatabaseUsername))
        {
            connectionStringBuilder.UserID = branchActionBaseModel.DatabaseUsername.DecryptWithAesWithSalt(gclSettings.DefaultEncryptionKey, useSlowerButMoreSecureMethod: true);
        }

        if (!String.IsNullOrWhiteSpace(branchActionBaseModel.DatabasePassword))
        {
            connectionStringBuilder.Password = branchActionBaseModel.DatabasePassword.DecryptWithAesWithSalt(gclSettings.DefaultEncryptionKey, useSlowerButMoreSecureMethod: true);
        }

        return connectionStringBuilder;
    }

    /// <summary>
    /// Handles the creation of a new branch. This will create the new database and fill it with the requested data.
    /// </summary>
    /// <param name="dataRowWithSettings">The <see cref="DataRow"/> from wiser_branch_queue.</param>
    /// <param name="branchQueue">The <see cref="BranchQueueModel"/> with the settings from the XML configuration.</param>
    /// <param name="configurationServiceName">The name of the configuration.</param>
    /// <param name="databaseConnection">The <see cref="IDatabaseConnection"/> with the connection to the database.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> for checking if a table exists, creating new tables etc.</param>
    /// <param name="wiserItemsService">The <see cref="IWiserItemsService"/> for getting settings of entity types and for (un)deleting items.</param>
    /// <param name="scope">The <see cref="IServiceScope"/> for dependency injection.</param>
    /// <param name="taskAlertsService">The <see cref="ITaskAlertsService"/> for sending notification to the user.</param>
    /// <returns>An <see cref="JObject"/> with properties "Success" and "ErrorMessage".</returns>
    /// <exception cref="ArgumentOutOfRangeException">Then we get unknown options in enums.</exception>
    private async Task<JObject> HandleCreateBranchActionAsync(DataRow dataRowWithSettings, BranchQueueModel branchQueue, string configurationServiceName, IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, IWiserItemsService wiserItemsService, IServiceScope scope, ITaskAlertsService taskAlertsService)
    {
        var errors = new JArray();
        var result = new JObject
        {
            {"SuccessfulChanges", 0},
            {"Errors", errors}
        };

        // Set the start date to the current datetime.
        var startDate = DateTime.Now;
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        var queueId = dataRowWithSettings.Field<int>("id");
        databaseConnection.SetCommandTimeout(900);
        databaseConnection.AddParameter("queueId", queueId);
        databaseConnection.AddParameter("now", startDate);
        await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET started_on = ?now WHERE id = ?queueId");

        // Get and validate the settings.
        var settings = JsonConvert.DeserializeObject<CreateBranchSettingsModel>(dataRowWithSettings.Field<string>("data") ?? "{}");
        if (String.IsNullOrWhiteSpace(settings?.DatabaseName))
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to create a branch, but it either had invalid settings, or the database name was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

            errors.Add("Trying to create a branch, but it either had invalid settings, or the database name was empty.");
            result.Add("ErrorMessage", errors[0]);
            result.Add("Success", false);
            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);
            return result;
        }

        // Create a new scope for dependency injection and get a new instance of the database connection, to use for connecting to the branch database.
        // This is because we need to keep two separate connections open at the same time, one for production database and one for the branch database.
        var branchDatabase = settings.DatabaseName;
        using var branchScope = serviceProvider.CreateScope();
        var branchDatabaseConnection = branchScope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        var branchDatabaseHelpersService = branchScope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        branchDatabaseConnection.SetCommandTimeout(900);

        try
        {
            await databaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WiserBranchesQueue]);

            // Build the connection strings.
            var productionConnectionStringBuilder = new MySqlConnectionStringBuilder(connectionString)
            {
                IgnoreCommandTransaction = true,
                AllowLoadLocalInfile = true,
                ConvertZeroDateTime = true
            };

            var branchConnectionStringBuilder = GetConnectionStringBuilderForBranch(settings, "", true);

            // If the branch database is on the same server as the production database, we can use a quicker and more efficient way of copying data.
            var branchIsOnSameServerAsProduction = String.Equals(productionConnectionStringBuilder.Server, branchConnectionStringBuilder.Server, StringComparison.OrdinalIgnoreCase);

            // Change connection string to one with a specific user for deleting a database.
            if (!String.IsNullOrWhiteSpace(branchQueue.UsernameForManagingBranches) && !String.IsNullOrWhiteSpace(branchQueue.PasswordForManagingBranches))
            {
                productionConnectionStringBuilder.UserID = branchQueue.UsernameForManagingBranches;
                productionConnectionStringBuilder.Password = branchQueue.PasswordForManagingBranches;

                if (branchIsOnSameServerAsProduction)
                {
                    // If the branch is on the same server as the production database, we can use the same user for both.
                    branchConnectionStringBuilder.UserID = branchQueue.UsernameForManagingBranches;
                    branchConnectionStringBuilder.Password = branchQueue.PasswordForManagingBranches;
                }
            }

            await databaseConnection.ChangeConnectionStringsAsync(productionConnectionStringBuilder.ConnectionString, productionConnectionStringBuilder.ConnectionString);
            await branchDatabaseConnection.ChangeConnectionStringsAsync(branchConnectionStringBuilder.ConnectionString, branchConnectionStringBuilder.ConnectionString);

            // Make sure that the database doesn't exist yet.
            if (await branchDatabaseHelpersService.DatabaseExistsAsync(branchDatabase))
            {
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to create a branch, but a database with name '{branchDatabase}' already exists. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

                errors.Add($"Trying to create a branch, but a database with name '{branchDatabase}' already exists.");
                result.Add("ErrorMessage", errors[0]);
                result.Add("Success", false);
                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);
                return result;
            }

            // Create the database in the same server/cluster. We already check if the database exists before this, so we can safely do this here.
            await branchDatabaseHelpersService.CreateDatabaseAsync(branchDatabase);

            var originalDatabase = databaseConnection.ConnectedDatabase;
            branchConnectionStringBuilder.Database = branchDatabase;

            // Update connection string again after creating and selecting the new database schema.
            await branchDatabaseConnection.ChangeConnectionStringsAsync(branchConnectionStringBuilder.ConnectionString, branchConnectionStringBuilder.ConnectionString);

            // Get all tables that don't start with an underscore (wiser tables never start with an underscore, and we often use that for temporary or backup tables).
            // Handle the wiser_itemfile tables as last to ensure all other tables are copied first.
            var query = $"""
                         SELECT *
                         FROM (
                             SELECT TABLE_NAME 
                             FROM INFORMATION_SCHEMA.TABLES
                             WHERE TABLE_SCHEMA = ?currentSchema
                             AND TABLE_TYPE = 'BASE TABLE'
                             AND TABLE_NAME NOT LIKE '\_%'
                             AND TABLE_NAME NOT LIKE '%{WiserTableNames.WiserItemFile}%'
                             ORDER BY TABLE_NAME ASC
                         ) AS x

                         UNION ALL

                         SELECT *
                         FROM (
                             SELECT TABLE_NAME 
                             FROM INFORMATION_SCHEMA.TABLES
                             WHERE TABLE_SCHEMA = ?currentSchema
                             AND TABLE_TYPE = 'BASE TABLE'
                             AND TABLE_NAME NOT LIKE '\_%'
                             AND TABLE_NAME LIKE '%{WiserTableNames.WiserItemFile}%'
                             ORDER BY TABLE_NAME ASC
                         ) AS x
                         """;

            databaseConnection.AddParameter("currentSchema", originalDatabase);
            var dataTable = await databaseConnection.GetAsync(query);

            // We don't want to copy the contents of log tables and certain other tables to the new branch.
            var tablesToAlwaysLeaveEmpty = new List<string>
            {
                WiserTableNames.WiserHistory,
                WiserTableNames.WiserImport,
                WiserTableNames.WiserImportLog,
                WiserTableNames.WiserUsersAuthenticationTokens,
                WiserTableNames.WiserCommunicationGenerated,
                WiserTableNames.WtsLogs,
                WiserTableNames.WtsServices,
                WiserTableNames.WiserBranchesQueue,
                "ais_logs",
                "ais_services",
                "jcl_email"
            };

            foreach (DataRow dataRow in dataTable.Rows)
            {
                var tableName = dataRow.Field<string>("TABLE_NAME");

                // Check if the structure of the table is excluded from the creation of the branch.
                if (branchQueue.CopyTableRules != null && branchQueue.CopyTableRules.Any(t => t.CopyType == CopyTypes.Nothing && (
                        (t.TableName.StartsWith('%') && t.TableName.EndsWith('%') && tableName.Contains(t.TableName.Substring(1, t.TableName.Length - 2), StringComparison.OrdinalIgnoreCase))
                        || (t.TableName.StartsWith('%') && tableName.EndsWith(t.TableName[1..], StringComparison.OrdinalIgnoreCase))
                        || (t.TableName.EndsWith('%') && tableName.StartsWith(t.TableName[..^1], StringComparison.OrdinalIgnoreCase))
                        || tableName.Equals(t.TableName, StringComparison.OrdinalIgnoreCase))))
                {
                    continue;
                }

                var createTableResult = await databaseConnection.GetAsync($"SHOW CREATE TABLE `{tableName.ToMySqlSafeValue(false)}`");
                await branchDatabaseConnection.ExecuteAsync(createTableResult.Rows[0].Field<string>("Create table"));
            }

            // Create the wiser_id_mappings table in the new branch.
            // This is used to know which IDs are already synced to the production environment.
            await branchDatabaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WiserIdMappings]);

            // Cache some settings that we'll need later.
            var allLinkTypes = await wiserItemsService.GetAllLinkTypeSettingsAsync();

            // Fill the tables with data.
            var copiedItemIds = new Dictionary<string, List<ulong>>();
            var copiedItemLinks = new Dictionary<string, List<ulong>>();
            var entityTypesDone = new List<string>();
            foreach (DataRow dataRow in dataTable.Rows)
            {
                var tableName = dataRow.Field<string>("TABLE_NAME");

                // Skip copying the contents of certain tables if the table should not be copied or only the structure.
                if (branchQueue.CopyTableRules != null && branchQueue.CopyTableRules.Any(t => (t.CopyType == CopyTypes.Nothing || t.CopyType == CopyTypes.Structure) && (
                        (t.TableName.StartsWith('%') && t.TableName.EndsWith('%') && tableName.Contains(t.TableName.Substring(1, t.TableName.Length - 2), StringComparison.OrdinalIgnoreCase))
                        || (t.TableName.StartsWith('%') && tableName.EndsWith(t.TableName[1..], StringComparison.OrdinalIgnoreCase))
                        || (t.TableName.EndsWith('%') && tableName.StartsWith(t.TableName[..^1], StringComparison.OrdinalIgnoreCase))
                        || tableName.Equals(t.TableName, StringComparison.OrdinalIgnoreCase))))
                {
                    continue;
                }

                var bulkCopy = new MySqlBulkCopy((MySqlConnection) branchDatabaseConnection.GetConnectionForWriting()) {DestinationTableName = tableName, ConflictOption = MySqlBulkLoaderConflictOption.Ignore};

                // For Wiser tables, we don't want to copy customer data, so copy everything except data of certain entity types.
                if (tableName!.EndsWith(WiserTableNames.WiserItem, StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var entity in settings.Entities)
                    {
                        if (String.IsNullOrWhiteSpace(entity.EntityType))
                        {
                            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy items of entity type to new branch, but it either had invalid settings, or the entity name was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                            continue;
                        }

                        // If we've already done items of this type, skip it.
                        if (entityTypesDone.Contains(entity.EntityType))
                        {
                            continue;
                        }

                        var orderBy = "";
                        var whereClauseBuilder = new StringBuilder($"WHERE item.entity_type = '{entity.EntityType.ToMySqlSafeValue(false)}'");

                        var startDateParameter = $"{entity.EntityType}_startOn";
                        var endDateParameter = $"{entity.EntityType}_endOn";

                        switch (entity.Mode)
                        {
                            case CreateBranchEntityModes.Everything:
                                // If the user wants to copy everything of this entity type, we don't need to do anymore checks.
                                break;
                            case CreateBranchEntityModes.Random:
                                if (entity.AmountOfItems <= 0)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy random X items of entity type '{entity.EntityType}' to new branch, but it either had invalid settings, or the AmountOfItems setting was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                orderBy = $"ORDER BY RAND() LIMIT {entity.AmountOfItems}";
                                break;
                            case CreateBranchEntityModes.Recent:
                                if (entity.AmountOfItems <= 0)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy recent most X items of entity type '{entity.EntityType}' to new branch, but it either had invalid settings, or the AmountOfItems setting was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                orderBy = $"ORDER BY IFNULL(item.changed_on, item.added_on) LIMIT {entity.AmountOfItems}";
                                break;
                            case CreateBranchEntityModes.CreatedBefore:
                                if (!entity.Start.HasValue || entity.Start.Value == DateTime.MinValue)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy items created before X of entity type '{entity.EntityType}' to new branch, but it either had invalid settings, or the Start date setting was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                databaseConnection.AddParameter(startDateParameter, entity.Start);
                                whereClauseBuilder.AppendLine($"AND item.added_on < ?{startDateParameter}");
                                break;
                            case CreateBranchEntityModes.CreatedAfter:
                                if (!entity.End.HasValue || entity.End.Value == DateTime.MinValue)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy items created before X of entity type '{entity.EntityType}' to new branch, but it either had invalid settings, or the End date setting was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                databaseConnection.AddParameter(endDateParameter, entity.End);
                                whereClauseBuilder.AppendLine($"AND item.added_on > ?{endDateParameter}");
                                break;
                            case CreateBranchEntityModes.CreatedBetween:
                                if (!entity.Start.HasValue || entity.Start.Value == DateTime.MinValue)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy items created before X of entity type '{entity.EntityType}' to new branch, but it either had invalid settings, or the Start date setting was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                if (!entity.End.HasValue || entity.End.Value == DateTime.MinValue)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to copy items created before X of entity type '{entity.EntityType}' to new branch, but it either had invalid settings, or the End date setting was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                databaseConnection.AddParameter(endDateParameter, entity.End);
                                databaseConnection.AddParameter(startDateParameter, entity.Start);

                                whereClauseBuilder.AppendLine($"AND added_on BETWEEN ?{startDateParameter} AND ?{endDateParameter}");
                                break;
                            case CreateBranchEntityModes.DataSelector:
                                if (entity.DataSelector <= 0)
                                {
                                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, "", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    continue;
                                }

                                var dataSelectorsService = scope.ServiceProvider.GetRequiredService<IDataSelectorsService>();
                                var dataSelectorSettings = new DataSelectorRequestModel
                                {
                                    DataSelectorId = entity.DataSelector
                                };

                                var (dataSelectorResult, _, _) = await dataSelectorsService.GetJsonResponseAsync(dataSelectorSettings, true);

                                if (!dataSelectorResult.Any())
                                {
                                    continue;
                                }

                                var dataSelectorIds = dataSelectorResult.Select(i => i["id"]).ToList();
                                whereClauseBuilder.AppendLine($"AND item.id IN ({String.Join(", ", dataSelectorIds)})");
                                break;
                            case CreateBranchEntityModes.Nothing:
                                // If mode is nothing, skip everything of this entity type.
                                continue;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(entity.Mode), entity.Mode.ToString(), null);
                        }

                        // Build a query to get all items of the current entity type and all items that are linked to those items.
                        var columnsClause = String.Join(", ", WiserTableDefinitions.TablesToUpdate.Single(t => t.Name == WiserTableNames.WiserItem).Columns.Select(c => $"{{0}}`{c.Name}`"));
                        var whereClause = whereClauseBuilder.ToString();
                        var queryBuilder = new StringBuilder();
                        if (branchIsOnSameServerAsProduction)
                        {
                            queryBuilder.Append($"""
                                                 INSERT IGNORE INTO `{branchDatabase}`.`{tableName}`
                                                 ({String.Format(columnsClause, "")})
                                                 """);
                        }

                        queryBuilder.Append($"""
                                             (
                                                 SELECT {String.Format(columnsClause, "item.")}
                                                 FROM `{originalDatabase}`.`{tableName}` AS item
                                                 {whereClause}
                                                 {orderBy}
                                             )
                                             UNION
                                             (
                                                 SELECT {String.Format(columnsClause, "linkedItem.")}
                                                 FROM `{originalDatabase}`.`{tableName}` AS item
                                                 JOIN `{originalDatabase}`.`{tableName}` AS linkedItem ON linkedItem.parent_item_id = item.id
                                                 {whereClause}
                                                 {orderBy}
                                             )
                                             """);

                        var linkTypes = allLinkTypes.Where(t => String.Equals(t.DestinationEntityType, entity.EntityType, StringComparison.OrdinalIgnoreCase)).ToList();

                        if (!linkTypes.Any())
                        {
                            queryBuilder.AppendLine($"""
                                                     UNION
                                                     (
                                                         SELECT {String.Format(columnsClause, "linkedItem.")}
                                                         FROM `{originalDatabase}`.`{tableName}` AS item
                                                         JOIN `{originalDatabase}`.`{WiserTableNames.WiserItemLink}` AS link ON link.destination_item_id = item.id
                                                         JOIN `{originalDatabase}`.`{tableName}` AS linkedItem ON linkedItem.id = link.item_id
                                                         {whereClause}
                                                         {orderBy}
                                                     )
                                                     """);
                        }
                        else
                        {
                            for (var i = 0; i < linkTypes.Count; i++)
                            {
                                var linkType = linkTypes[i];
                                var linkTablePrefix = wiserItemsService.GetTablePrefixForLink(linkType);
                                var itemTablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(linkType.SourceEntityType);
                                databaseConnection.AddParameter($"linkType{i}", linkType.Type);
                                databaseConnection.AddParameter($"sourceEntityType{i}", linkType.SourceEntityType);
                                queryBuilder.AppendLine($"""
                                                         UNION
                                                         (
                                                             SELECT {String.Format(columnsClause, "linkedItem.")}
                                                             FROM `{originalDatabase}`.`{tableName}` AS item
                                                             JOIN `{originalDatabase}`.`{linkTablePrefix}{WiserTableNames.WiserItemLink}` AS link ON link.destination_item_id = item.id AND link.type = ?linkType{i}
                                                             JOIN `{originalDatabase}`.`{itemTablePrefix}{WiserTableNames.WiserItem}` AS linkedItem ON linkedItem.id = link.item_id AND linkedItem.entity_type = ?sourceEntityType{i}
                                                             {whereClause}
                                                             {orderBy}
                                                         )
                                                         """);
                            }
                        }

                        if (branchIsOnSameServerAsProduction)
                        {
                            await databaseConnection.ExecuteAsync(queryBuilder.ToString());
                        }
                        else
                        {
                            var counter = 0;
                            var totalRowsInserted = 0;
                            while (true)
                            {
                                // Get the data from the production database.
                                var items = await databaseConnection.GetAsync($"{queryBuilder} ORDER BY id ASC LIMIT {counter * BatchSize}, {BatchSize}");
                                if (items.Rows.Count == 0)
                                {
                                    if (totalRowsInserted > 0)
                                    {
                                        entityTypesDone.Add(entity.EntityType);
                                    }

                                    break;
                                }

                                var prefix = tableName.Replace(WiserTableNames.WiserItem, "");
                                var ids = items.Rows.Cast<DataRow>().Select(x => x.Field<ulong>("id")).ToList();
                                if (!copiedItemIds.TryAdd(prefix, ids))
                                {
                                    copiedItemIds[prefix].AddRange(ids);
                                }

                                // Insert the data into the new branch database.
                                totalRowsInserted += await BulkInsertDataTableAsync(branchQueue, configurationServiceName, bulkCopy, items, tableName, branchDatabase, branchDatabaseConnection);

                                counter++;
                            }
                        }
                    }

                    await AddInitialIdMappingAsync(branchDatabaseConnection, tableName);

                    continue;
                }

                if (tableName!.EndsWith(WiserTableNames.WiserItemDetail, StringComparison.OrdinalIgnoreCase))
                {
                    // We order tables by table name, this means wiser_item always comes before wiser_itemdetail.
                    // So we can be sure that we already copied the items to the new branch, and we can use the IDs of those items to copy the details of those items.
                    // This way, we don't need to create the entire WHERE statement again based on the entity settings, like we did above for wiser_item.
                    var prefix = tableName.Replace(WiserTableNames.WiserItemDetail, "");

                    // We need to get all columns of the wiser_itemdetail table like this, instead of using SELECT *,
                    // because they can have virtual columns and you can't manually insert values into those.
                    var table = WiserTableDefinitions.TablesToUpdate.Single(x => x.Name == WiserTableNames.WiserItemDetail);
                    var itemDetailColumns = table.Columns.Where(x => !x.IsVirtual).Select(x => $"`{x.Name}`").ToList();

                    if (branchIsOnSameServerAsProduction)
                    {
                        await databaseConnection.ExecuteAsync($"""
                                                               INSERT INTO `{branchDatabase}`.`{tableName}` ({String.Join(", ", itemDetailColumns)})
                                                               SELECT {String.Join(", ", itemDetailColumns.Select(x => $"detail.{x}"))}
                                                               FROM `{originalDatabase}`.`{tableName}` AS detail
                                                               JOIN `{branchDatabase}`.`{prefix}{WiserTableNames.WiserItem}` AS item ON item.id = detail.item_id
                                                               """);
                    }
                    else
                    {
                        if (!copiedItemIds.TryGetValue(prefix, out var itemIds) || !itemIds.Any())
                        {
                            // There were no items copied from the wiser_item table, so we can't copy the details of those items.
                            continue;
                        }

                        var counter = 0;
                        while (true)
                        {
                            // Get the data from the production database.
                            var items = await databaseConnection.GetAsync($"""
                                                                           SELECT {String.Join(", ", itemDetailColumns.Select(x => $"detail.{x}"))}
                                                                           FROM `{originalDatabase}`.`{tableName}` AS detail
                                                                           WHERE detail.item_id IN ({String.Join(", ", itemIds)})
                                                                           ORDER BY detail.id ASC 
                                                                           LIMIT {counter * BatchSize}, {BatchSize}
                                                                           """);
                            if (items.Rows.Count == 0)
                            {
                                break;
                            }

                            // Insert the data into the new branch database.
                            await BulkInsertDataTableAsync(branchQueue, configurationServiceName, bulkCopy, items, tableName, branchDatabase, branchDatabaseConnection);
                            counter++;
                        }
                    }

                    continue;
                }

                if (tableName!.EndsWith(WiserTableNames.WiserItemFile, StringComparison.OrdinalIgnoreCase))
                {
                    // The wiser_itemfile tables are handled last, this means wiser_item and wiser_itemlink always comes before wiser_itemfile.
                    // So we can be sure that we already copied the items to the new branch, and we can use the IDs of those items to copy the details of those items.
                    // This way, we don't need to create the entire WHERE statement again based on the entity settings, like we did above for wiser_item.
                    var prefix = tableName.Replace(WiserTableNames.WiserItemFile, "");

                    if (await databaseHelpersService.TableExistsAsync($"{prefix}{WiserTableNames.WiserItem}"))
                    {
                        if (branchIsOnSameServerAsProduction)
                        {
                            await databaseConnection.ExecuteAsync($"""
                                                                   INSERT INTO `{branchDatabase}`.`{tableName}` 
                                                                   SELECT file.* FROM `{originalDatabase}`.`{tableName}` AS file
                                                                   JOIN `{branchDatabase}`.`{prefix}{WiserTableNames.WiserItem}` AS item ON item.id = file.item_id
                                                                   """);
                        }
                        else
                        {
                            if (!copiedItemIds.TryGetValue(prefix, out var itemIds) || !itemIds.Any())
                            {
                                // There were no items copied from the wiser_item table, so we can't copy the files of those items.
                                continue;
                            }

                            var counter = 0;
                            while (true)
                            {
                                // Get the data from the production database.
                                var items = await databaseConnection.GetAsync($"""
                                                                               SELECT file.*
                                                                               FROM `{originalDatabase}`.`{tableName}` AS file
                                                                               WHERE file.item_id IN ({String.Join(", ", itemIds)})
                                                                               ORDER BY file.id ASC
                                                                               LIMIT {counter * BatchSize}, {BatchSize}
                                                                               """);
                                if (items.Rows.Count == 0)
                                {
                                    break;
                                }

                                // Insert the data into the new branch database.
                                await BulkInsertDataTableAsync(branchQueue, configurationServiceName, bulkCopy, items, tableName, branchDatabase, branchDatabaseConnection);
                                counter++;
                            }
                        }

                        // Copy all files that are on the links that are copied to the new branch from the given (prefixed) table.
                        var entityTypesInTable = await databaseConnection.GetAsync($"SELECT DISTINCT entity_type FROM {prefix}{WiserTableNames.WiserItem} WHERE entity_type <> ''");
                        if (entityTypesInTable.Rows.Count > 0)
                        {
                            var processedLinkTypes = new List<string>();
                            foreach (var entityType in entityTypesInTable.Rows.Cast<DataRow>().Select(x => x.Field<string>("entity_type")))
                            {
                                // Get all link types that are connected to the entity type being processed.
                                var linkTypes = allLinkTypes.Where(t => String.Equals(t.SourceEntityType, entityType, StringComparison.OrdinalIgnoreCase)).ToList();
                                foreach (var linkType in linkTypes)
                                {
                                    // Check if the prefix already has been processed from the current (prefixed) table.
                                    var linkPrefix = await wiserItemsService.GetTablePrefixForLinkAsync(linkType.Type, entityType);
                                    if (processedLinkTypes.Contains(linkPrefix))
                                    {
                                        continue;
                                    }

                                    processedLinkTypes.Add(linkPrefix);

                                    if (branchIsOnSameServerAsProduction)
                                    {
                                        await databaseConnection.ExecuteAsync($"""
                                                                               INSERT IGNORE INTO `{branchDatabase}`.`{tableName}`
                                                                               SELECT file.* FROM `{originalDatabase}`.`{tableName}` AS file
                                                                               JOIN `{branchDatabase}`.`{linkPrefix}{WiserTableNames.WiserItemLink}` AS link ON link.id = file.itemlink_id
                                                                               """);
                                    }
                                    else
                                    {
                                        if (!copiedItemLinks.TryGetValue(linkPrefix, out var linkIds) || !linkIds.Any())
                                        {
                                            // There were no items copied from the wiser_itemlink table, so we can't copy the files of those items.
                                            continue;
                                        }

                                        var counter = 0;
                                        while (true)
                                        {
                                            // Get the data from the production database.
                                            var items = await databaseConnection.GetAsync($"""
                                                                                           SELECT file.*
                                                                                           FROM `{originalDatabase}`.`{tableName}` AS file
                                                                                           WHERE file.itemlink_id IN ({String.Join(", ", linkIds)})
                                                                                           ORDER BY file.id ASC 
                                                                                           LIMIT {counter * BatchSize}, {BatchSize}
                                                                                           """);
                                            if (items.Rows.Count == 0)
                                            {
                                                break;
                                            }

                                            // Insert the data into the new branch database.
                                            await BulkInsertDataTableAsync(branchQueue, configurationServiceName, bulkCopy, items, tableName, branchDatabase, branchDatabaseConnection);
                                            counter++;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (await databaseHelpersService.TableExistsAsync($"{prefix}{WiserTableNames.WiserItemLink}"))
                    {
                        // Copy all files that are on the links that are copied to the new branch from the given (prefixed) table.
                        if (branchIsOnSameServerAsProduction)
                        {
                            await databaseConnection.ExecuteAsync($"""
                                                                   INSERT IGNORE INTO `{branchDatabase}`.`{tableName}`
                                                                   SELECT file.* FROM `{originalDatabase}`.`{tableName}` AS file
                                                                   JOIN `{branchDatabase}`.`{prefix}{WiserTableNames.WiserItemLink}` AS link ON link.id = file.itemlink_id
                                                                   """);
                        }
                        else
                        {
                            if (!copiedItemLinks.TryGetValue(prefix, out var linkIds) || !linkIds.Any())
                            {
                                // There were no items copied from the wiser_itemlink table, so we can't copy the files of those items.
                                continue;
                            }

                            var counter = 0;
                            while (true)
                            {
                                // Get the data from the production database.
                                var items = await databaseConnection.GetAsync($"""
                                                                               SELECT file.*
                                                                               FROM `{originalDatabase}`.`{tableName}` AS file
                                                                               WHERE file.itemlink_id IN ({String.Join(", ", linkIds)})
                                                                               ORDER BY file.id ASC 
                                                                               LIMIT {counter * BatchSize}, {BatchSize}
                                                                               """);
                                if (items.Rows.Count == 0)
                                {
                                    break;
                                }

                                // Insert the data into the new branch database.
                                await BulkInsertDataTableAsync(branchQueue, configurationServiceName, bulkCopy, items, tableName, branchDatabase, branchDatabaseConnection);
                                counter++;
                            }
                        }
                    }

                    continue;
                }

                // Don't copy data from certain tables, such as log and archive tables.
                if (tablesToAlwaysLeaveEmpty.Any(t => String.Equals(t, tableName, StringComparison.OrdinalIgnoreCase))
                    || tableName!.StartsWith("log_", StringComparison.OrdinalIgnoreCase)
                    || tableName.EndsWith("_log", StringComparison.OrdinalIgnoreCase)
                    || tableName.EndsWith(WiserTableNames.ArchiveSuffix))
                {
                    continue;
                }

                // Don't copy data from tables that have specific rules. Either the table needs to stay empty or it does not exist in the new branch.
                if (branchQueue.CopyTableRules != null && branchQueue.CopyTableRules.Any(t => (t.TableName.StartsWith('%') && t.TableName.EndsWith('%') && tableName.Contains(t.TableName.Substring(1, t.TableName.Length - 2), StringComparison.OrdinalIgnoreCase))
                                                                                              || (t.TableName.StartsWith('%') && tableName.EndsWith(t.TableName[1..], StringComparison.OrdinalIgnoreCase))
                                                                                              || (t.TableName.EndsWith('%') && tableName.StartsWith(t.TableName[..^1], StringComparison.OrdinalIgnoreCase))
                                                                                              || tableName.Equals(t.TableName, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                // Get all columns that are not generated. We need to do this to support document store tables.
                query = """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = ?tableName
                        AND TABLE_SCHEMA = ?currentSchema
                        AND EXTRA NOT LIKE '%GENERATED'
                        """;
                databaseConnection.AddParameter("tableName", tableName);
                var columnsDataTable = await databaseConnection.GetAsync(query);
                if (columnsDataTable.Rows.Count == 0)
                {
                    continue;
                }

                var columns = columnsDataTable.Rows.Cast<DataRow>().Select(row => $"`{row.Field<string>("COLUMN_NAME")}`").ToList();

                // For all other tables, always copy everything to the new branch.
                if (branchIsOnSameServerAsProduction)
                {
                    query = $"""
                             INSERT INTO `{branchDatabase}`.`{tableName}` ({String.Join(", ", columns)})
                             SELECT {String.Join(", ", columns)} FROM `{originalDatabase}`.`{tableName}`
                             """;
                    await databaseConnection.ExecuteAsync(query);
                }
                else
                {
                    var counter = 0;
                    while (true)
                    {
                        // Get the data from the production database.
                        var sortColumn = "id ASC";
                        if (!columns.Any(c => String.Equals(c, "`id`", StringComparison.OrdinalIgnoreCase)))
                        {
                            sortColumn = String.Join(", ", columns.Select(c => $"{c} ASC"));
                        }

                        query = $"SELECT {String.Join(", ", columns)} FROM `{originalDatabase}`.`{tableName}` ORDER BY {sortColumn} LIMIT {counter * BatchSize}, {BatchSize}";
                        var items = await databaseConnection.GetAsync(query);
                        if (items.Rows.Count == 0)
                        {
                            break;
                        }

                        // Insert the data into the new branch database.
                        await BulkInsertDataTableAsync(branchQueue, configurationServiceName, bulkCopy, items, tableName, branchDatabase, branchDatabaseConnection);

                        if (tableName.EndsWith(WiserTableNames.WiserItemLink))
                        {
                            var prefix = tableName.Replace(WiserTableNames.WiserItem, "");
                            var ids = items.Rows.Cast<DataRow>().Select(x => Convert.ToUInt64(x["id"])).ToList();
                            if (!copiedItemLinks.TryAdd(prefix, ids))
                            {
                                copiedItemLinks[prefix].AddRange(ids);
                            }
                        }

                        counter++;
                    }
                }
            }

            // Add triggers to the new database, after inserting all data, so that the wiser_history table will still be empty.
            // We use wiser_history to later synchronise all changes to production, so it needs to be empty before the user starts to make changes in the new branch.
            query = """
                    SELECT 
                        TRIGGER_NAME,
                        EVENT_MANIPULATION,
                        EVENT_OBJECT_TABLE,
                    	ACTION_STATEMENT,
                    	ACTION_ORIENTATION,
                    	ACTION_TIMING
                    FROM information_schema.TRIGGERS
                    WHERE TRIGGER_SCHEMA = ?currentSchema
                    AND EVENT_OBJECT_TABLE NOT LIKE '\_%'
                    """;
            dataTable = await databaseConnection.GetAsync(query);

            foreach (DataRow dataRow in dataTable.Rows)
            {
                var tableName = dataRow.Field<string>("EVENT_OBJECT_TABLE");

                // Check if the structure of the table is excluded from the creation of the branch.
                if (branchQueue.CopyTableRules != null && branchQueue.CopyTableRules.Any(t => t.CopyType == CopyTypes.Nothing && (
                        (t.TableName.StartsWith('%') && t.TableName.EndsWith('%') && tableName.Contains(t.TableName.Substring(1, t.TableName.Length - 2), StringComparison.OrdinalIgnoreCase))
                        || (t.TableName.StartsWith('%') && tableName.EndsWith(t.TableName[1..], StringComparison.OrdinalIgnoreCase))
                        || (t.TableName.EndsWith('%') && tableName.StartsWith(t.TableName[..^1], StringComparison.OrdinalIgnoreCase))
                        || tableName.Equals(t.TableName, StringComparison.OrdinalIgnoreCase))))
                {
                    continue;
                }

                query = $"CREATE TRIGGER `{dataRow.Field<string>("TRIGGER_NAME")}` {dataRow.Field<string>("ACTION_TIMING")} {dataRow.Field<string>("EVENT_MANIPULATION")} ON `{branchDatabase.ToMySqlSafeValue(false)}`.`{dataRow.Field<string>("EVENT_OBJECT_TABLE")}` FOR EACH {dataRow.Field<string>("ACTION_ORIENTATION")} {dataRow.Field<string>("ACTION_STATEMENT")}";
                await branchDatabaseConnection.ExecuteAsync(query);
            }

            // Add stored procedures/functions to the new database.
            query = """
                    SELECT
                        ROUTINE_NAME,
                        ROUTINE_TYPE,
                        DEFINER
                    FROM INFORMATION_SCHEMA.ROUTINES 
                    WHERE ROUTINE_SCHEMA = ?currentSchema
                    AND ROUTINE_NAME NOT LIKE '\_%'
                    """;
            dataTable = await databaseConnection.GetAsync(query);
            foreach (DataRow dataRow in dataTable.Rows)
            {
                try
                {
                    var definer = dataRow.Field<string>("DEFINER");
                    var definerParts = definer.Split('@');
                    query = $"SHOW CREATE {dataRow.Field<string>("ROUTINE_TYPE")} `{originalDatabase.ToMySqlSafeValue(false)}`.`{dataRow.Field<string>("ROUTINE_NAME")}`";
                    var subDataTable = await databaseConnection.GetAsync(query);
                    query = subDataTable.Rows[0].Field<string>(2);

                    if (String.IsNullOrEmpty(query))
                    {
                        errors.Add($"Unable to create stored procedure '{dataRow.Field<string>("ROUTINE_NAME")}' in the new branch, because the user does not have permissions to view the routine definition.");
                        continue;
                    }

                    // Set the names and collation from the original and replace the definer with the current user, so that the stored procedure can be created by the current user.
                    query = $"SET NAMES {subDataTable.Rows[0].Field<string>(3)} COLLATE {subDataTable.Rows[0].Field<string>(4)}; {query.Replace($" DEFINER=`{definerParts[0]}`@`{definerParts[1]}`", " DEFINER=CURRENT_USER")}";
                    await branchDatabaseConnection.ExecuteAsync(query);
                }
                catch (Exception exception)
                {
                    errors.Add($"Unable to create stored procedure '{dataRow.Field<string>("ROUTINE_NAME")}' in the new branch, because of the following error: {exception}");
                }
            }
        }
        catch (Exception exception)
        {
            errors.Add(exception.ToString());

            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Failed to create the branch '{settings.DatabaseName}'. Error: {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

            // Save the error in the queue and set the finished on datetime to now.
            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);

            // Drop the new database it something went wrong, so that we can start over again later.
            // We can safely do this, because this method will return an error if the database already exists,
            // so we can be sure that this database was created here, and we can drop it again it something went wrong.
            try
            {
                if (await branchDatabaseHelpersService.DatabaseExistsAsync(branchDatabase))
                {
                    await branchDatabaseHelpersService.DropDatabaseAsync(branchDatabase);
                }
            }
            catch (Exception innerException)
            {
                errors.Add(innerException.ToString());
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Failed to drop new branch database '{settings.DatabaseName}', after getting an error while trying to fill it with data. Error: {innerException}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
            }
        }

        // Set the finish time to the current datetime, so that we can see how long it took.
        await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);
        result.Add("ErrorMessage", errors.FirstOrDefault() ?? "");
        result.Add("Success", !errors.Any());
        return result;
    }

    /// <summary>
    /// Handles the two different ways that we have to bulk insert data into a table of a branch, depending on the settings of the branch queue.
    /// </summary>
    /// <param name="branchQueue">The branch queue settings model.</param>
    /// <param name="configurationServiceName">The name of the current WTS configuration.</param>
    /// <param name="bulkCopy">The <see cref="MySqlBulkCopy"/> class that should be created outside of this function.</param>
    /// <param name="items">The <see cref="DataTable"/> with the data that should be inserted.</param>
    /// <param name="tableName">The name of the table to insert the data into.</param>
    /// <param name="branchDatabase">The name of the database of the branch.</param>
    /// <param name="branchDatabaseConnection">The <see cref="IDatabaseConnection"/> with the connection to the branch database.</param>
    /// <returns>The amount of inserted rows.</returns>
    private async Task<int> BulkInsertDataTableAsync(BranchQueueModel branchQueue, string configurationServiceName, MySqlBulkCopy bulkCopy, DataTable items, string tableName, string branchDatabase, IDatabaseConnection branchDatabaseConnection)
    {
        if (!branchQueue.UseMySqlBulkCopyWhenCreatingBranches)
        {
            return await branchDatabaseConnection.BulkInsertAsync(items, tableName, true, true);
        }

        bulkCopy.ColumnMappings.AddRange(MySqlHelpers.GetMySqlColumnMappingForBulkCopy(items));
        var bulkCopyResult = await bulkCopy.WriteToServerAsync(items);
        if (!bulkCopyResult.Warnings.Any())
        {
            return bulkCopyResult.RowsInserted;
        }

        var warnings = bulkCopyResult.Warnings.Select(warning => warning.Message);
        await logService.LogWarning(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Bulk copy of table '{tableName}' to branch database '{branchDatabase}' resulted in warnings: {String.Join(", ", warnings)}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

        return bulkCopyResult.RowsInserted;
    }

    /// <summary>
    /// Add the initial ID mappings for a table. These will be the same ID in both databases and is used to determine if an item is already mapped.
    /// </summary>
    /// <param name="branchDatabaseConnection">The database connection of the branch to use.</param>
    /// <param name="tableName">The name of the table to map the initial IDs for.</param>
    private static async Task AddInitialIdMappingAsync(IDatabaseConnection branchDatabaseConnection, string tableName)
    {
        // If the current table is of wiser_item then insert a record for the root folder to match for parent IDs.
        if (tableName.EndsWith("wiser_item"))
        {
            await branchDatabaseConnection.ExecuteAsync($"""
                                                             INSERT IGNORE INTO {WiserTableNames.WiserIdMappings} (table_name, our_id, production_id)
                                                             SELECT '{tableName}', 0, 0
                                                         """);
        }

        // Add all IDs that have been copied from the production database to the branch database to indicate that they are already mapped in the ID mappings.
        await branchDatabaseConnection.ExecuteAsync($"""
                                                         INSERT INTO {WiserTableNames.WiserIdMappings} (table_name, our_id, production_id)
                                                         SELECT '{tableName}', id, id
                                                         FROM {tableName}
                                                     """);
    }

    /// <summary>
    /// Handles the merging of changes from a branch back into the main/original branch.
    /// This will only merge the changes that the user requested to be merged.
    /// </summary>
    /// <param name="dataRowWithSettings">The <see cref="DataRow"/> from wiser_branch_queue.</param>
    /// <param name="branchQueue">The <see cref="BranchQueueModel"/> with the settings from the XML configuration.</param>
    /// <param name="configurationServiceName">The name of the configuration.</param>
    /// <param name="databaseConnection">The <see cref="IDatabaseConnection"/> with the connection to the database.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> for checking if a table exists, creating new tables etc.</param>
    /// <param name="wiserItemsService">The <see cref="IWiserItemsService"/> for getting settings of entity types and for (un)deleting items.</param>
    /// <param name="taskAlertsService">The <see cref="ITaskAlertsService"/> for sending notification to the user.</param>
    /// <returns>An <see cref="JObject"/> with properties "SuccessfulChanges" and "Errors".</returns>
    /// <exception cref="ArgumentOutOfRangeException">Then we get unknown options in enums.</exception>
    private async Task<JObject> HandleMergeBranchActionAsync(DataRow dataRowWithSettings, BranchQueueModel branchQueue, string configurationServiceName, IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, IWiserItemsService wiserItemsService, ITaskAlertsService taskAlertsService)
    {
        var successfulChanges = 0;
        var errors = new JArray();
        var result = new JObject
        {
            {"SuccessfulChanges", 0},
            {"Errors", errors},
            {"Success", false}
        };

        // Make sure the queue table is up-to-date.
        await databaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WiserBranchesQueue]);

        // Set the start date to the current datetime.
        var startDate = DateTime.Now;
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        var queueId = dataRowWithSettings.Field<int>("id");
        var queueName = dataRowWithSettings.Field<string>("name");
        var branchId = dataRowWithSettings.Field<int>("branch_id");
        databaseConnection.AddParameter("queueId", queueId);
        databaseConnection.AddParameter("now", startDate);
        await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET started_on = ?now WHERE id = ?queueId");

        // Validate the settings.
        var settings = JsonConvert.DeserializeObject<MergeBranchSettingsModel>(dataRowWithSettings.Field<string>("data") ?? "{}");
        if (settings is not {Id: > 0} || String.IsNullOrWhiteSpace(settings.DatabaseName))
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to merge a branch, but it either had invalid settings, or the branch ID was empty, or the database name was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
            errors.Add($"Trying to merge a branch, but it either had invalid settings, the branch ID was empty, or the database name was empty. Queue ID was: {queueId}");

            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.MergedBranchTemplateId, MergeBranchSubject, MergeBranchTemplate);
            return result;
        }

        // Get all merge settings that we'll need later.
        var entityMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.Entity);
        var entityPropertyMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.EntityProperty);
        var linkMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.Link);
        var moduleMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.Module);
        var permissionMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.Permission);
        var queryMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.Query);
        var roleMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.Role);
        var apiConnectionMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.ApiConnection);
        var dataSelectorMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.DataSelector);
        var fieldTemplatesMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.FieldTemplates);
        var userRoleMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.UserRole);
        var styledOutputMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.StyledOutput);
        var objectMergeSettings = settings.Settings.SingleOrDefault(s => s.Type == WiserSettingTypes.EasyObjects);

        // Store database names in variables for later use and create connection string for the branch database.
        var productionConnectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);
        var originalDatabase = productionConnectionStringBuilder.Database;
        var branchDatabase = settings.DatabaseName;
        var branchConnectionStringBuilder = GetConnectionStringBuilderForBranch(settings, branchDatabase);

        // Create and open connections to both databases and start transactions.
        var productionConnection = new MySqlConnection(connectionString);
        var branchConnection = new MySqlConnection(branchConnectionStringBuilder.ConnectionString);
        await productionConnection.OpenAsync();
        await branchConnection.OpenAsync();
        var productionTransaction = await productionConnection.BeginTransactionAsync();
        var branchTransaction = await branchConnection.BeginTransactionAsync();

        // We have our own dictionary with SQL parameters, so that we can reuse them easier and add them easily all at once to every command we create.
        var sqlParameters = new Dictionary<string, object>();

        try
        {
            // Create a new scope for dependency injection and get a new instance of the database connection, to use for connecting to the branch database.
            using var branchScope = serviceProvider.CreateScope();
            var branchDatabaseConnection = branchScope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
            await branchDatabaseConnection.ChangeConnectionStringsAsync(branchConnectionStringBuilder.ConnectionString, branchConnectionStringBuilder.ConnectionString);
            var branchDatabaseHelpersService = branchScope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
            var branchLinkTypesService = branchScope.ServiceProvider.GetRequiredService<ILinkTypesService>();
            var branchEntityTypesService = branchScope.ServiceProvider.GetRequiredService<IEntityTypesService>();

            // Create the wiser_id_mappings table, in the selected branch, if it doesn't exist yet.
            // We need it to map IDs of the selected environment to IDs of the production environment, because they are not always the same.
            await branchDatabaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WiserIdMappings, WiserTableNames.WiserBranchMergeLog]);

            // Empty the log table before we start, we only want to see the logs of the last merge, otherwise the table will become way too large.
            await branchDatabaseConnection.ExecuteAsync($"TRUNCATE TABLE {WiserTableNames.WiserBranchMergeLog}");

            // Get all history since last synchronisation.
            var dataTable = new DataTable();
            await using (var environmentCommand = branchConnection.CreateCommand())
            {
                environmentCommand.CommandText = $"SELECT * FROM `{WiserTableNames.WiserHistory}` ORDER BY id ASC";
                using var environmentAdapter = new MySqlDataAdapter(environmentCommand);
                environmentAdapter.Fill(dataTable);
            }

            var totalItemsInHistory = dataTable.Rows.Count;
            databaseConnection.AddParameter("totalItems", totalItemsInHistory);
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET total_items = ?totalItems, items_processed = 0 WHERE id = ?queueId");

            // Set saveHistory and username parameters for all queries.
            const string queryPrefix = "SET @saveHistory = TRUE; SET @_username = ?username; ";
            var username = $"{dataRowWithSettings.Field<string>("added_by")} (Sync from {branchDatabase})";
            if (username.Length > 50)
            {
                username = dataRowWithSettings.Field<string>("added_by");
            }

            sqlParameters.Add("username", username);

            // Fetch Link settings before we lock the tables.
            var allLinkTypeSettings = await branchLinkTypesService.GetAllLinkTypeSettingsAsync();

            // Make a list of objects that have been created and deleted in this branch, so that can just skip them when we're synchronising to keep the history of the production clean.
            var objectsCreatedInBranch = new List<ObjectCreatedInBranchModel>();

            // We need to lock all tables we're going to use, to make sure no other changes can be done while we're busy synchronising.
            var tablesToLock = new List<string> {WiserTableNames.WiserHistory};
            foreach (DataRow dataRow in dataTable.Rows)
            {
                var tableName = dataRow.Field<string>("tablename");
                if (String.IsNullOrWhiteSpace(tableName))
                {
                    continue;
                }

                tablesToLock.Add(tableName);
                if (WiserTableNames.TablesWithArchive.Any(table => tableName.EndsWith(table, StringComparison.OrdinalIgnoreCase)))
                {
                    tablesToLock.Add($"{tableName}{WiserTableNames.ArchiveSuffix}");
                }

                // If we have a table that has an ID from wiser_item, then always lock wiser_item as well, because we will read from it later.
                var originalItemId = Convert.ToUInt64(dataRow["item_id"]);
                var (tablePrefix, isWiserItemChange) = BranchesHelpers.GetTablePrefix(tableName, originalItemId);
                var wiserItemTableName = $"{tablePrefix}{WiserTableNames.WiserItem}";

                // Only lock the wiser_item table if the table prefix is not a link type.
                // We have to check this because wiser_itemfile could have a prefix for an entity type, or for a link type.
                // We want to lock the wiser_item table only if the prefix is not a link type.
                if (isWiserItemChange && originalItemId > 0 && !tablesToLock.Contains(wiserItemTableName) && !String.IsNullOrWhiteSpace(tablePrefix) && !allLinkTypeSettings.Any(t => t.UseDedicatedTable && tablePrefix.TrimEnd('_') == t.Type.ToString()))
                {
                    tablesToLock.Add(wiserItemTableName);
                    tablesToLock.Add($"{wiserItemTableName}{WiserTableNames.ArchiveSuffix}");
                }

                var action = dataRow.Field<string>("action").ToUpperInvariant();
                var field = dataRow.Field<string>("field");
                var objectId = originalItemId.ToString();
                switch (action)
                {
                    case "ADD_LINK":
                    {
                        // With ADD_LINK actions, the ID of the link itself isn't saved in wiser_history, so we need to use the concat the destination item ID (which is saved in "item_id"),
                        // the source item ID (which is saved in "newvalue") and the link type (which is saved in "field", together with the ordering) to get a unique ID for the link.
                        var linkType = field?.Split(",").FirstOrDefault() ?? "0";
                        objectId = $"{originalItemId}_{dataRow["newvalue"]}_{linkType}";
                        break;
                    }
                    case "REMOVE_LINK":
                    {
                        // With REMOVE_LINK actions, the ID of the link itself isn't saved in wiser_history, so we need to use the concat the destination item ID (which is saved in "item_id"),
                        // the source item ID (which is saved in "oldvalue") and the link type (which is saved in "field") to get a unique ID for the link.
                        var linkType = field ?? "0";
                        objectId = $"{originalItemId}_{dataRow["oldvalue"]}_{linkType}";
                        break;
                    }
                    case "INSERT_PERMISSION":
                    {
                        // With INSERT_PERMISSION actions, we need to look up the highest role ID to create a temporary unique ID for the role.
                        // So we need to lock the wiser_roles table, otherwise we can't query it later.
                        tablesToLock.Add(WiserTableNames.WiserRoles);
                        break;
                    }
                }

                BranchesHelpers.TrackObjectAction(objectsCreatedInBranch, action, objectId, tableName);
            }

            tablesToLock = tablesToLock.Distinct().ToList();

            // Add tables from wiser_id_mappings to tables to lock.
            await using (var command = branchConnection.CreateCommand())
            {
                command.CommandText = $"SELECT DISTINCT table_name FROM `{WiserTableNames.WiserIdMappings}`";
                var mappingDataTable = new DataTable();
                using var adapter = new MySqlDataAdapter(command);
                adapter.Fill(mappingDataTable);
                foreach (DataRow dataRow in mappingDataTable.Rows)
                {
                    var tableName = dataRow.Field<string>("table_name");
                    if (String.IsNullOrWhiteSpace(tableName) || tablesToLock.Contains(tableName))
                    {
                        continue;
                    }

                    tablesToLock.Add(tableName);

                    if (WiserTableNames.TablesWithArchive.Any(table => tableName.EndsWith(table, StringComparison.OrdinalIgnoreCase)))
                    {
                        tablesToLock.Add($"{tableName}{WiserTableNames.ArchiveSuffix}");
                    }
                }
            }

            // Lock the tables we're going to use, to be sure that other processes don't mess up our synchronisation.
            await LockTablesAsync(productionConnection, tablesToLock, false);
            await LockTablesAsync(branchConnection, tablesToLock, true);

            // This is to cache some information about items, like the entity type and whether the item has been deleted.
            // By using this, we don't have to look up the entity type of an item multiple times, if an item has multiple changes in the history.
            // The key is the table prefix and the value is a list with the items that we already looked up.
            var itemsCache = new Dictionary<string, List<BranchMergeItemCacheModel>>();
            var filesCache = new Dictionary<string, List<BranchMergeFileCacheModel>>();
            var linksCache = new List<BranchMergeLinkCacheModel>();

            // This is to map one item ID to another. This is needed because when someone creates a new item in the other environment, that ID could already exist in the production environment.
            // So we need to map the ID that is saved in wiser_history to the new ID of the item that we create in the production environment.
            var idMapping = new Dictionary<string, Dictionary<ulong, ulong>>(StringComparer.OrdinalIgnoreCase);
            var idMappingsAddedInCurrentMerge = new Dictionary<string, Dictionary<ulong, ulong>>(StringComparer.OrdinalIgnoreCase);
            await using (var environmentCommand = branchConnection.CreateCommand())
            {
                environmentCommand.CommandText = $"SELECT table_name, our_id, production_id FROM `{WiserTableNames.WiserIdMappings}`";
                using var environmentAdapter = new MySqlDataAdapter(environmentCommand);

                var idMappingDatatable = new DataTable();
                environmentAdapter.Fill(idMappingDatatable);
                foreach (DataRow dataRow in idMappingDatatable.Rows)
                {
                    var tableName = dataRow.Field<string>("table_name");
                    var ourId = dataRow.Field<ulong>("our_id");
                    var productionId = dataRow.Field<ulong>("production_id");

                    if (!idMapping.TryGetValue(tableName!, out var idMappingsForTable))
                    {
                        idMappingsForTable = new Dictionary<ulong, ulong>();
                        idMapping.Add(tableName, idMappingsForTable);
                    }

                    idMappingsForTable[ourId] = productionId;
                }

                // Get all IDs from the mappings that don't exist anymore in production, so that we can remove them.
                var idMappingsToRemove = new Dictionary<string, List<ulong>>(StringComparer.OrdinalIgnoreCase);
                foreach (var (table, tableIdMappings) in idMapping)
                {
                    // Get the list of all values from the current table, which are the IDs from production.
                    var productionIds = tableIdMappings.Values.Distinct().ToList();

                    // Find all IDs that don't exist anymore in production.
                    var query = $"SELECT id FROM {table} WHERE id IN ({String.Join(", ", productionIds)})";
                    await using var productionCommand = productionConnection.CreateCommand();
                    productionCommand.CommandText = query;
                    using var productionAdapter = new MySqlDataAdapter(productionCommand);
                    var productionIdsDatatable = new DataTable();
                    productionAdapter.Fill(productionIdsDatatable);

                    // Get the list of IDs that do exist in the production database.
                    var productionIdsInTable = productionIdsDatatable.Rows.Cast<DataRow>().Select(x => Convert.ToUInt64(x["id"])).ToList();

                    // Compare both lists, to find out which IDs to remove from the mappings.
                    var valuesToRemove = productionIds.Where(id => !productionIdsInTable.Contains(id)).ToList();
                    var keysToRemove = tableIdMappings.Where(mapping => valuesToRemove.Contains(mapping.Value)).Select(mapping => mapping.Key).ToList();
                    idMappingsToRemove.Add(table, keysToRemove);
                }

                // Remove all ID mappings that don't exist anymore in production.
                foreach (var (table, ids) in idMappingsToRemove)
                {
                    await RemoveIdMappingsAsync(idMapping, table, ids, branchConnection);
                }
            }

            // Cache some settings that we'll need later.
            var allEntityTypeSettings = new Dictionary<string, EntitySettingsModel>();

            // Start synchronising all history items one by one.
            var historyItemsSynchronised = new List<ulong>();
            var itemsProcessed = 0;
            foreach (DataRow dataRow in dataTable.Rows)
            {
                var historyId = Convert.ToUInt64(dataRow["id"]);
                var action = dataRow.Field<string>("action").ToUpperInvariant();
                var tableName = dataRow.Field<string>("tablename") ?? "";
                var fieldName = dataRow.Field<string>("field");

                // Create the object that will be used to store values we will need for the merge and also to log the merge action.
                var actionData = new BranchMergeLogModel(queueId, queueName, branchId, historyId, tableName, action, fieldName, productionConnectionStringBuilder, branchConnectionStringBuilder);

                var conflict = settings.ConflictSettings.SingleOrDefault(setting => setting.Id == historyId);
                actionData.UsedConflictSettings = conflict;

                // If this history item has a conflict, and it's not accepted, skip and delete this history record.
                if (conflict is {AcceptChange: not null} && !conflict.AcceptChange.Value)
                {
                    historyItemsSynchronised.Add(historyId);
                    itemsProcessed++;
                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                    actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                    actionData.MessageBuilder.AppendLine("When the user setup this merge, we found a conflict for this item and the user said to skip it, to keep the value from production. Therefor we skipped this action and deleted the row from wiser_history.");
                    branchBatchLoggerService.LogMergeAction(actionData);

                    continue;
                }

                // For ID mappings, if we have a row for wiser_itemdetail or wiser_itemlink, we want to check the ID of the item itself, not the ID of the detail or link.
                // So we need to use the wiser_item table for that, not the wiser_itemdetail or wiser_itemlink table.
                actionData.ObjectIdOriginal = Convert.ToUInt64(dataRow["item_id"]);
                actionData.ObjectIdMapped = actionData.ObjectIdOriginal;
                actionData.OldValue = dataRow.Field<string>("oldvalue");
                actionData.NewValue = dataRow.Field<string>("newvalue");
                actionData.ItemDetailLanguageCode = dataRow.Field<string>("language_code") ?? "";
                actionData.ItemDetailGroupName = dataRow.Field<string>("groupname") ?? "";

                var targetId = dataTable.Columns.Contains("target_id") ? dataRow.Field<ulong>("target_id") : 0;

                // Variables for item link changes.
                ulong? oldItemId = null;
                ulong? oldDestinationItemId = null;
                string columnNameForFileLink = null;
                (string SourceType, string SourceTablePrefix, string DestinationType, string DestinationTablePrefix)? linkData = null;
                LinkTypeMergeSettingsModel linkTypeSettings = null;

                var idForComparison = actionData.ItemIdOriginal.ToString();
                switch (action)
                {
                    case "ADD_LINK":
                    {
                        // With ADD_LINK actions, the ID of the link itself isn't saved in wiser_history, so we need to use the concat the destination item ID (which is saved in "item_id"),
                        // the source item ID (which is saved in "newvalue") and the link type (which is saved in "field", together with the ordering) to get a unique ID for the link.
                        var type = actionData.Field?.Split(",").FirstOrDefault() ?? "0";
                        idForComparison = $"{actionData.ObjectIdOriginal}_{actionData.NewValue}_{type}";
                        actionData.MessageBuilder.AppendLine($"Generated the following value for later checks if the link was created and deleted in the branch: {idForComparison}");
                        break;
                    }
                    case "REMOVE_LINK":
                    {
                        // With REMOVE_LINK actions, the ID of the link itself isn't saved in wiser_history, so we need to use the concat the destination item ID (which is saved in "item_id"),
                        // the source item ID (which is saved in "oldvalue") and the link type (which is saved in "field") to get a unique ID for the link.
                        var type = actionData.Field ?? "0";
                        idForComparison = $"{actionData.ObjectIdOriginal}_{actionData.OldValue}_{type}";
                        actionData.MessageBuilder.AppendLine($"Generated the following value for later checks if the link was created and deleted in the branch: {idForComparison}");
                        break;
                    }
                }

                // Treat details as items during the check for deleted items.
                var tableNameForDeletedItemCheck = tableName.Replace(WiserTableNames.WiserItemDetail, WiserTableNames.WiserItem);

                var objectCreatedInBranch = objectsCreatedInBranch.FirstOrDefault(i => i.ObjectId == idForComparison && String.Equals(i.TableName, tableNameForDeletedItemCheck, StringComparison.OrdinalIgnoreCase));
                if (objectCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false})
                {
                    // This item was created and then deleted in the branch, so we don't need to do anything.
                    historyItemsSynchronised.Add(historyId);
                    itemsProcessed++;
                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                    actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                    actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because the object was both created and deleted in the branch, which means that there is no point in merging that.");
                    branchBatchLoggerService.LogMergeAction(actionData);
                    continue;
                }

                var (tablePrefix, _) = BranchesHelpers.GetTablePrefix(tableName, actionData.ObjectIdOriginal);
                actionData.MessageBuilder.AppendLine($"Table prefix for original object is: '{tablePrefix}'");
                if (!itemsCache.TryGetValue(tablePrefix, out var listOfItems))
                {
                    listOfItems = [];
                    itemsCache.Add(tablePrefix, listOfItems);
                    actionData.MessageBuilder.AppendLine("This was a new table prefix, so we added it to the items cache dictionary.");
                }

                try
                {
                    // Make sure we have the correct item ID. For some actions the item id is saved in a different column.
                    BranchMergeLinkCacheModel linkCacheData;
                    switch (action)
                    {
                        case "REMOVE_LINK":
                        {
                            // In the REMOVE_LINK action, the destination item ID is saved in the item_id column of wiser_history.
                            actionData.LinkDestinationItemIdOriginal = actionData.ObjectIdOriginal;
                            actionData.LinkDestinationItemIdMapped = actionData.ObjectIdMapped;

                            // In the REMOVE_LINK action, the source item ID is saved in the old value column of wiser_history.
                            actionData.ItemIdOriginal = Convert.ToUInt64(actionData.OldValue);
                            actionData.ItemIdMapped = actionData.ItemIdOriginal;
                            actionData.OldValue = null;

                            // In the REMOVE_LINK action, the link type is saved in the field column of wiser_history.
                            actionData.LinkType = Int32.Parse(actionData.Field);
                            actionData.Field = String.Empty;

                            break;
                        }
                        case "UPDATE_ITEMLINKDETAIL":
                        case "CHANGE_LINK":
                        {
                            actionData.LinkIdOriginal = actionData.ObjectIdOriginal;
                            actionData.LinkIdMapped = actionData.ObjectIdMapped;

                            // When a link has been changed, it's possible that the ID of one of the items is changed.
                            // It's also possible that this is a new link that the production database didn't have yet (and so the ID of the link will most likely be different).
                            // Therefor we need to find the original item and destination IDs, so that we can use those to update the link in the production database.
                            linkCacheData = await GetLinkDataAsync(actionData.LinkIdOriginal, sqlParameters, tableName, branchConnection, linksCache, actionData);
                            if (linkCacheData.IsDeleted)
                            {
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because the link has been deleted, which means that there is no point in merging that.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (linkCacheData.Id > 0)
                            {
                                actionData.ItemIdOriginal = linkCacheData.ItemId ?? 0;
                                actionData.ItemIdMapped = linkCacheData.ItemId ?? 0;
                                actionData.LinkDestinationItemIdOriginal = linkCacheData.DestinationItemId ?? 0;
                                actionData.LinkDestinationItemIdMapped = linkCacheData.DestinationItemId ?? 0;
                                actionData.LinkType = linkCacheData.Type ?? 0;

                                switch (actionData.Field)
                                {
                                    case "destination_item_id":
                                        oldDestinationItemId = Convert.ToUInt64(actionData.OldValue);
                                        oldItemId = actionData.ItemIdOriginal;
                                        break;
                                    case "item_id":
                                        oldItemId = Convert.ToUInt64(actionData.OldValue);
                                        oldDestinationItemId = actionData.LinkDestinationItemIdOriginal;
                                        break;
                                    default:
                                        oldItemId = actionData.ItemIdOriginal;
                                        oldDestinationItemId = actionData.LinkDestinationItemIdOriginal;
                                        break;
                                }
                            }

                            break;
                        }
                        case "ADD_LINK":
                        {
                            actionData.LinkDestinationItemIdOriginal = actionData.ObjectIdOriginal;
                            actionData.LinkDestinationItemIdMapped = actionData.LinkDestinationItemIdOriginal;
                            actionData.ItemIdOriginal = Convert.ToUInt64(actionData.NewValue);
                            actionData.ItemIdMapped = actionData.ItemIdOriginal;
                            actionData.NewValue = null;

                            var split = actionData.Field.Split(',');
                            actionData.LinkType = Int32.Parse(split[0]);
                            actionData.LinkOrdering = split.Length > 1 ? Int32.Parse(split[1]) : 0;

                            break;
                        }
                        case "ADD_FILE":
                        case "DELETE_FILE":
                        {
                            actionData.ItemTableName = tableName.Replace(WiserTableNames.WiserItemFile, WiserTableNames.WiserItem);
                            actionData.FileIdOriginal = actionData.ObjectIdOriginal;
                            actionData.FileIdMapped = actionData.ObjectIdMapped;

                            // The ADD_FILE and DELETE_FILE use the old value column to indicate whether it's a file for a link or for an item. The new value column is the ID of the link or item.
                            actionData.ItemIdOriginal = String.Equals(actionData.OldValue, "item_id", StringComparison.OrdinalIgnoreCase) ? Convert.ToUInt64(actionData.NewValue) : 0;
                            actionData.LinkIdOriginal = String.Equals(actionData.OldValue, "itemlink_id", StringComparison.OrdinalIgnoreCase) ? Convert.ToUInt64(actionData.NewValue) : 0;
                            columnNameForFileLink = actionData.OldValue;
                            actionData.OldValue = null;
                            actionData.NewValue = null;

                            actionData.ItemIdMapped = actionData.ItemIdOriginal;
                            actionData.LinkIdMapped = actionData.LinkIdOriginal;

                            // If we have a table prefix, then we need to check if it's a valid link type.
                            if (Int32.TryParse(tablePrefix.TrimEnd('_'), out var linkTypeFromTablePrefix) && allLinkTypeSettings.Any(t => t.UseDedicatedTable && t.Type == linkTypeFromTablePrefix))
                            {
                                actionData.LinkType = linkTypeFromTablePrefix;
                            }

                            var fileData = await GetFileDataAsync(actionData.FileIdOriginal, sqlParameters, tableName, tablePrefix, branchConnection, filesCache, actionData);
                            if (fileData.IsDeleted)
                            {
                                // If the file is deleted, we can use the item id and link id from wiser_history.
                                fileData.ItemId = actionData.ItemIdOriginal;
                                fileData.LinkId = actionData.LinkIdOriginal;
                            }

                            // If the file is linked to a link, then we need to find the data of that link.
                            if (actionData.LinkIdOriginal > 0)
                            {
                                linkCacheData = await GetLinkDataAsync(actionData.LinkIdOriginal, sqlParameters, tableName, branchConnection, linksCache, actionData);
                                actionData.ItemIdOriginal = linkCacheData.ItemId ?? 0;
                                actionData.ItemIdMapped = linkCacheData.ItemId ?? 0;
                                actionData.LinkDestinationItemIdOriginal = linkCacheData.DestinationItemId ?? 0;
                                actionData.LinkDestinationItemIdMapped = linkCacheData.DestinationItemId ?? 0;
                                actionData.LinkType = linkCacheData.Type ?? 0;

                                // If the link itself has been deleted, then there's no point in merging the file.
                                if (linkCacheData.IsDeleted)
                                {
                                    historyItemsSynchronised.Add(historyId);
                                    itemsProcessed++;
                                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                    actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                    actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because the file is linked to an item link that is deleted in the branch, which means that there is no point in merging that.");
                                    branchBatchLoggerService.LogMergeAction(actionData);
                                    continue;
                                }
                            }

                            break;
                        }
                        case "UPDATE_FILE":
                        {
                            actionData.ItemTableName = tableName.Replace(WiserTableNames.WiserItemFile, WiserTableNames.WiserItem);
                            actionData.FileIdOriginal = actionData.ObjectIdOriginal;
                            actionData.FileIdMapped = actionData.ObjectIdMapped;

                            // If we have a table prefix, then we need to check if it's a valid link type.
                            if (Int32.TryParse(tablePrefix.TrimEnd('_'), out var linkTypeFromTablePrefix) && allLinkTypeSettings.Any(t => t.UseDedicatedTable && t.Type == linkTypeFromTablePrefix))
                            {
                                actionData.LinkType = linkTypeFromTablePrefix;
                            }

                            var fileData = await GetFileDataAsync(actionData.FileIdOriginal, sqlParameters, tableName, tablePrefix, branchConnection, filesCache, actionData);
                            actionData.ItemIdOriginal = fileData.ItemId ?? 0;
                            actionData.ItemIdMapped = fileData.ItemId ?? 0;
                            actionData.LinkIdOriginal = fileData.LinkId ?? 0;
                            actionData.LinkIdMapped = fileData.LinkId ?? 0;

                            if (fileData.IsDeleted)
                            {
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because the file has been deleted in the branch, which means that there is no point in merging any updates to this file.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            // If the file is linked to a link, then we need to find the data of that link.
                            if (actionData.LinkIdOriginal > 0)
                            {
                                linkCacheData = await GetLinkDataAsync(actionData.LinkIdOriginal, sqlParameters, tableName, branchConnection, linksCache, actionData);
                                actionData.ItemIdOriginal = linkCacheData.ItemId ?? 0;
                                actionData.ItemIdMapped = linkCacheData.ItemId ?? 0;
                                actionData.LinkDestinationItemIdOriginal = linkCacheData.DestinationItemId ?? 0;
                                actionData.LinkDestinationItemIdMapped = linkCacheData.DestinationItemId ?? 0;
                                actionData.LinkType = linkCacheData.Type ?? 0;

                                if (linkCacheData.IsDeleted)
                                {
                                    historyItemsSynchronised.Add(historyId);
                                    itemsProcessed++;
                                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                    actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                    actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because the file is linked to an item link that has been deleted in the branch, which means that there is no point in merging that.");
                                    branchBatchLoggerService.LogMergeAction(actionData);
                                    continue;
                                }
                            }

                            break;
                        }
                        case "DELETE_ITEM":
                        case "UNDELETE_ITEM":
                        {
                            actionData.ItemIdOriginal = actionData.ObjectIdOriginal;
                            actionData.ItemIdMapped = actionData.ObjectIdMapped;
                            actionData.ItemEntityType = actionData.Field;
                            actionData.Field = String.Empty;
                            break;
                        }
                        case "UPDATE_ITEM_DETAIL":
                        case "UPDATE_ITEM" when tableName.EndsWith(WiserTableNames.WiserItemDetail, StringComparison.OrdinalIgnoreCase):
                        {
                            actionData.ItemIdOriginal = actionData.ObjectIdOriginal;
                            actionData.ItemIdMapped = actionData.ObjectIdMapped;
                            actionData.ItemTableName = tableName.Replace(WiserTableNames.WiserItemDetail, WiserTableNames.WiserItem);
                            actionData.ItemDetailIdOriginal = targetId;
                            actionData.ItemDetailIdMapped = targetId;
                            break;
                        }
                        case "UPDATE_ITEM":
                        case "CREATE_ITEM":
                        {
                            actionData.ItemIdOriginal = actionData.ObjectIdOriginal;
                            actionData.ItemIdMapped = actionData.ObjectIdMapped;
                            actionData.ItemTableName = tableName;
                            break;
                        }
                    }

                    // Get information we need for Wiser item links.
                    if (action is "ADD_LINK" or "CHANGE_LINK" or "REMOVE_LINK" or "UPDATE_ITEMLINKDETAIL" || (action is "ADD_FILE" or "UPDATE_FILE" or "DELETE_FILE" && actionData.LinkIdOriginal > 0 && actionData.LinkType > 0))
                    {
                        // Unlock the tables temporarily so that we can call GetEntityTypesOfLinkAsync, which calls wiserItemsService.GetTablePrefixForEntityAsync, since that method doesn't use our custom database connection.
                        await using var productionCommand = productionConnection.CreateCommand();
                        productionCommand.CommandText = "UNLOCK TABLES";
                        await productionCommand.ExecuteNonQueryAsync();

                        await using var branchCommand = branchConnection.CreateCommand();
                        branchCommand.CommandText = "UNLOCK TABLES";
                        await branchCommand.ExecuteNonQueryAsync();

                        linkData = await GetEntityTypesOfLinkAsync(actionData.ItemIdOriginal, actionData.LinkDestinationItemIdOriginal, actionData.LinkType, branchConnection, branchEntityTypesService, allLinkTypeSettings, allEntityTypeSettings, actionData);
                        // If we couldn't find any link data, then most likely one of the items doesn't exist anymore, so skip this history record.
                        // The other reason could be that the link type is not configured (correctly), in that case we also can't do anything, so skip it as well.
                        if (!linkData.HasValue)
                        {
                            // TODO: If linkData is null, look if the source and destinaition items exist in the default wiser_item or wiser_item_archive tables.
                            // TODO: If we do find something, then get the entity type from those items and use those to see if we can find the link type that way from "settings.LinkTypes" and store that into "linkTypeSettings".
                            // TODO: If linkTypeSettings is still null after that, then we know that this is a link type that hasn't been added to the settings yet.
                            // TODO: In that case, we should log a warning and then check if either the source entity or the destination entity has been selected to be merged in "settings.Entities".
                            // TODO: If that is the case, then merge the link change as well, otherwise skip it.
                            // TODO: The GCL has a similar fallback mechanism, this way we can still merge those unspecified link types.
                            historyItemsSynchronised.Add(historyId);
                            itemsProcessed++;
                            await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                            actionData.Status = ObjectActionMergeStatuses.Skipped;
                            actionData.MessageBuilder.AppendLine("The current row was skipped, because this is a link change (or something related to a link) and we couldn't find any information about this link type. This most likely means that one of the linked items doesn't exist anymore, or that the link type is not configured (correctly).");
                            branchBatchLoggerService.LogMergeAction(actionData);

                            // Lock the tables again when we're done.
                            await LockTablesAsync(productionConnection, tablesToLock, false);
                            await LockTablesAsync(branchConnection, tablesToLock, true);
                            continue;
                        }

                        linkTypeSettings = settings.LinkTypes.SingleOrDefault(s => s.Type == actionData.LinkType && String.Equals(s.SourceEntityType, linkData.Value.SourceType) && String.Equals(s.DestinationEntityType, linkData.Value.DestinationType));

                        actionData.ItemTableName = $"{linkData.Value.SourceTablePrefix}{WiserTableNames.WiserItem}";
                        actionData.LinkDestinationItemTableName = $"{linkData.Value.DestinationTablePrefix}{WiserTableNames.WiserItem}";

                        actionData.ItemEntityType = linkData.Value.SourceType;
                        actionData.LinkDestinationItemEntityType = linkData.Value.DestinationType;

                        if (!tablesToLock.Contains($"{linkData.Value.SourceTablePrefix}{WiserTableNames.WiserItem}"))
                        {
                            tablesToLock.Add($"{linkData.Value.SourceTablePrefix}{WiserTableNames.WiserItem}");
                            tablesToLock.Add($"{linkData.Value.SourceTablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix}");
                        }

                        if (!tablesToLock.Contains($"{linkData.Value.DestinationTablePrefix}{WiserTableNames.WiserItem}"))
                        {
                            tablesToLock.Add($"{linkData.Value.DestinationTablePrefix}{WiserTableNames.WiserItem}");
                            tablesToLock.Add($"{linkData.Value.DestinationTablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix}");
                        }

                        // Lock the tables again when we're done.
                        await LockTablesAsync(productionConnection, tablesToLock, false);
                        await LockTablesAsync(branchConnection, tablesToLock, true);
                    }

                    // Get mapped IDs for everything.
                    actionData.ItemIdMapped = GetMappedId(actionData.ItemTableName, idMapping, actionData.ItemIdOriginal, actionData).Value;
                    actionData.LinkDestinationItemIdMapped = GetMappedId(actionData.LinkDestinationItemTableName, idMapping, actionData.LinkDestinationItemIdOriginal, actionData).Value;
                    actionData.LinkIdMapped = GetMappedId(tableName, idMapping, actionData.LinkIdOriginal, actionData).Value;
                    actionData.FileIdMapped = GetMappedId(tableName, idMapping, actionData.FileIdOriginal, actionData).Value;
                    actionData.ObjectIdMapped = GetMappedId(tableName, idMapping, actionData.ObjectIdOriginal, actionData).Value;

                    oldItemId = GetMappedId(actionData.ItemTableName, idMapping, oldItemId, actionData);
                    oldDestinationItemId = GetMappedId(actionData.LinkDestinationItemTableName, idMapping, oldDestinationItemId, actionData);
                    var mappedTargetId = GetMappedId(tableName, idMapping, targetId, actionData, true);
                    targetId = mappedTargetId ?? targetId;

                    var linkSourceItemCreatedInBranch = objectsCreatedInBranch.FirstOrDefault(i => i.ObjectId == actionData.ItemIdOriginal.ToString() && String.Equals(i.TableName, $"{linkData?.SourceTablePrefix}{WiserTableNames.WiserItem}", StringComparison.OrdinalIgnoreCase));
                    var linkDestinationItemCreatedInBranch = objectsCreatedInBranch.FirstOrDefault(i => i.ObjectId == actionData.LinkDestinationItemIdOriginal.ToString() && String.Equals(i.TableName, $"{linkData?.DestinationTablePrefix}{WiserTableNames.WiserItem}", StringComparison.OrdinalIgnoreCase));
                    var linkSourceItemIsCreatedAndDeletedInBranch = linkSourceItemCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false};
                    var linkDestinationItemIsCreatedAndDeletedInBranch = linkDestinationItemCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false};

                    // Find and/or cache the entity type of the source item.
                    if (actionData.ItemIdOriginal > 0)
                    {
                        var itemData = await GetOrAddItemDataAsync(actionData.ItemIdOriginal, sqlParameters, tableName, tablePrefix, branchConnection, itemsCache, actionData, actionData.ItemEntityType);
                        actionData.ItemEntityType = itemData.EntityType;
                    }

                    // Find and/or cache the entity type of the destination item.
                    if (actionData.LinkDestinationItemIdOriginal > 0)
                    {
                        var itemData = await GetOrAddItemDataAsync(actionData.LinkDestinationItemIdOriginal, sqlParameters, tableName, tablePrefix, branchConnection, itemsCache, actionData, actionData.LinkDestinationItemEntityType);
                        actionData.LinkDestinationItemEntityType = itemData.EntityType;
                    }

                    var entityTypeMergeSettings = settings.Entities.SingleOrDefault(e => String.Equals(e.Type, actionData.ItemEntityType, StringComparison.OrdinalIgnoreCase)) ?? new EntityMergeSettingsModel();

                    // Update the item in the production environment.
                    switch (action)
                    {
                        case "CREATE_ITEM":
                        {
                            actionData.UsedMergeSettings = entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (entityTypeMergeSettings is not {Create: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the entity type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge create actions of items of type '{entityTypeMergeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            // Id mapping already exists for this item, which means the item already exists in production, so no need to create it again.
                            if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(actionData.ItemIdOriginal))
                            {
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because an ID mapping already exists for this item. This means that the current item was already created in production. This is most likely because Wiser created it in the branch and production at the same time, or someone added the ID mapping manually without deleting the corresponding row in wiser_history.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            var newItemId = await GenerateNewIdAsync(tableName, productionConnection, branchConnection, actionData);
                            sqlParameters["newId"] = newItemId;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             INSERT INTO `{tableName}` (id, entity_type) VALUES (?newId, '')
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                            await AddIdMappingAsync(idMapping, idMappingsAddedInCurrentMerge, tableName, actionData.ItemIdOriginal, newItemId, branchConnection, productionConnection, actionData);

                            break;
                        }
                        case "UPDATE_ITEM" when tableName.EndsWith(WiserTableNames.WiserItemDetail, StringComparison.OrdinalIgnoreCase):
                        {
                            actionData.ItemDetailIdMapped = targetId;
                            actionData.UsedMergeSettings = entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (entityTypeMergeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the entity type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of items of type '{entityTypeMergeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["itemId"] = actionData.ItemIdMapped;
                            sqlParameters["key"] = actionData.Field;
                            sqlParameters["languageCode"] = actionData.ItemDetailLanguageCode;
                            sqlParameters["groupName"] = actionData.ItemDetailGroupName;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = queryPrefix;
                            if (String.IsNullOrWhiteSpace(actionData.NewValue))
                            {
                                actionData.MessageBuilder.AppendLine($"The new value is empty, so we will delete the item detail row from '{tableName}'.");
                                productionCommand.CommandText += $"""
                                                                  DELETE FROM `{tableName}`
                                                                  WHERE item_id = ?itemId
                                                                  AND `key` = ?key
                                                                  AND language_code = ?languageCode
                                                                  AND groupname = ?groupName
                                                                  """;

                                await RemoveIdMappingAsync(idMapping, tableName, actionData.ItemDetailIdOriginal, branchConnection, actionData);
                            }
                            else
                            {
                                var useLongValue = actionData.NewValue.Length > 1000;
                                sqlParameters["value"] = useLongValue ? "" : actionData.NewValue;
                                sqlParameters["longValue"] = useLongValue ? actionData.NewValue : "";

                                ulong existingId;
                                if (mappedTargetId is > 0)
                                {
                                    // If we already found an ID in the mappings, just use that one.
                                    existingId = mappedTargetId.Value;
                                    actionData.MessageBuilder.AppendLine($"Found the current item detail in the mappings, so using that ID ('{existingId}') for updating the correct row in production.");
                                }
                                else
                                {
                                    // Check if this item detail already exists in production.
                                    productionCommand.CommandText = $"""
                                                                     SELECT id
                                                                     FROM `{tableName}`
                                                                     WHERE item_id = ?itemId
                                                                     AND `key` = ?key
                                                                     AND language_code = ?languageCode
                                                                     AND groupname = ?groupName
                                                                     """;
                                    existingId = Convert.ToUInt64(await productionCommand.ExecuteScalarAsync() ?? 0);
                                    actionData.MessageBuilder.AppendLine($"Did not find the current item detail in the mappings, but we found it in the table '{tableName}' in production, based on item_id, key, language_code and groupname. The ID for that item detail in production is '{existingId}'.");
                                }

                                if (existingId > 0)
                                {
                                    actionData.ItemDetailIdMapped = existingId;
                                    actionData.MessageBuilder.AppendLine("Updating the existing item detail in production, via the ID we found before.");
                                    sqlParameters["existingId"] = actionData.ItemDetailIdMapped;
                                    AddParametersToCommand(sqlParameters, productionCommand);
                                    productionCommand.CommandText = $"""
                                                                     UPDATE `{tableName}` SET value = ?value, long_value = ?longValue
                                                                     WHERE id = ?existingId
                                                                     """;

                                    // Map the item detail ID from wiser_history to the ID of the current item detail, locally and in database.
                                    if (actionData.ItemDetailIdOriginal > 0)
                                    {
                                        await AddIdMappingAsync(idMapping, idMappingsAddedInCurrentMerge, tableName, actionData.ItemDetailIdOriginal, actionData.ItemDetailIdMapped, branchConnection, productionConnection, actionData);
                                    }
                                }
                                else
                                {
                                    var newItemId = await GenerateNewIdAsync(tableName, productionConnection, branchConnection, actionData);
                                    sqlParameters["newId"] = newItemId;

                                    actionData.MessageBuilder.AppendLine($"Adding a new row in '{tableName}' in production, with ID '{newItemId}'.");
                                    AddParametersToCommand(sqlParameters, productionCommand);
                                    productionCommand.CommandText = $"""
                                                                     INSERT INTO `{tableName}` (id, language_code, item_id, groupname, `key`, value, long_value)
                                                                     VALUES (?newId, ?languageCode, ?itemId, ?groupName, ?key, ?value, ?longValue)
                                                                     """;

                                    // Map the item detail ID from wiser_history to the ID of the current item detail, locally and in database.
                                    if (actionData.ItemDetailIdOriginal > 0)
                                    {
                                        await AddIdMappingAsync(idMapping, idMappingsAddedInCurrentMerge, tableName, actionData.ItemDetailIdOriginal, newItemId, branchConnection, productionConnection, actionData);
                                    }
                                }
                            }

                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        case "UPDATE_ITEM" when tableName.EndsWith(WiserTableNames.WiserItem, StringComparison.OrdinalIgnoreCase):
                        {
                            actionData.UsedMergeSettings = entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (entityTypeMergeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the entity type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of items of type '{entityTypeMergeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (String.IsNullOrWhiteSpace(actionData.Field))
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Failed;
                                actionData.MessageBuilder.AppendLine($"For an {action} action, we need to know the field that was updated, but the field was not saved in `wiser_history` for some reason.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["itemId"] = actionData.ItemIdMapped;
                            sqlParameters["newValue"] = actionData.NewValue;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             UPDATE `{tableName}` 
                                                             SET `{actionData.Field.ToMySqlSafeValue(false)}` = ?newValue
                                                             WHERE id = ?itemId
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        case "UPDATE_ITEM_DETAIL":
                        {
                            actionData.ItemDetailIdMapped = targetId;
                            actionData.UsedMergeSettings = entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (entityTypeMergeSettings is not {Create: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the entity type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of items of type '{entityTypeMergeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (String.IsNullOrWhiteSpace(actionData.Field))
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Failed;
                                actionData.MessageBuilder.AppendLine($"For an {action} action, we need to know the field that was updated, but the field was not saved in `wiser_history` for some reason.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["itemId"] = actionData.ItemIdMapped;
                            sqlParameters["newValue"] = actionData.NewValue;
                            sqlParameters["detailId"] = actionData.ItemDetailIdMapped;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             UPDATE `{tableName}` SET `{actionData.Field.ToMySqlSafeValue(false)}` = ?newValue
                                                             WHERE id = ?detailId
                                                             """;

                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        case "DELETE_ITEM":
                        {
                            actionData.UsedMergeSettings = entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (entityTypeMergeSettings is not {Delete: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the entity type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge delete actions of items of type '{entityTypeMergeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            // Unlock the tables temporarily so that we can call wiserItemsService.DeleteAsync, since that method doesn't use our custom database connection.
                            await using var productionCommand = productionConnection.CreateCommand();
                            productionCommand.CommandText = "UNLOCK TABLES";
                            await productionCommand.ExecuteNonQueryAsync();
                            await wiserItemsService.DeleteAsync(actionData.ItemDetailIdMapped, entityType: actionData.ItemEntityType, skipPermissionsCheck: true, username: username);

                            // Lock the tables again when we're done with deleting.
                            await LockTablesAsync(productionConnection, tablesToLock, false);

                            break;
                        }
                        case "UNDELETE_ITEM":
                        {
                            actionData.UsedMergeSettings = entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (entityTypeMergeSettings is not {Delete: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the entity type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge delete actions of items of type '{entityTypeMergeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            // Unlock the tables temporarily so that we can call wiserItemsService.DeleteAsync, since that method doesn't use our custom database connection.
                            await using var productionCommand = productionConnection.CreateCommand();
                            productionCommand.CommandText = "UNLOCK TABLES";
                            await productionCommand.ExecuteNonQueryAsync();
                            await wiserItemsService.DeleteAsync(actionData.ItemIdMapped, entityType: actionData.ItemEntityType, skipPermissionsCheck: true, username: username, undelete: true);

                            // Lock the tables again when we're done with deleting.
                            await LockTablesAsync(productionConnection, tablesToLock, false);

                            break;
                        }
                        case "ADD_LINK":
                        {
                            actionData.UsedMergeSettings = linkTypeSettings;

                            // Check if the link type is in the list of changes.
                            if (linkTypeSettings is not {Create: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the link type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge create actions of links of type '{linkTypeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (linkSourceItemIsCreatedAndDeletedInBranch || linkDestinationItemIsCreatedAndDeletedInBranch)
                            {
                                // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                var text = linkSourceItemIsCreatedAndDeletedInBranch switch
                                {
                                    true when linkDestinationItemIsCreatedAndDeletedInBranch => "both the source and destination items of the link were created and then deleted in the branch",
                                    true => "the source item of the link was created and then deleted in the branch",
                                    _ => "the destination item of the link was created and then deleted in the branch"
                                };

                                actionData.MessageBuilder.AppendLine($"The current row was skipped and removed, because {text}, so we don't need to do anything..");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["itemId"] = actionData.ItemIdMapped;
                            sqlParameters["originalItemId"] = actionData.ItemIdOriginal;
                            sqlParameters["ordering"] = actionData.LinkOrdering;
                            sqlParameters["destinationItemId"] = actionData.LinkDestinationItemIdMapped;
                            sqlParameters["originalDestinationItemId"] = actionData.LinkDestinationItemIdOriginal;
                            sqlParameters["type"] = actionData.LinkType;

                            // Get the original link ID, so we can map it to the new one.
                            await using (var environmentCommand = branchConnection.CreateCommand())
                            {
                                AddParametersToCommand(sqlParameters, environmentCommand);
                                environmentCommand.CommandText = $"SELECT id FROM `{tableName}` WHERE item_id = ?originalItemId AND destination_item_id = ?originalDestinationItemId AND type = ?type";
                                var getLinkIdDataTable = new DataTable();
                                using var environmentAdapter = new MySqlDataAdapter(environmentCommand);
                                environmentAdapter.Fill(getLinkIdDataTable);
                                if (getLinkIdDataTable.Rows.Count == 0)
                                {
                                    await logService.LogWarning(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Could not find link ID with itemId = {actionData.ItemIdOriginal}, destinationItemId = {actionData.LinkDestinationItemIdOriginal} and type = {actionData.LinkType}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                    historyItemsSynchronised.Add(historyId);
                                    itemsProcessed++;
                                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                    actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                    actionData.MessageBuilder.AppendLine($"The current row was skipped and removed, because we were not able to find the link ID in the branch database based on source ID ('{actionData.ItemIdOriginal}'), destination ID ('{actionData.LinkDestinationItemIdOriginal}') and type ('{actionData.LinkType}'). This means that the link has been deleted in the branch and so there is no point in merging it.");
                                    branchBatchLoggerService.LogMergeAction(actionData);
                                    continue;
                                }

                                actionData.LinkIdOriginal = Convert.ToUInt64(getLinkIdDataTable.Rows[0]["id"]);
                                actionData.LinkIdMapped = await GenerateNewIdAsync(tableName, productionConnection, branchConnection, actionData);
                            }

                            if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(actionData.LinkIdOriginal))
                            {
                                // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because an ID mapping already exists for this link. This means that the current link was already created in production. Most likely that Wiser created it in the branch and production at the same time, or someone added the ID mapping manually without deleting the corresponding row in wiser_history.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["newId"] = actionData.LinkIdMapped;
                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             INSERT IGNORE INTO `{tableName}` (id, item_id, destination_item_id, ordering, type)
                                                             VALUES (?newId, ?itemId, ?destinationItemId, ?ordering, ?type);
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                            await AddIdMappingAsync(idMapping, idMappingsAddedInCurrentMerge, tableName, actionData.LinkIdOriginal, actionData.LinkIdMapped, branchConnection, productionConnection, actionData);

                            break;
                        }
                        case "CHANGE_LINK":
                        {
                            actionData.UsedMergeSettings = linkTypeSettings;

                            // Check if the link type is in the list of changes.
                            if (linkTypeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the link type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of links of type '{linkTypeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (linkSourceItemIsCreatedAndDeletedInBranch || linkDestinationItemIsCreatedAndDeletedInBranch)
                            {
                                // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                var text = linkSourceItemIsCreatedAndDeletedInBranch switch
                                {
                                    true when linkDestinationItemIsCreatedAndDeletedInBranch => "both the source and destination items of the link were created and then deleted in the branch",
                                    true => "the source item of the link was created and then deleted in the branch",
                                    _ => "the destination item of the link was created and then deleted in the branch"
                                };

                                actionData.MessageBuilder.AppendLine($"The current row was skipped and removed, because {text}, so we don't need to do anything..");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (String.IsNullOrWhiteSpace(actionData.Field))
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Failed;
                                actionData.MessageBuilder.AppendLine($"For an {action} action, we need to know the field that was updated, but the field was not saved in `wiser_history` for some reason.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["oldItemId"] = oldItemId;
                            sqlParameters["oldDestinationItemId"] = oldDestinationItemId;
                            sqlParameters["newValue"] = actionData.NewValue;
                            sqlParameters["type"] = actionData.LinkType;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             UPDATE `{tableName}` 
                                                             SET `{actionData.Field.ToMySqlSafeValue(false)}` = ?newValue
                                                             WHERE item_id = ?oldItemId
                                                             AND destination_item_id = ?oldDestinationItemId
                                                             AND type = ?type
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();
                            break;
                        }
                        case "REMOVE_LINK":
                        {
                            actionData.UsedMergeSettings = linkTypeSettings;

                            // Check if the link type is in the list of changes.
                            if (linkTypeSettings is not {Delete: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the link type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge delete actions of links of type '{linkTypeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (linkSourceItemIsCreatedAndDeletedInBranch || linkDestinationItemIsCreatedAndDeletedInBranch)
                            {
                                // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                var text = linkSourceItemIsCreatedAndDeletedInBranch switch
                                {
                                    true when linkDestinationItemIsCreatedAndDeletedInBranch => "both the source and destination items of the link were created and then deleted in the branch",
                                    true => "the source item of the link was created and then deleted in the branch",
                                    _ => "the destination item of the link was created and then deleted in the branch"
                                };

                                actionData.MessageBuilder.AppendLine($"The current row was skipped and removed, because {text}, so we don't need to do anything..");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["itemId"] = actionData.ItemIdMapped;
                            sqlParameters["destinationItemId"] = actionData.LinkDestinationItemIdMapped;
                            sqlParameters["type"] = actionData.LinkType;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             DELETE FROM `{tableName}`
                                                             WHERE item_id = ?itemId
                                                             AND destination_item_id = ?destinationItemId
                                                             AND type = ?type
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();
                            break;
                        }
                        case "UPDATE_ITEMLINKDETAIL":
                        {
                            actionData.UsedMergeSettings = linkTypeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (linkTypeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? "The current row was skipped, because we were not able to find the link type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of links of type '{linkTypeSettings.Type}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (linkSourceItemIsCreatedAndDeletedInBranch || linkDestinationItemIsCreatedAndDeletedInBranch)
                            {
                                // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                var text = linkSourceItemIsCreatedAndDeletedInBranch switch
                                {
                                    true when linkDestinationItemIsCreatedAndDeletedInBranch => "both the source and destination items of the link were created and then deleted in the branch",
                                    true => "the source item of the link was created and then deleted in the branch",
                                    _ => "the destination item of the link was created and then deleted in the branch"
                                };

                                actionData.MessageBuilder.AppendLine($"The current row was skipped and removed, because {text}, so we don't need to do anything..");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["linkId"] = actionData.LinkIdMapped;
                            sqlParameters["key"] = actionData.Field;
                            sqlParameters["languageCode"] = actionData.ItemDetailLanguageCode;
                            sqlParameters["groupName"] = actionData.ItemDetailGroupName;

                            // TODO: Use ID mappings and targetId the same way we do for wiser_itemdetail. Need to also update the triggers for wiser_itemlinkdetail for that.
                            await using var productionCommand = productionConnection.CreateCommand();
                            productionCommand.CommandText = queryPrefix;
                            if (String.IsNullOrWhiteSpace(actionData.NewValue))
                            {
                                actionData.MessageBuilder.AppendLine($"The new value is empty, so we will delete the item detail row from '{tableName}'.");
                                productionCommand.CommandText += $"""
                                                                  DELETE FROM `{tableName}`
                                                                  WHERE itemlink_id = ?linkId
                                                                  AND `key` = ?key
                                                                  AND language_code = ?languageCode
                                                                  AND groupname = ?groupName
                                                                  """;
                            }
                            else
                            {
                                var useLongValue = actionData.NewValue.Length > 1000;
                                sqlParameters["value"] = useLongValue ? "" : actionData.NewValue;
                                sqlParameters["longValue"] = useLongValue ? actionData.NewValue : "";

                                productionCommand.CommandText += $"""
                                                                  INSERT INTO `{tableName}` (language_code, itemlink_id, groupname, `key`, value, long_value)
                                                                  VALUES (?languageCode, ?linkId, ?groupName, ?key, ?value, ?longValue)
                                                                  ON DUPLICATE KEY UPDATE groupname = VALUES(groupname), value = VALUES(value), long_value = VALUES(long_value)
                                                                  """;
                            }

                            AddParametersToCommand(sqlParameters, productionCommand);
                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        case "ADD_FILE":
                        {
                            actionData.UsedMergeSettings = actionData.LinkIdOriginal > 0 ? linkTypeSettings : entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (actionData.UsedMergeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? $"The current row was skipped, because we were not able to find the {(actionData.LinkIdOriginal > 0 ? "link" : "entity")} type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of {(actionData.LinkIdOriginal > 0 ? $"links of type '{linkMergeSettings.Type}'" : $"items of type '{entityTypeMergeSettings.Type}'")}.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(actionData.ObjectIdOriginal))
                            {
                                // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because an ID mapping already exists for this file. This means that the current file was already created in production. Most likely Wiser created it both in production and branch at the same time, or someone added the ID mapping manually without deleting the corresponding row in wiser_history.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (String.IsNullOrWhiteSpace(columnNameForFileLink))
                            {
                                // We somehow do now have a column name for the file link, so we can't do anything with this row.
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because we have not been able to decide whether to use `item_id` or `itemlink_id` for the new file.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            // oldValue contains either "item_id" or "itemlink_id", to indicate which of these columns is used for the ID that is saved in newValue.
                            actionData.FileIdMapped = await GenerateNewIdAsync(tableName, productionConnection, branchConnection, actionData);
                            sqlParameters["fileItemId"] = actionData.NewValue;
                            sqlParameters["newId"] = actionData.FileIdMapped;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             INSERT INTO `{tableName}` (id, `{columnNameForFileLink.ToMySqlSafeValue(false)}`) 
                                                             VALUES (?newId, ?fileItemId)
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                            await AddIdMappingAsync(idMapping, idMappingsAddedInCurrentMerge, tableName, actionData.FileIdOriginal, actionData.FileIdMapped, branchConnection, productionConnection, actionData);

                            break;
                        }
                        case "UPDATE_FILE":
                        {
                            actionData.UsedMergeSettings = actionData.LinkIdOriginal > 0 ? linkTypeSettings : entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (actionData.UsedMergeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? $"The current row was skipped, because we were not able to find the {(actionData.LinkIdOriginal > 0 ? "link" : "entity")} type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of {(actionData.LinkIdOriginal > 0 ? $"links of type '{linkMergeSettings.Type}'" : $"items of type '{entityTypeMergeSettings.Type}'")}.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["fileId"] = actionData.FileIdMapped;
                            sqlParameters["originalFileId"] = actionData.FileIdOriginal;

                            if (String.Equals(actionData.Field, "content_length", StringComparison.OrdinalIgnoreCase))
                            {
                                // If the content length has been updated, we need to get the actual content from wiser_itemfile.
                                // We don't save the content bytes in wiser_history, because then the history table would become too huge.
                                byte[] file = null;
                                await using (var environmentCommand = branchConnection.CreateCommand())
                                {
                                    AddParametersToCommand(sqlParameters, environmentCommand);
                                    environmentCommand.CommandText = $"SELECT content FROM `{tableName}` WHERE id = ?originalFileId";
                                    await using var productionReader = await environmentCommand.ExecuteReaderAsync();
                                    if (await productionReader.ReadAsync())
                                    {
                                        file = (byte[]) productionReader.GetValue(0);
                                    }
                                }

                                sqlParameters["contents"] = file;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 UPDATE `{tableName}`
                                                                 SET content = ?contents
                                                                 WHERE id = ?fileId
                                                                 """;
                                await productionCommand.ExecuteNonQueryAsync();
                            }
                            else
                            {
                                if (String.IsNullOrWhiteSpace(actionData.Field))
                                {
                                    itemsProcessed++;
                                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                    actionData.Status = ObjectActionMergeStatuses.Failed;
                                    actionData.MessageBuilder.AppendLine($"For an {action} action, we need to know the field that was updated, but the field was not saved in `wiser_history` for some reason.");
                                    branchBatchLoggerService.LogMergeAction(actionData);
                                    continue;
                                }

                                sqlParameters["newValue"] = actionData.NewValue;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 UPDATE `{tableName}` 
                                                                 SET `{actionData.Field.ToMySqlSafeValue(false)}` = ?newValue
                                                                 WHERE id = ?fileId
                                                                 """;
                                await productionCommand.ExecuteNonQueryAsync();
                            }

                            break;
                        }
                        case "DELETE_FILE":
                        {
                            actionData.UsedMergeSettings = actionData.LinkIdOriginal > 0 ? linkTypeSettings : entityTypeMergeSettings;

                            // Check if the user requested this change to be synchronised.
                            if (actionData.UsedMergeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? $"The current row was skipped, because we were not able to find the {(actionData.LinkIdOriginal > 0 ? "link" : "entity")} type in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions of {(actionData.LinkIdOriginal > 0 ? $"links of type '{linkMergeSettings.Type}'" : $"items of type '{entityTypeMergeSettings.Type}'")}.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["fileId"] = actionData.FileIdMapped;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             DELETE FROM `{tableName}`
                                                             WHERE id = ?fileId
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        case "INSERT_ENTITY":
                        case "INSERT_ENTITYPROPERTY":
                        case "INSERT_QUERY":
                        case "INSERT_MODULE":
                        case "INSERT_DATA_SELECTOR":
                        case "INSERT_PERMISSION":
                        case "INSERT_USER_ROLE":
                        case "INSERT_FIELD_TEMPLATE":
                        case "INSERT_LINK_SETTING":
                        case "INSERT_API_CONNECTION":
                        case "INSERT_ROLE":
                        case "CREATE_STYLED_OUTPUT":
                        case "CREATE_EASY_OBJECT":
                        {
                            actionData.UsedMergeSettings = tableName switch
                            {
                                WiserTableNames.WiserEntity => entityMergeSettings,
                                WiserTableNames.WiserEntityProperty => entityPropertyMergeSettings,
                                WiserTableNames.WiserQuery => queryMergeSettings,
                                WiserTableNames.WiserModule => moduleMergeSettings,
                                WiserTableNames.WiserDataSelector => dataSelectorMergeSettings,
                                WiserTableNames.WiserPermission => permissionMergeSettings,
                                WiserTableNames.WiserUserRoles => userRoleMergeSettings,
                                WiserTableNames.WiserFieldTemplates => fieldTemplatesMergeSettings,
                                WiserTableNames.WiserLink => linkMergeSettings,
                                WiserTableNames.WiserApiConnection => apiConnectionMergeSettings,
                                WiserTableNames.WiserRoles => roleMergeSettings,
                                WiserTableNames.WiserStyledOutput => styledOutputMergeSettings,
                                "easy_objects" => objectMergeSettings,
                                _ => null
                            };

                            // Check if the user requested this change to be synchronised.
                            if (actionData.UsedMergeSettings is not {Create: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? $"The current row was skipped, because we were not able to find the merge settings for '{tableName}' in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge create actions from '{tableName}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(actionData.ObjectIdOriginal))
                            {
                                // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                historyItemsSynchronised.Add(historyId);
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.SkippedAndRemoved;
                                actionData.MessageBuilder.AppendLine("The current row was skipped and removed, because an ID mapping already exists for this object. This means that the current object was already created in production by Wiser, via the option 'Also create in main branch'. Or someone added the ID mapping manually without deleting the corresponding row in wiser_history.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            actionData.ObjectIdMapped = await GenerateNewIdAsync(tableName, productionConnection, branchConnection, actionData);
                            sqlParameters["newId"] = actionData.ObjectIdMapped;
                            sqlParameters["guid"] = Guid.NewGuid().ToString("N");
                            sqlParameters["guid2"] = Guid.NewGuid().ToString("N");

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);

                            // TODO: Dynamically look up if the current table has a unique index, and if so, generate a unique value for each column from that index.
                            // TODO: Then we can get rid of these if-else statements and make this code more generic.
                            // TODO: Afterwards, look up all values from the columns of this index, by using the newId variable and looking up the row in the current table in the branch.
                            // TODO: Then look in production to see if it already has a row with the same values. If so, map the newId to the ID of the row in production and skip the insert statement.
                            // TODO: Then all follow up update/delete statements should update the correct row in production and no longer cause duplicate key exceptions if someone has already added this row manually on production before the merge.

                            if (tableName.Equals(WiserTableNames.WiserEntity, StringComparison.OrdinalIgnoreCase))
                            {
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 SET @temporaryModuleId = (SELECT IFNULL(MAX(module_id), 0) + 1 FROM `{tableName}`);
                                                                 INSERT INTO `{tableName}` (id, `name`, `module_id`) 
                                                                 VALUES (?newId, ?guid, @temporaryModuleId)
                                                                 """;
                            }
                            else if (tableName.Equals(WiserTableNames.WiserEntityProperty, StringComparison.OrdinalIgnoreCase))
                            {
                                // The table wiser_entityproperty has a unique index (on entity_name, property_name), so we need to temporarily generate a unique property name, to prevent duplicate index errors when multiple properties are added in a row.
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 INSERT INTO `{tableName}` (id, `entity_name`, `property_name`) 
                                                                 VALUES (?newId, 'temp_wts', ?guid)
                                                                 """;
                                actionData.MessageBuilder.AppendLine($"Created a temporary property name ('{sqlParameters["guid"]}') for the entity property '{actionData.NewValue}', to prevent duplicate index errors when multiple properties are added in a row.");
                            }
                            else if (tableName.Equals(WiserTableNames.WiserLink, StringComparison.OrdinalIgnoreCase))
                            {
                                // The table wiser_link has a unique index (on type, desintation_entity_type, connected_entity_type), so we need to temporarily generate unique entity names, to prevent duplicate index errors when rows properties are added in a row.
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 INSERT INTO `{tableName}` (id, `type`, `destination_entity_type`, `connected_entity_type`) 
                                                                 VALUES (?newId, 0, ?guid, ?guid2)
                                                                 """;
                                actionData.MessageBuilder.AppendLine($"Saved temporary GUIDs in destination_entity_type ('{sqlParameters["guid"]}') and connected_entity_type ('{sqlParameters["guid2"]}'), to prevent duplicate index errors when multiple properties are added in a row.");
                            }
                            else if (tableName.Equals(WiserTableNames.WiserPermission, StringComparison.OrdinalIgnoreCase))
                            {
                                // The table wiser_permission has a unique index on all columns, so we need to temporarily use a role ID that doesn't exist yet, to prevent duplicate index errors when multiple permissions are added in a row.
                                // This query first gets the maximum role ID from the table, and then adds 1 to it, so that we can be sure that we have ID of a role that doesn't exist.
                                // Then it checks the highest role_id from the wiser_permission table, and if that is higher than the one we just generated, we add 1 to that one and use that as the temporary ID.
                                // This all is to prevent duplicate index errors and also to prevent accidentally overwriting someone's permissions.
                                // Lastly, it also adds temporary non-existing values to most other columns, to prevent duplicate index errors when the role_id gets updated in an "UPDATE_PERMISSION" action.
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 SET @temporaryRoleId = (SELECT IFNULL(MAX(id), 0) + 1 FROM `{WiserTableNames.WiserRoles}`);
                                                                 SET @temporaryRoleId = (SELECT IF(MAX(role_id) >= @temporaryRoleId, MAX(role_id) + 1, @temporaryRoleId) FROM `{tableName}`);
                                                                 SET @temporaryItemId = (SELECT IFNULL(MAX(item_id), 0) + 1 FROM `{tableName}`);
                                                                 SET @temporaryEntityPropertyId = (SELECT IFNULL(MAX(entity_property_id), 0) + 1 FROM `{tableName}`);
                                                                 SET @temporaryModuleId = (SELECT IFNULL(MAX(module_id), 0) + 1 FROM `{tableName}`);
                                                                 SET @temporaryQueryId = (SELECT IFNULL(MAX(query_id), 0) + 1 FROM `{tableName}`);
                                                                 SET @temporaryDataSelectorId = (SELECT IFNULL(MAX(data_selector_id), 0) + 1 FROM `{tableName}`);
                                                                 INSERT INTO `{tableName}` (id, `role_id`, `item_id`, `entity_property_id`, `module_id`, `query_id`, `data_selector_id`, `endpoint_url`)
                                                                 VALUES (?newId, @temporaryRoleId, @temporaryItemId, @temporaryEntityPropertyId, @temporaryModuleId, @temporaryQueryId, @temporaryDataSelectorId, ?guid)
                                                                 """;
                                actionData.MessageBuilder.AppendLine("Saved temporary values in most columns of wiser_permission, to prevent duplicate index errors when multiple properties are added in a row.");
                            }
                            else
                            {
                                productionCommand.CommandText = $"""
                                                                 {queryPrefix}
                                                                 INSERT INTO `{tableName}` (id) 
                                                                 VALUES (?newId)
                                                                 """;
                            }

                            await productionCommand.ExecuteNonQueryAsync();

                            // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                            await AddIdMappingAsync(idMapping, idMappingsAddedInCurrentMerge, tableName, actionData.ObjectIdOriginal, actionData.ObjectIdMapped, branchConnection, productionConnection, actionData);

                            break;
                        }
                        case "UPDATE_ENTITY":
                        case "UPDATE_ENTITYPROPERTY":
                        case "UPDATE_QUERY":
                        case "UPDATE_DATA_SELECTOR":
                        case "UPDATE_MODULE":
                        case "UPDATE_PERMISSION":
                        case "UPDATE_USER_ROLE":
                        case "UPDATE_FIELD_TEMPLATE":
                        case "UPDATE_LINK_SETTING":
                        case "UPDATE_API_CONNECTION":
                        case "UPDATE_ROLE":
                        case "UPDATE_STYLED_OUTPUT":
                        case "UPDATE_EASY_OBJECT":
                        {
                            actionData.UsedMergeSettings = tableName switch
                            {
                                WiserTableNames.WiserEntity => entityMergeSettings,
                                WiserTableNames.WiserEntityProperty => entityPropertyMergeSettings,
                                WiserTableNames.WiserQuery => queryMergeSettings,
                                WiserTableNames.WiserModule => moduleMergeSettings,
                                WiserTableNames.WiserDataSelector => dataSelectorMergeSettings,
                                WiserTableNames.WiserPermission => permissionMergeSettings,
                                WiserTableNames.WiserUserRoles => userRoleMergeSettings,
                                WiserTableNames.WiserFieldTemplates => fieldTemplatesMergeSettings,
                                WiserTableNames.WiserLink => linkMergeSettings,
                                WiserTableNames.WiserApiConnection => apiConnectionMergeSettings,
                                WiserTableNames.WiserRoles => roleMergeSettings,
                                WiserTableNames.WiserStyledOutput => styledOutputMergeSettings,
                                "easy_objects" => objectMergeSettings,
                                _ => null
                            };

                            // Check if the user requested this change to be synchronised.
                            if (actionData.UsedMergeSettings is not {Update: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? $"The current row was skipped, because we were not able to find the merge settings for '{tableName}' in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge update actions from '{tableName}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            if (String.IsNullOrWhiteSpace(actionData.Field))
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Failed;
                                actionData.MessageBuilder.AppendLine($"For an {action} action, we need to know the field that was updated, but the field was not saved in `wiser_history` for some reason.");
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["id"] = actionData.ObjectIdMapped;
                            sqlParameters["newValue"] = actionData.NewValue;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             UPDATE `{tableName}` 
                                                             SET `{actionData.Field.ToMySqlSafeValue(false)}` = ?newValue
                                                             WHERE id = ?id
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        case "DELETE_ENTITY":
                        case "DELETE_ENTITYPROPERTY":
                        case "DELETE_QUERY":
                        case "DELETE_DATA_SELECTOR":
                        case "DELETE_MODULE":
                        case "DELETE_PERMISSION":
                        case "DELETE_USER_ROLE":
                        case "DELETE_FIELD_TEMPLATE":
                        case "DELETE_LINK_SETTING":
                        case "DELETE_API_CONNECTION":
                        case "DELETE_ROLE":
                        case "DELETE_STYLED_OUTPUT":
                        case "DELETE_EASY_OBJECT":
                        {
                            actionData.UsedMergeSettings = tableName switch
                            {
                                WiserTableNames.WiserEntity => entityMergeSettings,
                                WiserTableNames.WiserEntityProperty => entityPropertyMergeSettings,
                                WiserTableNames.WiserQuery => queryMergeSettings,
                                WiserTableNames.WiserModule => moduleMergeSettings,
                                WiserTableNames.WiserDataSelector => dataSelectorMergeSettings,
                                WiserTableNames.WiserPermission => permissionMergeSettings,
                                WiserTableNames.WiserUserRoles => userRoleMergeSettings,
                                WiserTableNames.WiserFieldTemplates => fieldTemplatesMergeSettings,
                                WiserTableNames.WiserLink => linkMergeSettings,
                                WiserTableNames.WiserApiConnection => apiConnectionMergeSettings,
                                WiserTableNames.WiserRoles => roleMergeSettings,
                                WiserTableNames.WiserStyledOutput => styledOutputMergeSettings,
                                "easy_objects" => objectMergeSettings,
                                _ => null
                            };

                            // Check if the user requested this change to be synchronised.
                            if (actionData.UsedMergeSettings is not {Create: true})
                            {
                                itemsProcessed++;
                                await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                                actionData.Status = ObjectActionMergeStatuses.Skipped;
                                var message = linkTypeSettings == null
                                    ? $"The current row was skipped, because we were not able to find the merge settings for '{tableName}' in the settings, so we don't know if we should merge it or not."
                                    : $"The current row was skipped, because the user indicated that they don't want to merge delete actions from '{tableName}'.";
                                actionData.MessageBuilder.AppendLine(message);
                                branchBatchLoggerService.LogMergeAction(actionData);
                                continue;
                            }

                            sqlParameters["id"] = actionData.ObjectIdMapped;

                            await using var productionCommand = productionConnection.CreateCommand();
                            AddParametersToCommand(sqlParameters, productionCommand);
                            productionCommand.CommandText = $"""
                                                             {queryPrefix}
                                                             DELETE FROM `{tableName}`
                                                             WHERE `id` = ?id
                                                             """;
                            await productionCommand.ExecuteNonQueryAsync();

                            break;
                        }
                        default:
                            throw new ArgumentOutOfRangeException(nameof(action), action, $"Unsupported action for history synchronisation: '{action}'");
                    }

                    successfulChanges++;
                    historyItemsSynchronised.Add(historyId);
                    itemsProcessed++;
                    await UpdateProgressInQueue(databaseConnection, queueId, itemsProcessed);

                    actionData.Status = ObjectActionMergeStatuses.Merged;
                    actionData.MessageBuilder.AppendLine("The current row was successfully merged.");
                    branchBatchLoggerService.LogMergeAction(actionData);
                }
                catch (Exception exception)
                {
                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"An error occurred while trying to synchronise history ID '{historyId}' from '{branchDatabase}' to '{originalDatabase}': {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                    errors.Add($"Het is niet gelukt om de wijziging '{action}' voor object '{actionData.ObjectIdOriginal}' over te zetten. De fout was: {exception.Message}");

                    actionData.Status = ObjectActionMergeStatuses.Failed;
                    actionData.MessageBuilder.AppendLine($"An exception occurred while trying to merge the current action: {exception}");
                    branchBatchLoggerService.LogMergeAction(actionData);
                }
            }

            await UpdateProgressInQueue(databaseConnection, queueId, totalItemsInHistory, true);

            // Check if any ids need to be updated for styled output entries.
            if (idMappingsAddedInCurrentMerge.TryGetValue(WiserTableNames.WiserStyledOutput, out var styledOutputMappings))
            {
                await UpdateStyledOutputReferencesAsync(productionConnection, styledOutputMappings);
            }

            try
            {
                // Clear wiser_history in the selected environment, so that next time we can just sync all changes again.
                if (historyItemsSynchronised.Count != 0)
                {
                    await using var environmentCommand = branchConnection.CreateCommand();
                    environmentCommand.CommandText = $"DELETE FROM `{WiserTableNames.WiserHistory}` WHERE id IN ({String.Join(",", historyItemsSynchronised)})";
                    await environmentCommand.ExecuteNonQueryAsync();
                }
            }
            catch (Exception exception)
            {
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"An error occurred while trying to clean up after synchronising from '{branchDatabase}' to '{originalDatabase}': {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                errors.Add($"Er is iets fout gegaan tijdens het opruimen na de synchronisatie. Het wordt aangeraden om deze omgeving niet meer te gebruiken voor synchroniseren naar productie, anders kunnen dingen dubbel gescynchroniseerd worden. U kunt wel een nieuwe omgeving maken en vanuit daar weer verder werken. De fout was: {exception.Message}");
            }

            // Always commit, so we keep our progress.
            await branchTransaction.CommitAsync();
            await productionTransaction.CommitAsync();

            result["SuccessfulChanges"] = successfulChanges;
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Failed to merge the branch '{settings.DatabaseName}'. Error: {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
            errors.Add(exception.ToString());
        }
        finally
        {
            // Make sure we always unlock all tables when we're done, no matter what happens.
            await using (var environmentCommand = branchConnection.CreateCommand())
            {
                environmentCommand.CommandText = "UNLOCK TABLES";
                await environmentCommand.ExecuteNonQueryAsync();
            }

            await using (var productionCommand = productionConnection.CreateCommand())
            {
                productionCommand.CommandText = "UNLOCK TABLES";
                await productionCommand.ExecuteNonQueryAsync();
            }

            // Dispose and cleanup.
            await branchTransaction.DisposeAsync();
            await productionTransaction.DisposeAsync();

            await branchConnection.CloseAsync();
            await productionConnection.CloseAsync();

            await branchConnection.DisposeAsync();
            await productionConnection.DisposeAsync();
        }

        // Delete the branch if there were no errors and the user indicated it should be deleted after a successful merge.
        if (!errors.Any() && settings.DeleteAfterSuccessfulMerge)
        {
            try
            {
                // Change connection string to one with a specific user for deleting a database.
                if (!String.IsNullOrWhiteSpace(branchQueue.UsernameForManagingBranches) && !String.IsNullOrWhiteSpace(branchQueue.PasswordForManagingBranches))
                {
                    branchConnectionStringBuilder.UserID = branchQueue.UsernameForManagingBranches;
                    branchConnectionStringBuilder.Password = branchQueue.PasswordForManagingBranches;
                    await databaseConnection.ChangeConnectionStringsAsync(branchConnectionStringBuilder.ConnectionString, branchConnectionStringBuilder.ConnectionString);
                }

                await databaseHelpersService.DropDatabaseAsync(branchDatabase);
            }
            catch (Exception exception)
            {
                await logService.LogWarning(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Dropping the branch database '{branchDatabase}' failed after succesful merge. Error: {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                errors.Add($"Het verwijderen van de branch is niet gelukt: {exception}");
            }
        }

        await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.MergedBranchTemplateId, MergeBranchSubject, MergeBranchTemplate);

        result["Success"] = !errors.Any();
        return result;
    }

    /// <summary>
    /// Update the progress in the queue, so that we can see how far we are.
    /// This only updates after every 100 items, so that we don't execute too many queries.
    /// </summary>
    /// <param name="databaseConnection">The database connection to the main branch that contains the wiser_branch_queue table.</param>
    /// <param name="queueId">The ID of the row in the wiser_branch_queue table to update.</param>
    /// <param name="itemsProcessed">The amount of items that we processed so far.</param>
    /// <param name="forceUpdate">Whether to ignore the check on how many items are processed and just update it.</param>
    private static async Task UpdateProgressInQueue(IDatabaseConnection databaseConnection, int queueId, int itemsProcessed, bool forceUpdate = false)
    {
        // Only update after every 100 records, so that we don't execute too many queries.
        if (!forceUpdate && itemsProcessed % 100 != 0)
        {
            return;
        }

        databaseConnection.AddParameter("queueId", queueId);
        databaseConnection.AddParameter("itemsProcessed", itemsProcessed);
        await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET items_processed = ?itemsProcessed WHERE id = ?queueId");
    }

    /// <summary>
    /// Checks if any of the styled outputs have a different ID in production than the branch.
    /// If that is the case, finds any other styles outputs that use these IDs and updates their references with the new IDs.
    /// </summary>
    /// <param name="productionConnection">The <see cref="MySqlConnection"/> to the production database.</param>
    /// <param name="styledOutputMappings">The list of ID mappings for the styles output table.</param>
    private static async Task UpdateStyledOutputReferencesAsync(MySqlConnection productionConnection, Dictionary<ulong, ulong> styledOutputMappings)
    {
        var idsThatHaveChanged = styledOutputMappings.Where(x => x.Key != x.Value).ToList();
        var newIdsClause = String.Join(",", idsThatHaveChanged.Select(x => x.Value));
        foreach (var (branchId, productionId) in idsThatHaveChanged)
        {
            // Note: Curly Brace needs to be included in search/replace
            var searchString = $"{{StyledOutput~{branchId}";
            var replaceString = $"{{StyledOutput~{productionId}";

            await using var productionCommand = productionConnection.CreateCommand();
            productionCommand.Parameters.AddWithValue("searchString", searchString);

            // Update all references to this styled output via simple string replacement.
            // References to other styled output can look like this: "{StyledOutput~1}", or this: "{StyledOutput~999~SomeOtherParameter~SomeValue}".
            // We need to replace the ID in both cases, but we can't just replace "{StyledOutput~1" with "{StyledOutput~4",
            // because that would then turn "{StyledOutput~10}" into "{StyledOutput~40}", for example.
            productionCommand.CommandText = $$"""
                                             UPDATE {{WiserTableNames.WiserStyledOutput}}
                                             SET format_begin = REPLACE(format_begin, '{{searchString}}~', '{{replaceString}}~'),
                                                 format_begin = REPLACE(format_begin, '{{searchString}}}', '{{replaceString}}~'),
                                                 format_item = REPLACE(format_item, '{{searchString}}~', '{{replaceString}}}'),
                                                 format_item = REPLACE(format_item, '{{searchString}}}', '{{replaceString}}}'),
                                                 format_end = REPLACE(format_end, '{{searchString}}~', '{{replaceString}}~'),
                                                 format_end = REPLACE(format_end, '{{searchString}}}', '{{replaceString}}}'),
                                                 format_empty = REPLACE(format_empty, '{{searchString}}~', '{{replaceString}}~'),
                                                 format_empty = REPLACE(format_empty, '{{searchString}}}', '{{replaceString}}}')
                                             WHERE id IN ({{newIdsClause}})
                                             AND 
                                             (
                                                 format_begin LIKE '%{{searchString}}~%' OR
                                                 format_begin LIKE '%{{searchString}}}%' OR
                                                 format_item LIKE '%{{searchString}}~%' OR
                                                 format_item LIKE '%{{searchString}}}%' OR
                                                 format_end LIKE '%{{searchString}}~%' OR
                                                 format_end LIKE '%{{searchString}}}%' OR
                                                 format_empty LIKE '%{{searchString}}~%' OR
                                                 format_empty LIKE '%{{searchString}}}%'
                                             )
                                             """;

            await productionCommand.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Find information in a wiser_itemlink table, based on the link ID.
    /// This gets the source and destination item IDs and also checks if the link has been deleted in the branch database.
    /// </summary>
    /// <param name="linkId">The ID of the link to find.</param>
    /// <param name="sqlParameters">The dictionary that contains any previously added SQL parameters.</param>
    /// <param name="tableName">The full name of the table to find the ID in.</param>
    /// <param name="branchConnection">The database connection to the branch database.</param>
    /// <param name="linksCache">The cache object that is used to cache all link data.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    /// <returns>A <see cref="BranchMergeLinkCacheModel" /> with the link data we found.</returns>
    private static async Task<BranchMergeLinkCacheModel> GetLinkDataAsync(ulong linkId, Dictionary<string, object> sqlParameters, string tableName, MySqlConnection branchConnection, List<BranchMergeLinkCacheModel> linksCache, BranchMergeLogModel actionData)
    {
        actionData.MessageBuilder.AppendLine($"Attempting to get link data for link ID '{linkId}' from table '{tableName}'...");
        if (linkId == 0)
        {
            actionData.MessageBuilder.AppendLine("--> The link ID is 0, so there is nothing to check.");
            return new BranchMergeLinkCacheModel {Id = linkId};
        }

        var linkData = linksCache.SingleOrDefault(l => l.Id == linkId);
        if (linkData != null)
        {
            actionData.MessageBuilder.AppendLine("--> The link ID was found in the cache, so we're returning the information we found.");
            return linkData;
        }

        linkData = new BranchMergeLinkCacheModel {Id = linkId};
        linksCache.Add(linkData);

        sqlParameters["linkId"] = linkId;
        var itemLinkTableName = tableName
            .Replace(WiserTableNames.WiserItemFile, WiserTableNames.WiserItemLink, StringComparison.OrdinalIgnoreCase)
            .Replace(WiserTableNames.WiserItemLinkDetail, WiserTableNames.WiserItemLink, StringComparison.OrdinalIgnoreCase);

        await using var branchCommand = branchConnection.CreateCommand();
        AddParametersToCommand(sqlParameters, branchCommand);
        branchCommand.CommandText = $"SELECT item_id, destination_item_id, type FROM `{itemLinkTableName}` WHERE id = ?linkId LIMIT 1";
        var fileDataTable = new DataTable();
        using var adapter = new MySqlDataAdapter(branchCommand);
        adapter.Fill(fileDataTable);
        if (fileDataTable.Rows.Count == 0)
        {
            actionData.MessageBuilder.AppendLine("--> The link ID was not found in the database, so we assume it has been deleted.");
            linkData.IsDeleted = true;
            return linkData;
        }

        actionData.MessageBuilder.AppendLine("--> The link ID was found in the database, so we're returning the information we found.");
        linkData.ItemId = Convert.ToUInt64(fileDataTable.Rows[0]["item_id"]);
        linkData.DestinationItemId = Convert.ToUInt64(fileDataTable.Rows[0]["destination_item_id"]);
        linkData.Type = Convert.ToInt32(fileDataTable.Rows[0]["type"]);
        return linkData;
    }

    /// <summary>
    /// Find information in a wiser_itemfile table, based on the file ID.
    /// This gets the item_id and the itemlink_id of the file.
    /// </summary>
    /// <param name="fileId">The ID of the file to find.</param>
    /// <param name="sqlParameters">The dictionary that contains any previously added SQL parameters.</param>
    /// <param name="tableName">The full name of the table to find the ID in.</param>
    /// <param name="tablePrefix">The table prefix for the wiser_itemfile table.</param>
    /// <param name="branchConnection">The database connection to the branch database.</param>
    /// <param name="filesCache">The cache object that is used to cache all file data.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    /// <returns>A <see cref="BranchMergeFileCacheModel" /> with the file data we found.</returns>
    private static async Task<BranchMergeFileCacheModel> GetFileDataAsync(ulong fileId, Dictionary<string, object> sqlParameters, string tableName, string tablePrefix, MySqlConnection branchConnection, Dictionary<string, List<BranchMergeFileCacheModel>> filesCache, BranchMergeLogModel actionData)
    {
        actionData.MessageBuilder.AppendLine($"Attempting to get file data for file ID '{fileId}' from table '{tableName}'...");
        if (fileId == 0)
        {
            actionData.MessageBuilder.AppendLine("--> The file ID is 0, so there is nothing to check.");
            return new BranchMergeFileCacheModel {Id = fileId};
        }

        if (!filesCache.TryGetValue(tablePrefix, out var listOfFiles))
        {
            listOfFiles = [];
            filesCache.Add(tablePrefix, listOfFiles);
            actionData.MessageBuilder.AppendLine($"--> New table prefix found ('{tablePrefix}'), so we added it to the files cache dictionary.");
        }

        var fileData = listOfFiles.SingleOrDefault(file => file.Id == fileId);
        if (fileData != null)
        {
            actionData.MessageBuilder.AppendLine("--> The file ID was found in the cache, so we're returning the information we found.");
            return fileData;
        }

        sqlParameters["fileId"] = fileId;
        fileData = new BranchMergeFileCacheModel {Id = fileId};
        listOfFiles.Add(fileData);

        // If the code reaches this point, it means that we don't know the ID of the file yet, so look it up in the database.
        await using var branchCommand = branchConnection.CreateCommand();
        AddParametersToCommand(sqlParameters, branchCommand);
        branchCommand.CommandText = $"""
                                     SELECT item_id, itemlink_id FROM `{tableName}` WHERE id = ?fileId
                                     UNION
                                     SELECT item_id, itemlink_id FROM `{tableName}{WiserTableNames.ArchiveSuffix}` WHERE id = ?fileId
                                     LIMIT 1
                                     """;
        var fileDataTable = new DataTable();
        using var adapter = new MySqlDataAdapter(branchCommand);
        adapter.Fill(fileDataTable);
        if (fileDataTable.Rows.Count == 0)
        {
            actionData.MessageBuilder.AppendLine("--> The link ID was not found in the database, so we assume it has been deleted.");
            fileData.IsDeleted = true;
            return fileData;
        }

        actionData.MessageBuilder.AppendLine("--> The file ID was found in the database, so we're returning the information we found.");
        fileData.ItemId = Convert.ToUInt64(fileDataTable.Rows[0]["item_id"]);
        fileData.LinkId = Convert.ToUInt64(fileDataTable.Rows[0]["itemlink_id"]);
        return fileData;
    }

    /// <summary>
    /// Find information in a wiser_item table, based on the item ID.
    /// This gets the entity type of the item.
    /// </summary>
    /// <param name="itemId">The ID of the item to find.</param>
    /// <param name="sqlParameters">The dictionary that contains any previously added SQL parameters.</param>
    /// <param name="tableName">The full name of the table to find the ID in.</param>
    /// <param name="tablePrefix">The table prefix for the wiser_item table.</param>
    /// <param name="branchConnection">The database connection to the branch database.</param>
    /// <param name="itemsCache">The cache object that is used to cache all item data.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    /// <param name="entityType">Optional: If the entity type is already known, then you can enter that here so that it's just added to cache and not looked up in database.</param>
    /// <returns>A <see cref="BranchMergeItemCacheModel" /> with the item data we found.</returns>
    private static async Task<BranchMergeItemCacheModel> GetOrAddItemDataAsync(ulong itemId, Dictionary<string, object> sqlParameters, string tableName, string tablePrefix, MySqlConnection branchConnection, Dictionary<string, List<BranchMergeItemCacheModel>> itemsCache, BranchMergeLogModel actionData, string entityType = null)
    {
        actionData.MessageBuilder.AppendLine($"Attempting to get item data for item ID '{itemId}' from table '{tableName}'...");
        if (itemId == 0)
        {
            actionData.MessageBuilder.AppendLine("--> The item ID is 0, so there is nothing to check.");
            return new BranchMergeItemCacheModel {Id = itemId};
        }

        if (!itemsCache.TryGetValue(tablePrefix, out var listOfItems))
        {
            listOfItems = [];
            itemsCache.Add(tablePrefix, listOfItems);
            actionData.MessageBuilder.AppendLine($"--> New table prefix found ('{tablePrefix}'), so we added it to the items cache dictionary.");
        }

        var itemData = listOfItems.SingleOrDefault(item => item.Id == itemId);
        if (itemData != null)
        {
            actionData.MessageBuilder.AppendLine("--> The item ID was found in the cache, so we're returning the information we found.");
            return itemData;
        }

        itemData = new BranchMergeItemCacheModel {Id = itemId};
        listOfItems.Add(itemData);

        if (!String.IsNullOrWhiteSpace(entityType))
        {
            actionData.MessageBuilder.AppendLine($"--> The item ID was not found in the cache, but we already know the entity type ('{entityType}'), so we're adding it to cache and returning the information.");
            itemData.EntityType = entityType;
            return itemData;
        }

        // If the code reaches this point, it means that we don't know the ID of the item yet, so look it up in the database.
        sqlParameters["itemId"] = itemId;
        await using var branchCommand = branchConnection.CreateCommand();
        AddParametersToCommand(sqlParameters, branchCommand);
        branchCommand.CommandText = $"""
                                     SELECT entity_type FROM `{tableName}` WHERE id = ?itemId
                                     UNION
                                     SELECT entity_type FROM `{tableName}{WiserTableNames.ArchiveSuffix}` WHERE id = ?itemId
                                     LIMIT 1
                                     """;
        var itemDataTable = new DataTable();
        using var adapter = new MySqlDataAdapter(branchCommand);
        adapter.Fill(itemDataTable);
        if (itemDataTable.Rows.Count == 0)
        {
            actionData.MessageBuilder.AppendLine("--> The item ID was not found in the database, so we assume it has been deleted.");
            itemData.IsDeleted = true;
            return itemData;
        }

        actionData.MessageBuilder.AppendLine("--> The item ID was found in the database, so we're returning the information we found.");
        itemData.EntityType = itemDataTable.Rows[0].Field<string>("entity_type");
        return itemData;
    }

    /// <summary>
    /// Finish the branch action by updating the queue with the results of the action and sending an e-mail to the user that initialized the action.
    /// This method should be called at the end of every branch action (e.g. create, merge, delete, etc.).
    /// </summary>
    /// <param name="queueId">The ID from the wiser_branch_queue table.</param>
    /// <param name="dataRowWithSettings">The <see cref="DataRow"/> from the wiser_branch_queue table, for the current action.</param>
    /// <param name="branchQueue">The <see cref="BranchQueueModel"/> with the settings from the WTS configuration for handling branches.</param>
    /// <param name="configurationServiceName">The name of the configuration.</param>
    /// <param name="databaseConnection">The database connection to the database of the main branch.</param>
    /// <param name="wiserItemsService">The <see cref="IWiserItemsService"/> for the main branch, that we can use to find the e-mail template in.</param>
    /// <param name="taskAlertsService">The <see cref="ITaskAlertsService"/> for the main branch, that we can use to send the user a notification in Wiser.</param>
    /// <param name="errors">The list of errors that was generated by the branch action method, to save in the wiser_branch_queue table for logging/debugging.</param>
    /// <param name="stopwatch">The <see cref="Stopwatch"/> that was used in the branch action method, so that we can log how long the action took.</param>
    /// <param name="startDate">The <see cref="DateTime"/> that the action was started.</param>
    /// <param name="templateId">The ID of the e-mail template in Wiser.</param>
    /// <param name="defaultMessageSubject">The subject to use for the e-mail, if none was found in the template in Wiser.</param>
    /// <param name="defaultMessageContent">The body use for the e-mail, if none was found in the template in Wiser.</param>
    private static async Task FinishBranchActionAsync(int queueId, DataRow dataRowWithSettings, BranchQueueModel branchQueue, string configurationServiceName, IDatabaseConnection databaseConnection, IWiserItemsService wiserItemsService, ITaskAlertsService taskAlertsService, JArray errors, Stopwatch stopwatch, DateTime startDate, ulong templateId, string defaultMessageSubject, string defaultMessageContent)
    {
        var errorsString = errors.ToString();
        if (errorsString == "[]")
        {
            errorsString = "";
        }

        // Set the finish date to the current datetime, so that we can see how long it took.
        var endDate = DateTime.Now;
        stopwatch.Stop();
        databaseConnection.AddParameter("queueId", queueId);
        databaseConnection.AddParameter("now", endDate);
        databaseConnection.AddParameter("error", errorsString);
        databaseConnection.AddParameter("success", String.IsNullOrWhiteSpace(errorsString));
        await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET finished_on = ?now, success = ?success, errors = ?error WHERE id = ?queueId");

        var userId = dataRowWithSettings.Field<ulong>("user_id");
        var addedBy = dataRowWithSettings.Field<string>("added_by");
        var replaceData = new Dictionary<string, object>
        {
            {"name", dataRowWithSettings.Field<string>("name")},
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
        if (templateId > 0)
        {
            template = await wiserItemsService.GetItemDetailsAsync(branchQueue.MergedBranchTemplateId, userId: userId, skipPermissionsCheck: true);
        }

        var subject = template?.GetDetailValue("subject");
        var content = template?.GetDetailValue("template");
        if (String.IsNullOrWhiteSpace(subject))
        {
            subject = defaultMessageSubject;
        }

        if (String.IsNullOrWhiteSpace(content))
        {
            content = defaultMessageContent;
        }

        if (userId > 0)
        {
            await taskAlertsService.NotifyUserByEmailAsync(userId, addedBy, branchQueue, configurationServiceName, subject, content, replaceData, template?.GetDetailValue("sender_email"), template?.GetDetailValue("sender_name"));
            await taskAlertsService.SendMessageToUserAsync(userId, addedBy, subject, branchQueue, configurationServiceName, replaceData, userId, addedBy);
        }
    }

    /// <summary>
    /// Handles the merging of changes from a branch back into the main/original branch.
    /// This will only merge the changes that the user requested to be merged.
    /// </summary>
    /// <param name="dataRowWithSettings">The <see cref="DataRow"/> from wiser_branch_queue.</param>
    /// <param name="branchQueue">The <see cref="BranchQueueModel"/> with the settings from the XML configuration.</param>
    /// <param name="configurationServiceName">The name of the configuration.</param>
    /// <param name="databaseConnection">The <see cref="IDatabaseConnection"/> with the connection to the database.</param>
    /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> for checking if a table exists, creating new tables etc.</param>
    /// <param name="wiserItemsService">The <see cref="IWiserItemsService"/> for getting settings of entity types and for (un)deleting items.</param>
    /// <param name="taskAlertsService">Service to send task alerts to a user.</param>
    /// <returns>An <see cref="JObject"/> with properties "Success" and "ErrorMessage".</returns>
    /// <exception cref="ArgumentOutOfRangeException">Then we get unknown options in enums.</exception>
    private async Task<JToken> HandleDeleteBranchActionAsync(DataRow dataRowWithSettings, BranchQueueModel branchQueue, string configurationServiceName, IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, IWiserItemsService wiserItemsService, ITaskAlertsService taskAlertsService)
    {
        var error = "";
        var result = new JObject();
        var startDate = DateTime.Now;
        var stopwatch = new Stopwatch();
        stopwatch.Start();

        // Set the start date to the current datetime.
        var queueId = dataRowWithSettings.Field<int>("id");
        databaseConnection.AddParameter("queueId", queueId);
        databaseConnection.AddParameter("now", startDate);
        await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET started_on = ?now WHERE id = ?queueId");

        // Get and validate the settings.
        var settings = JsonConvert.DeserializeObject<BranchActionBaseModel>(dataRowWithSettings.Field<string>("data") ?? "{}");
        if (String.IsNullOrWhiteSpace(settings?.DatabaseName))
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to delete a branch, but it either had invalid settings, or the database name was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

            error = "Trying to delete a branch, but it either had invalid settings, or the database name was empty.";
            result.Add("ErrorMessage", error);
            result.Add("Success", false);

            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? [] : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
            return result;
        }

        // If the database to be deleted is the same as the currently connected database, then it's most likely the main branch, so stop the deletion.
        if (databaseConnection.ConnectedDatabase == settings.DatabaseName)
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to delete a branch, but it looks to be the main branch and for safety reasons we don't allow the main branch to be deleted. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

            error = "Trying to delete a branch, but it looks to be the main branch and for safety reasons we don't allow the main branch to be deleted.";
            result.Add("ErrorMessage", error);
            result.Add("Success", false);

            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? [] : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
            return result;
        }

        // The name of the branch database is always prefixed with the name of the main branch database.
        // If that is not the case, then we're trying to delete a branch of a different tenant, so stop the deletion.
        if (!settings.DatabaseName.StartsWith(databaseConnection.ConnectedDatabase))
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to delete a branch, but it looks like this branch does not belong to the customer that this WTS is connected to. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

            error = "Trying to delete a branch, but it looks like this branch does not belong to the customer that this WTS is connected to.";
            result.Add("ErrorMessage", error);
            result.Add("Success", false);

            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? [] : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
            return result;
        }

        try
        {
            // Delete the database, if it exists.
            if (await databaseHelpersService.DatabaseExistsAsync(settings.DatabaseName))
            {
                await databaseHelpersService.DropDatabaseAsync(settings.DatabaseName);
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Failed to delete the branch '{settings.DatabaseName}'. Error: {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
            error = exception.ToString();
        }

        // Set the finish time to the current datetime, so that we can see how long it took.
        await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? [] : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
        result.Add("ErrorMessage", error);
        result.Add("Success", String.IsNullOrWhiteSpace(error));
        return result;
    }

    /// <summary>
    /// Lock a list of tables in a <see cref="MySqlConnection"/>.
    /// </summary>
    /// <param name="mySqlConnection">The <see cref="MySqlConnection"/> to lock the tables in.</param>
    /// <param name="tablesToLock">The list of tables to lock.</param>
    /// <param name="alsoLockIdMappingsTable">Whether to also lock the table "wiser_id_mappings". Only set this to true for the branch database, since this table doesn't exist in the original/main database.</param>
    private static async Task LockTablesAsync(MySqlConnection mySqlConnection, IEnumerable<string> tablesToLock, bool alsoLockIdMappingsTable)
    {
        await using var productionCommand = mySqlConnection.CreateCommand();
        productionCommand.CommandText = $"LOCK TABLES {(!alsoLockIdMappingsTable ? "" : $"{WiserTableNames.WiserIdMappings} WRITE, ")}{String.Join(", ", tablesToLock.Select(table => $"{table} WRITE"))}";
        await productionCommand.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Add all parameters from a dictionary to a <see cref="MySqlCommand"/>.
    /// </summary>
    /// <param name="parameters">The dictionary with the parameters.</param>
    /// <param name="command">The database command.</param>
    private static void AddParametersToCommand(Dictionary<string, object> parameters, MySqlCommand command)
    {
        command.Parameters.Clear();
        foreach (var parameter in parameters)
        {
            command.Parameters.AddWithValue(parameter.Key, parameter.Value);
        }
    }

    /// <summary>
    /// Get the ID of an item from the mappings. The returned ID will be the ID of the same item in the production environment.
    /// If there is no mapping for this ID, it means the ID is the same in both environments and the input will be returned.
    /// </summary>
    /// <param name="tableName">The table that the ID belongs to.</param>
    /// <param name="idMapping">The dictionary that contains all the ID mappings.</param>
    /// <param name="id">The ID to get the mapped value of.</param>
    /// <param name="actionData"></param>
    /// <param name="returnNullIfNotFound">Optional: Whether to return <c>null</c> if no mapping was found. When <c>false</c>, we'll return the same value as the input. Default value is <c>false</c>.</param>
    /// <returns>The ID of the same item in the production environment.</returns>
    private static ulong? GetMappedId(string tableName, Dictionary<string, Dictionary<ulong, ulong>> idMapping, ulong? id, BranchMergeLogModel actionData, bool returnNullIfNotFound = false)
    {
        var defaultValue = returnNullIfNotFound ? null : id;
        if (id is null or 0)
        {
            return defaultValue;
        }

        actionData.MessageBuilder.AppendLine($"Attempting to get mapped ID for table '{tableName}' and ID '{id}'...");

        if (!idMapping.TryGetValue(tableName, out var tableIdMapping))
        {
            actionData.MessageBuilder.AppendLine($"--> No ID mappings found for this table, so returning {(returnNullIfNotFound ? "null" : "the same value")}.");
            return defaultValue;
        }

        if (!tableIdMapping.TryGetValue(id.Value, out var mappedId))
        {
            actionData.MessageBuilder.AppendLine($"--> The ID is not mapped, so returning {(returnNullIfNotFound ? "null" : "the same value")}.");
            return defaultValue;
        }

        actionData.MessageBuilder.AppendLine($"--> The ID is mapped to {mappedId}, so returning that.");
        return mappedId;
    }

    /// <summary>
    /// Get the entity types and table prefixes for both items in a link.
    /// </summary>
    /// <param name="sourceId">The ID of the source item.</param>
    /// <param name="destinationId">The ID of the destination item.</param>
    /// <param name="linkType">The link type number.</param>
    /// <param name="mySqlConnection">The connection to the database.</param>
    /// <param name="branchEntityTypesService">An instance of the <see cref="IEntityTypesService"/> with a database connection to the branch database.</param>
    /// <param name="allLinkTypeSettings">The list with all link type settings.</param>
    /// <param name="allEntityTypeSettings">The dictionary with settings for each entity type. This function will add items to this dictionary if they don't exist yet.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    /// <returns>A named tuple with the entity types and table prefixes for both the source and the destination.</returns>
    private static async Task<(string SourceType, string SourceTablePrefix, string DestinationType, string DestinationTablePrefix)?> GetEntityTypesOfLinkAsync(ulong sourceId, ulong destinationId, int linkType, MySqlConnection mySqlConnection, IEntityTypesService branchEntityTypesService, List<LinkSettingsModel> allLinkTypeSettings, Dictionary<string, EntitySettingsModel> allEntityTypeSettings, BranchMergeLogModel actionData)
    {
        actionData.MessageBuilder.AppendLine($"Looking up entity types for link type '{linkType}', sourceId '{sourceId}' and destinationId '{destinationId}'...");
        var currentLinkTypeSettings = allLinkTypeSettings.Where(l => l.Type == linkType).ToList();
        actionData.MessageBuilder.AppendLine($"--> Found {currentLinkTypeSettings.Count} link type settings with this number.");

        await using var command = mySqlConnection.CreateCommand();
        command.Parameters.AddWithValue("sourceId", sourceId);
        command.Parameters.AddWithValue("destinationId", destinationId);

        // It's possible that there are multiple link types that use the same number, so we have to check all of them.
        foreach (var linkTypeSettings in currentLinkTypeSettings)
        {
            actionData.MessageBuilder.AppendLine($"--> Checking link type settings with ID '{linkTypeSettings.Id}', source entity type '{linkTypeSettings.SourceEntityType}' and destination entity type '{linkTypeSettings.DestinationEntityType}'...");

            // Get settings entity settings from cache if we have them, otherwise get them from database and add to cache.
            if (!allEntityTypeSettings.TryGetValue(linkTypeSettings.SourceEntityType, out var sourceEntityTypeSettings))
            {
                sourceEntityTypeSettings = await branchEntityTypesService.GetEntityTypeSettingsAsync(linkTypeSettings.SourceEntityType);
                allEntityTypeSettings.Add(linkTypeSettings.SourceEntityType, sourceEntityTypeSettings);

                actionData.MessageBuilder.AppendLine($"----> We did not find the source entity type settings in the cache, so we fetched them from the database.");
            }
            else
            {
                actionData.MessageBuilder.AppendLine("----> Found the source entity type settings in the cache.");
            }

            if (!allEntityTypeSettings.TryGetValue(linkTypeSettings.DestinationEntityType, out var destinationEntityTypeSettings))
            {
                destinationEntityTypeSettings = await branchEntityTypesService.GetEntityTypeSettingsAsync(linkTypeSettings.DestinationEntityType);
                allEntityTypeSettings.Add(linkTypeSettings.DestinationEntityType, destinationEntityTypeSettings);

                actionData.MessageBuilder.AppendLine($"----> We did not find the destination entity type settings in the cache, so we fetched them from the database.");
            }
            else
            {
                actionData.MessageBuilder.AppendLine("----> Found the destination entity type settings in the cache.");
            }

            // Get the table prefixes we need from the entity settings.
            var sourceTablePrefix = branchEntityTypesService.GetTablePrefixForEntity(sourceEntityTypeSettings);
            var destinationTablePrefix = branchEntityTypesService.GetTablePrefixForEntity(destinationEntityTypeSettings);
            var sourceTableName = $"{sourceTablePrefix}{WiserTableNames.WiserItem}";
            var destinationTableName = $"{destinationTablePrefix}{WiserTableNames.WiserItem}";

            // Check if the source item exists in this table.
            command.CommandText = $"""
                                   SELECT entity_type FROM {sourceTableName} WHERE id = ?sourceId
                                   UNION
                                   SELECT entity_type FROM {sourceTableName}{WiserTableNames.ArchiveSuffix} WHERE id = ?sourceId
                                   LIMIT 1
                                   """;
            var sourceDataTable = new DataTable();
            using var sourceAdapter = new MySqlDataAdapter(command);
            sourceAdapter.Fill(sourceDataTable);
            if (sourceDataTable.Rows.Count == 0)
            {
                actionData.MessageBuilder.AppendLine($"----> Did not find any item with ID '{sourceId}' in table '{sourceTableName}'. So this cannot be the correct link type.");
                continue;
            }

            var entityType = sourceDataTable.Rows[0].Field<string>("entity_type");
            if (!String.Equals(entityType, linkTypeSettings.SourceEntityType))
            {
                actionData.MessageBuilder.AppendLine($"----> We found an item with ID '{sourceId}' in table '{sourceTableName}', but it has the entity type '{entityType}' while we expected it to have the entity type '{linkTypeSettings.SourceEntityType}'. So this cannot be the correct link type.");
                continue;
            }

            // Check if the destination item exists in this table.
            command.CommandText = $"""
                                   SELECT entity_type FROM {destinationTableName} WHERE id = ?destinationId
                                   UNION
                                   SELECT entity_type FROM {destinationTableName}{WiserTableNames.ArchiveSuffix} WHERE id = ?destinationId
                                   LIMIT 1
                                   """;
            var destinationDataTable = new DataTable();
            using var destinationAdapter = new MySqlDataAdapter(command);
            destinationAdapter.Fill(destinationDataTable);
            if (destinationDataTable.Rows.Count == 0)
            {
                actionData.MessageBuilder.AppendLine($"----> Did not find any item with ID '{destinationId}' in table '{destinationTableName}'. So this cannot be the correct link type.");
                continue;
            }

            entityType = destinationDataTable.Rows[0].Field<string>("entity_type");
            if (!String.Equals(entityType, linkTypeSettings.DestinationEntityType))
            {
                actionData.MessageBuilder.AppendLine($"----> We found an item with ID '{destinationId}' in table '{destinationTableName}', but it has the entity type '{entityType}' while we expected it to have the entity type '{linkTypeSettings.DestinationEntityType}'. So this cannot be the correct link type.");
                continue;
            }

            // If we reached this point, it means we found the correct link type and entity types.
            actionData.MessageBuilder.AppendLine("----> We found both the source and destination item in the correct tables with the expected entity types, so we can use this link type.");
            return (linkTypeSettings.SourceEntityType, sourceTablePrefix, linkTypeSettings.DestinationEntityType, destinationTablePrefix);
        }

        actionData.MessageBuilder.AppendLine("--> We were NOT able to find the correct link type settings.");
        return null;
    }

    /// <summary>
    /// Generates a new ID for the specified table. This will get the highest number from both databases and add 1 to that number.
    /// This is to make sure that the new ID will not exist anywhere yet, to prevent later synchronisation problems.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="productionConnection">The connection to the production database.</param>
    /// <param name="environmentConnection">The connection to the environment database.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    /// <returns>The new ID that should be used for the first new item to be inserted into this table.</returns>
    private static async Task<ulong> GenerateNewIdAsync(string tableName, MySqlConnection productionConnection, MySqlConnection environmentConnection, BranchMergeLogModel actionData)
    {
        actionData.MessageBuilder.AppendLine($"Generating new ID for table '{tableName}'...");
        await using var productionCommand = productionConnection.CreateCommand();
        await using var environmentCommand = environmentConnection.CreateCommand();

        productionCommand.CommandText = $"SELECT IFNULL(MAX(id), 0) AS maxId FROM `{tableName}`";
        environmentCommand.CommandText = $"SELECT IFNULL(MAX(id), 0) AS maxId FROM `{tableName}`";

        var maxProductionId = Convert.ToUInt64(await productionCommand.ExecuteScalarAsync() ?? 0);
        actionData.MessageBuilder.AppendLine($"--> The highest ID in the production database is: {maxProductionId}");

        var maxEnvironmentId = Convert.ToUInt64(await environmentCommand.ExecuteScalarAsync() ?? 0);
        actionData.MessageBuilder.AppendLine($"--> The highest ID in the branch database is: {maxEnvironmentId}");

        var newId = Math.Max(maxProductionId, maxEnvironmentId) + 1;
        actionData.MessageBuilder.AppendLine($"--> Done! The new ID for production will be: {newId}");

        return newId;
    }

    /// <summary>
    /// Add an ID mapping, to map the ID of the environment database to the same item with a different ID in the production database.
    /// </summary>
    /// <param name="idMappings">The dictionary that contains the in-memory mappings.</param>
    /// <param name="idMappingsAddedInCurrentMerge">The list of id mappings that were added in the current merge.</param>
    /// <param name="tableName">The table that the ID belongs to.</param>
    /// <param name="originalItemId">The ID of the item in the selected environment.</param>
    /// <param name="newItemId">The ID of the item in the production environment.</param>
    /// <param name="environmentConnection">The database connection to the selected environment.</param>
    /// <param name="productionConnection">The database connection to the production environment.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    private static async Task AddIdMappingAsync(Dictionary<string, Dictionary<ulong, ulong>> idMappings, Dictionary<string, Dictionary<ulong, ulong>> idMappingsAddedInCurrentMerge, string tableName, ulong originalItemId, ulong newItemId, MySqlConnection environmentConnection, MySqlConnection productionConnection, BranchMergeLogModel actionData)
    {
        actionData.MessageBuilder.AppendLine($"Attempting to map ID '{originalItemId}' to '{newItemId}', for table '{tableName}'...");

        // Make sure that we have a list of mappings for this table.
        if (!idMappings.TryGetValue(tableName, out var tableIdMappings))
        {
            tableIdMappings = new Dictionary<ulong, ulong>();
            idMappings.Add(tableName, tableIdMappings);
        }

        // Check if the ID is already mapped to something.
        if (tableIdMappings.TryGetValue(originalItemId, out var previouslyMappedId))
        {
            if (previouslyMappedId == newItemId)
            {
                // If we had already mapped these IDs together before, we can skip it.
                actionData.MessageBuilder.AppendLine("--> The mapping already exists, so nothing to do here.");
                return;
            }

            // If the item is already mapped to something else, check if that other item still exists.
            await using var productionCommand = productionConnection.CreateCommand();
            productionCommand.CommandText = $"""
                                             SELECT id 
                                             FROM `{tableName}` 
                                             WHERE id = ?previouslyMappedId
                                             """;

            productionCommand.Parameters.AddWithValue("previouslyMappedId", previouslyMappedId);
            var existingId = Convert.ToUInt64(await productionCommand.ExecuteScalarAsync() ?? 0);

            if (existingId == 0)
            {
                // If the mapped ID does not exist anymore, remove it so that we can remap it.
                actionData.MessageBuilder.AppendLine($"--> This object was already mapped to ID '{previouslyMappedId}', but that ID does not exist anymore, so we can remove the mapping and add a new one.");
                await RemoveIdMappingAsync(idMappings, tableName, originalItemId, environmentConnection, actionData);
            }
            else
            {
                // If the mapped ID still exists, we can't map this ID to a different ID, so throw an error because we can't safely continue.
                actionData.MessageBuilder.AppendLine($"--> This object was already mapped to ID '{previouslyMappedId}' and that object still exists, so we have to stop here because we this situation requires manual checks by a developer.");
                throw new Exception($"Trying to map ID '{originalItemId}' to '{newItemId}', of table '{tableName}', but it was already mapped to '{previouslyMappedId}'.");
            }
        }

        tableIdMappings.Add(originalItemId, newItemId);
        await using var environmentCommand = environmentConnection.CreateCommand();
        environmentCommand.CommandText = $"""
                                          INSERT INTO `{WiserTableNames.WiserIdMappings}` 
                                          (table_name, our_id, production_id)
                                          VALUES (?tableName, ?ourId, ?productionId)
                                          """;

        environmentCommand.Parameters.AddWithValue("tableName", tableName);
        environmentCommand.Parameters.AddWithValue("ourId", originalItemId);
        environmentCommand.Parameters.AddWithValue("productionId", newItemId);
        await environmentCommand.ExecuteNonQueryAsync();

        // Add the mapping to the list of mappings that were added in the current merge, so that we can do some checks/logging with that later.
        if (!idMappingsAddedInCurrentMerge.TryGetValue(tableName, out tableIdMappings))
        {
            tableIdMappings = new Dictionary<ulong, ulong>();
            idMappingsAddedInCurrentMerge.Add(tableName, tableIdMappings);
        }

        // Ignore the result from TryAdd, because we don't need to do anything if it already exists.
        _ = tableIdMappings.TryAdd(originalItemId, newItemId);

        actionData.MessageBuilder.AppendLine("--> New mapping successfully added to database and in-memory list.");
    }

    /// <summary>
    /// Remove an ID mapping, if an item that was already mapped was removed, it should also be removed from the mapping.
    /// </summary>
    /// <param name="idMappings">The dictionary that contains the in-memory mappings.</param>
    /// <param name="tableName">The table that the ID belongs to.</param>
    /// <param name="originalItemId">The ID of the item in the selected environment.</param>
    /// <param name="environmentConnection">The database connection to the selected environment.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    private static async Task RemoveIdMappingAsync(IDictionary<string, Dictionary<ulong, ulong>> idMappings, string tableName, ulong originalItemId, MySqlConnection environmentConnection, BranchMergeLogModel actionData = null)
    {
        await RemoveIdMappingsAsync(idMappings, tableName, [originalItemId], environmentConnection, actionData);
    }

    /// <summary>
    /// Remove multiple ID mappings. If an item that was already mapped was removed, it should also be removed from the mapping table.
    /// </summary>
    /// <param name="idMappings">The dictionary that contains the in-memory mappings.</param>
    /// <param name="tableName">The table that the ID belongs to.</param>
    /// <param name="originalItemIds">The IDs of the items in the selected environment.</param>
    /// <param name="environmentConnection">The database connection to the selected environment.</param>
    /// <param name="actionData">The <see cref="BranchMergeLogModel" /> where we can add log messages to.</param>
    private static async Task RemoveIdMappingsAsync(IDictionary<string, Dictionary<ulong, ulong>> idMappings, string tableName, List<ulong> originalItemIds, MySqlConnection environmentConnection, BranchMergeLogModel actionData = null)
    {
        actionData?.MessageBuilder.AppendLine($"Attempting to remove ID mappings '{String.Join(", ", originalItemIds)}' for table '{tableName}'...");
        if (!idMappings.TryGetValue(tableName, out var tableIdMappings))
        {
            tableIdMappings = new Dictionary<ulong, ulong>();
            idMappings.Add(tableName, tableIdMappings);
        }

        var idsToRemoveFromDatabase = new List<ulong>();
        foreach (var originalItemId in originalItemIds.Where(id => tableIdMappings.ContainsKey(id)))
        {
            tableIdMappings.Remove(originalItemId);
            idsToRemoveFromDatabase.Add(originalItemId);
        }

        if (idsToRemoveFromDatabase.Count == 0)
        {
            actionData?.MessageBuilder.AppendLine("--> None of these IDs were found in the mappings, so nothing to do here.");
            return;
        }

        await using var environmentCommand = environmentConnection.CreateCommand();
        environmentCommand.CommandText = $"""
                                          DELETE FROM `{WiserTableNames.WiserIdMappings}` 
                                          WHERE table_name = ?tableName 
                                          AND our_id IN ({String.Join(",", idsToRemoveFromDatabase)})
                                          """;

        environmentCommand.Parameters.AddWithValue("tableName", tableName);
        await environmentCommand.ExecuteNonQueryAsync();

        actionData?.MessageBuilder.AppendLine("--> Mappings successfully removed from database and in-memory list.");
    }
}