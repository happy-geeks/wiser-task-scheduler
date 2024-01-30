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
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Branches.Enums;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Branches.Models;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Modules.Branches.Services
{
    /// <inheritdoc cref="IBranchQueueService" />
    public class BranchQueueService : IBranchQueueService, IActionsService, IScopedService
    {
        private const string CreateBranchSubject = "Branch with the name '{name}' [if({errorCount}=0)]has been created successfully[else]could not be created[endif] on {date:DateTime(dddd\\, dd MMMM yyyy,en-US)}";
        private const string CreateBranchTemplate = "<p>The branch creation started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The creation took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the creation of the branch: {errors:Raw}[endif]";
        private const string MergeBranchSubject = "Branch with the name '{name}' [if({errorCount}=0)]has been merged successfully[else]could not be merged[endif] on {date:DateTime(dddd\\, dd MMMM yyyy,en-US)}";
        private const string MergeBranchTemplate = "<p>The branch merge started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The merge took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the merge of the branch: {errors:Raw}[endif]";
        private const string DeleteBranchSubject = "Branch with the name '{name}' [if({errorCount}=0)]has been deleted successfully[else]could not be deleted[endif] on {date:DateTime(dddd\\, dd MMMM yyyy,en-US)}";
        private const string DeleteBranchTemplate = "<p>The branch deletion started on {startDate:DateTime(HH\\:mm\\:ss)} and finished on {endDate:DateTime(HH\\:mm\\:ss)}. The deletion took a total of {hours} hour(s), {minutes} minute(s) and {seconds} second(s).</p>[if({errorCount}!0)] <br /><br />The following errors occurred during the deletion of the branch: {errors:Raw}[endif]";

        private readonly ILogService logService;
        private readonly ILogger<BranchQueueService> logger;
        private readonly IServiceProvider serviceProvider;

        private string connectionString;

        /// <summary>
        /// Creates a new instance of <see cref="BranchQueueService"/>.
        /// </summary>
        public BranchQueueService(ILogService logService, ILogger<BranchQueueService> logger, IServiceProvider serviceProvider)
        {
            this.logService = logService;
            this.logger = logger;
            this.serviceProvider = serviceProvider;
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

            // Use .NET time and not database time, because we often use DigitalOcean and they have their timezone set to UTC by default.
            databaseConnection.AddParameter("now", DateTime.Now);
            var dataTable = await databaseConnection.GetAsync($@"SELECT * 
FROM {WiserTableNames.WiserBranchesQueue}
WHERE started_on IS NULL
AND start_on <= ?now
ORDER BY start_on ASC, id ASC");

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
                        throw new ArgumentOutOfRangeException(nameof(branchAction), branchAction);
                }
            }

            return new JObject
            {
                {"Results", results}
            };
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
            var error = "";
            var result = new JObject();

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

                error = "Trying to create a branch, but it either had invalid settings, or the database name was empty.";
                result.Add("ErrorMessage", error);
                result.Add("Success", false);
                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);
                return result;
            }

            // Make sure that the database doesn't exist yet.
            var branchDatabase = settings.DatabaseName;
            if (await databaseHelpersService.DatabaseExistsAsync(branchDatabase))
            {
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to create a branch, but a database with name '{branchDatabase}' already exists. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

                error = $"Trying to create a branch, but a database with name '{branchDatabase}' already exists.";
                result.Add("ErrorMessage", error);
                result.Add("Success", false);
                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);
                return result;
            }

            try
            {
                // Some variables we'll need a lot, for easier access.
                var connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);

                // Change connection string to one with a specific user for deleting a database.
                if (!String.IsNullOrWhiteSpace(branchQueue.UsernameForManagingBranches) && !String.IsNullOrWhiteSpace(branchQueue.PasswordForManagingBranches))
                {
                    connectionStringBuilder.UserID = branchQueue.UsernameForManagingBranches;
                    connectionStringBuilder.Password = branchQueue.PasswordForManagingBranches;
                    await databaseConnection.ChangeConnectionStringsAsync(connectionStringBuilder.ConnectionString, connectionStringBuilder.ConnectionString);
                }

                // Create the database in the same server/cluster. We already check if the database exists before this, so we can safely do this here.
                await databaseHelpersService.CreateDatabaseAsync(branchDatabase);

                var originalDatabase = connectionStringBuilder.Database;
                connectionStringBuilder.Database = branchDatabase;

                // Get all tables that don't start with an underscore (wiser tables never start with an underscore and we often use that for temporary or backup tables).
                // Handle the wiser_itemfile tables as last to ensure all other tables are copied first.
                var query = @$"SELECT *
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
) AS x";

                databaseConnection.AddParameter("currentSchema", originalDatabase);
                databaseConnection.AddParameter("newSchema", branchDatabase);
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

                // Create the tables in a new connection, because these cause implicit commits.
                await using (var mysqlConnection = new MySqlConnection(connectionStringBuilder.ConnectionString))
                {
                    await mysqlConnection.OpenAsync();
                    await using (var command = mysqlConnection.CreateCommand())
                    {
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

                            command.CommandText = $"CREATE TABLE `{branchDatabase.ToMySqlSafeValue(false)}`.`{tableName.ToMySqlSafeValue(false)}` LIKE `{originalDatabase.ToMySqlSafeValue(false)}`.`{tableName.ToMySqlSafeValue(false)}`";
                            await command.ExecuteNonQueryAsync();
                        }
                    }
                }

                await databaseConnection.BeginTransactionAsync();

                var allLinkTypes = await wiserItemsService.GetAllLinkTypeSettingsAsync();

                // Fill the tables with data.
                foreach (DataRow dataRow in dataTable.Rows)
                {
                    var tableName = dataRow.Field<string>("TABLE_NAME");

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

                            // If mode is nothing, skip everything of this entity type.
                            if (entity.Mode == CreateBranchEntityModes.Nothing)
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
                                        await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
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
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(entity.Mode), entity.Mode.ToString());
                            }

                            // Build a query to get all items of the current entity type and all items that are linked to those items.
                            var columnsClause = String.Join(", ", WiserTableDefinitions.TablesToUpdate.Single(t => t.Name == WiserTableNames.WiserItem).Columns.Select(c => $"{{0}}`{c.Name}`"));
                            var whereClause = whereClauseBuilder.ToString();
                            var queryBuilder = new StringBuilder($@"INSERT IGNORE INTO `{branchDatabase}`.`{tableName}`
({String.Format(columnsClause, "")})
(
    SELECT {String.Format(columnsClause, "item.")}
    FROM `{originalDatabase}`.`{tableName}` AS item
    {whereClause}
    {orderBy}
)
UNION ALL
(
    SELECT {String.Format(columnsClause, "linkedItem.")}
    FROM `{originalDatabase}`.`{tableName}` AS item
    JOIN `{originalDatabase}`.`{tableName}` AS linkedItem ON linkedItem.parent_item_id = item.id
    {whereClause}
    {orderBy}
)
");
                            var linkTypes = allLinkTypes.Where(t => String.Equals(t.DestinationEntityType, entity.EntityType, StringComparison.OrdinalIgnoreCase)).ToList();
                            if (!linkTypes.Any())
                            {
                                queryBuilder.AppendLine($@"UNION ALL
(
    SELECT {String.Format(columnsClause, "linkedItem.")}
    FROM `{originalDatabase}`.`{tableName}` AS item
    JOIN `{originalDatabase}`.`{WiserTableNames.WiserItemLink}` AS link ON link.destination_item_id = item.id
    JOIN `{originalDatabase}`.`{tableName}` AS linkedItem ON linkedItem.id = link.item_id
    {whereClause}
    {orderBy}
)");
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
                                    queryBuilder.AppendLine($@"UNION ALL
(
    SELECT {String.Format(columnsClause, "linkedItem.")}
    FROM `{originalDatabase}`.`{tableName}` AS item
    JOIN `{originalDatabase}`.`{linkTablePrefix}{WiserTableNames.WiserItemLink}` AS link ON link.destination_item_id = item.id AND link.type = ?linkType{i}
    JOIN `{originalDatabase}`.`{itemTablePrefix}{WiserTableNames.WiserItem}` AS linkedItem ON linkedItem.id = link.item_id AND linkedItem.entity_type = ?sourceEntityType{i}
    {whereClause}
    {orderBy}
)");
                                }
                            }

                            await databaseConnection.ExecuteAsync(queryBuilder.ToString());
                        }

                        continue;
                    }

                    if (tableName!.EndsWith(WiserTableNames.WiserItemDetail, StringComparison.OrdinalIgnoreCase))
                    {
                        // We order tables by table name, this means wiser_item always comes before wiser_itemdetail.
                        // So we can be sure that we already copied the items to the new branch and we can use the IDs of those items to copy the details of those items.
                        // This way, we don't need to create the entire WHERE statement again based on the entity settings, like we did above for wiser_item.
                        var prefix = tableName.Replace(WiserTableNames.WiserItemDetail, "");

                        // We need to get all columns of the wiser_itemdetail table like this, instead of using SELECT *,
                        // because they can have virtual columns and you can't manually insert values into those.
                        var table = WiserTableDefinitions.TablesToUpdate.Single(x => x.Name == WiserTableNames.WiserItemDetail);
                        var itemDetailColumns = table.Columns.Select(x => $"`{x.Name}`").ToList();
                        await databaseConnection.ExecuteAsync($@"INSERT INTO `{branchDatabase}`.`{tableName}` ({String.Join(", ", itemDetailColumns)})
SELECT {String.Join(", ", itemDetailColumns.Select(x => $"detail.{x}"))} FROM `{originalDatabase}`.`{tableName}` AS detail
JOIN `{branchDatabase}`.`{prefix}{WiserTableNames.WiserItem}` AS item ON item.id = detail.item_id");
                        continue;
                    }

                    if (tableName!.EndsWith(WiserTableNames.WiserItemFile, StringComparison.OrdinalIgnoreCase))
                    {
                        // The wiser_itemfile tables are handled last, this means wiser_item and wiser_itemlink always comes before wiser_itemfile.
                        // So we can be sure that we already copied the items to the new branch and we can use the IDs of those items to copy the details of those items.
                        // This way, we don't need to create the entire WHERE statement again based on the entity settings, like we did above for wiser_item.
                        var prefix = tableName.Replace(WiserTableNames.WiserItemFile, "");

                        if (await databaseHelpersService.TableExistsAsync($"{prefix}{WiserTableNames.WiserItem}"))
                        {
                            await databaseConnection.ExecuteAsync($@"INSERT INTO `{branchDatabase}`.`{tableName}` 
SELECT file.* FROM `{originalDatabase}`.`{tableName}` AS file
JOIN `{branchDatabase}`.`{prefix}{WiserTableNames.WiserItem}` AS item ON item.id = file.item_id");

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

                                        await databaseConnection.ExecuteAsync($@"INSERT IGNORE INTO `{branchDatabase}`.`{tableName}`
SELECT file.* FROM `{originalDatabase}`.`{tableName}` AS file
JOIN `{branchDatabase}`.`{linkPrefix}{WiserTableNames.WiserItemLink}` AS link ON link.id = file.itemlink_id");

                                        processedLinkTypes.Add(linkPrefix);
                                    }
                                }
                            }
                        }
                        else if (await databaseHelpersService.TableExistsAsync($"{prefix}{WiserTableNames.WiserItemLink}"))
                        {
                            // Copy all files that are on the links that are copied to the new branch from the given (prefixed) table.
                            await databaseConnection.ExecuteAsync($@"INSERT IGNORE INTO `{branchDatabase}`.`{tableName}`
SELECT file.* FROM `{originalDatabase}`.`{tableName}` AS file
JOIN `{branchDatabase}`.`{prefix}{WiserTableNames.WiserItemLink}` AS link ON link.id = file.itemlink_id");
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
                    query = @"SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = ?tableName
AND TABLE_SCHEMA = ?currentSchema
AND EXTRA NOT LIKE '%GENERATED'";
                    databaseConnection.AddParameter("tableName", tableName);
                    var columnsDataTable = await databaseConnection.GetAsync(query);
                    if (columnsDataTable.Rows.Count == 0)
                    {
                        continue;
                    }

                    var columns = columnsDataTable.Rows.Cast<DataRow>().Select(row => $"`{row.Field<string>("COLUMN_NAME")}`").ToList();

                    // For all other tables, always copy everything to the new branch.
                    query = $@"INSERT INTO `{branchDatabase}`.`{tableName}` ({String.Join(", ", columns)})
SELECT {String.Join(", ", columns)} FROM `{originalDatabase}`.`{tableName}`";
                    await databaseConnection.ExecuteAsync(query);
                }

                await databaseConnection.CommitTransactionAsync();

                // Add triggers to the new database, after inserting all data, so that the wiser_history table will still be empty.
                // We use wiser_history to later synchronise all changes to production, so it needs to be empty before the user starts to make changes in the new branch.
                query = @"SELECT 
    TRIGGER_NAME,
    EVENT_MANIPULATION,
    EVENT_OBJECT_TABLE,
	ACTION_STATEMENT,
	ACTION_ORIENTATION,
	ACTION_TIMING
FROM information_schema.TRIGGERS
WHERE TRIGGER_SCHEMA = ?currentSchema
AND EVENT_OBJECT_TABLE NOT LIKE '\_%'";
                dataTable = await databaseConnection.GetAsync(query);

                await using (var mysqlConnection = new MySqlConnection(connectionStringBuilder.ConnectionString))
                {
                    await mysqlConnection.OpenAsync();
                    await using (var command = mysqlConnection.CreateCommand())
                    {
                        foreach (DataRow dataRow in dataTable.Rows)
                        {
                            query = $@"CREATE TRIGGER `{dataRow.Field<string>("TRIGGER_NAME")}` {dataRow.Field<string>("ACTION_TIMING")} {dataRow.Field<string>("EVENT_MANIPULATION")} ON `{branchDatabase.ToMySqlSafeValue(false)}`.`{dataRow.Field<string>("EVENT_OBJECT_TABLE")}` FOR EACH {dataRow.Field<string>("ACTION_ORIENTATION")} {dataRow.Field<string>("ACTION_STATEMENT")}";
                            command.CommandText = query;
                            await command.ExecuteNonQueryAsync();
                        }

                        // Add stored procedures/functions to the new database.
                        query = @"SELECT
    ROUTINE_NAME,
    ROUTINE_TYPE,
    DEFINER
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = ?currentSchema
AND ROUTINE_NAME NOT LIKE '\_%'";
                        dataTable = await databaseConnection.GetAsync(query);
                        foreach (DataRow dataRow in dataTable.Rows)
                        {
                            var definer = dataRow.Field<string>("DEFINER");
                            var definerParts = definer.Split('@');
                            query = $"SHOW CREATE {dataRow.Field<string>("ROUTINE_TYPE")} `{originalDatabase.ToMySqlSafeValue(false)}`.`{dataRow.Field<string>("ROUTINE_NAME")}`";
                            var subDataTable = await databaseConnection.GetAsync(query);
                            query = subDataTable.Rows[0].Field<string>(2);

                            // Set the names and collation from the original and replace the definer with the current user, so that the stored procedure can be created by the current user.
                            query = $"SET NAMES {subDataTable.Rows[0].Field<string>(3)} COLLATE {subDataTable.Rows[0].Field<string>(4)}; {query.Replace($" DEFINER=`{definerParts[0]}`@`{definerParts[1]}`", " DEFINER=CURRENT_USER")}";
                            command.CommandText = query;
                            await command.ExecuteNonQueryAsync();
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                error = exception.ToString();

                // Rollback transaction if started
                await databaseConnection.RollbackTransactionAsync(false);
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Failed to create the branch '{settings.DatabaseName}'. Error: {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

                // Save the error in the queue and set the finished on datetime to now.
                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);

                // Drop the new database it something went wrong, so that we can start over again later.
                // We can safely do this, because this method will return an error if the database already exists,
                // so we can be sure that this database was created here and we can drop it again it something went wrong.
                try
                {
                    if (await databaseHelpersService.DatabaseExistsAsync(branchDatabase))
                    {
                        await databaseHelpersService.DropDatabaseAsync(branchDatabase);
                    }
                }
                catch (Exception innerException)
                {
                    await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Failed to drop new branch database '{settings.DatabaseName}', after getting an error while trying to fill it with data. Error: {innerException}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                }

                error = exception.ToString();
            }

            // Set the finish time to the current datetime, so that we can see how long it took.
            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, CreateBranchSubject, CreateBranchTemplate);
            result.Add("ErrorMessage", error);
            result.Add("Success", String.IsNullOrWhiteSpace(error));
            return result;
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
                {"Errors", errors}
            };

            // Set the start date to the current datetime.
            var startDate = DateTime.Now;
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var queueId = dataRowWithSettings.Field<int>("id");
            databaseConnection.AddParameter("queueId", queueId);
            databaseConnection.AddParameter("now", startDate);
            await databaseConnection.ExecuteAsync($"UPDATE {WiserTableNames.WiserBranchesQueue} SET started_on = ?now WHERE id = ?queueId");

            // Validate the settings.
            var settings = JsonConvert.DeserializeObject<MergeBranchSettingsModel>(dataRowWithSettings.Field<string>("data") ?? "{}");
            if (settings is not {Id: > 0} || String.IsNullOrWhiteSpace(settings.DatabaseName))
            {
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to merge a branch, but it either had invalid settings, or the branch ID was empty, or the database name was empty. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                errors.Add($"Trying to merge a branch, but it either had invalid settings, or the branch ID was empty, or the database name was empty. Queue ID was: {queueId}");


                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, errors, stopwatch, startDate, branchQueue.MergedBranchTemplateId, MergeBranchSubject, MergeBranchTemplate);
                return result;
            }

            // Store database names in variables for later use and create connection string for the branch database.
            var connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString);
            var originalDatabase = connectionStringBuilder.Database;
            var branchDatabase = settings.DatabaseName;
            connectionStringBuilder.Database = branchDatabase;

            // Create and open connections to both databases and start transactions.
            var productionConnection = new MySqlConnection(connectionString);
            var branchConnection = new MySqlConnection(connectionStringBuilder.ConnectionString);
            await productionConnection.OpenAsync();
            await branchConnection.OpenAsync();
            var productionTransaction = await productionConnection.BeginTransactionAsync();
            var branchTransaction = await branchConnection.BeginTransactionAsync();

            // We have our own dictionary with SQL parameters, so that we can reuse them easier and add them easily all at once to every command we create.
            var sqlParameters = new Dictionary<string, object>();

            try
            {
                // Create the wiser_id_mappings table, in the selected branch, if it doesn't exist yet.
                // We need it to map IDs of the selected environment to IDs of the production environment, because they are not always the same.
                await databaseHelpersService.CheckAndUpdateTablesAsync(new List<string> {WiserTableNames.WiserIdMappings}, branchDatabase);

                // Get all history since last synchronisation.
                var dataTable = new DataTable();
                await using (var environmentCommand = branchConnection.CreateCommand())
                {
                    environmentCommand.CommandText = $"SELECT * FROM `{WiserTableNames.WiserHistory}` ORDER BY id ASC";
                    using var environmentAdapter = new MySqlDataAdapter(environmentCommand);
                    await environmentAdapter.FillAsync(dataTable);
                }

                // Srt saveHistory and username parameters for all queries.
                var queryPrefix = @"SET @saveHistory = TRUE; SET @_username = ?username; ";
                var username = $"{dataRowWithSettings.Field<string>("added_by")} (Sync from {branchDatabase})";
                if (username.Length > 50)
                {
                    username = dataRowWithSettings.Field<string>("added_by");
                }

                sqlParameters.Add("username", username);

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
                    if (isWiserItemChange && originalItemId > 0 && !tablesToLock.Contains(wiserItemTableName))
                    {
                        tablesToLock.Add(wiserItemTableName);
                        tablesToLock.Add($"{wiserItemTableName}{WiserTableNames.ArchiveSuffix}");
                    }

                    var action = dataRow.Field<string>("action").ToUpperInvariant();
                    BranchesHelpers.TrackObjectAction(objectsCreatedInBranch, action, originalItemId, tableName);
                }

                tablesToLock = tablesToLock.Distinct().ToList();

                // Add tables from wiser_id_mappings to tables to lock.
                await using (var command = branchConnection.CreateCommand())
                {
                    command.CommandText = $"SELECT DISTINCT table_name FROM `{WiserTableNames.WiserIdMappings}`";
                    var mappingDataTable = new DataTable();
                    using var adapter = new MySqlDataAdapter(command);
                    await adapter.FillAsync(mappingDataTable);
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

                // This is to cache the entity types for all changed items, so that we don't have to execute a query for every changed detail of the same item.
                var entityTypes = new Dictionary<ulong, string>();

                // This is to map one item ID to another. This is needed because when someone creates a new item in the other environment, that ID could already exist in the production environment.
                // So we need to map the ID that is saved in wiser_history to the new ID of the item that we create in the production environment.
                var idMapping = new Dictionary<string, Dictionary<ulong, ulong>>();
                await using (var environmentCommand = branchConnection.CreateCommand())
                {
                    environmentCommand.CommandText = $@"SELECT table_name, our_id, production_id FROM `{WiserTableNames.WiserIdMappings}`";
                    using var environmentAdapter = new MySqlDataAdapter(environmentCommand);

                    var idMappingDatatable = new DataTable();
                    await environmentAdapter.FillAsync(idMappingDatatable);
                    foreach (DataRow dataRow in idMappingDatatable.Rows)
                    {
                        var tableName = dataRow.Field<string>("table_name");
                        var ourId = dataRow.Field<ulong>("our_id");
                        var productionId = dataRow.Field<ulong>("production_id");

                        if (!idMapping.ContainsKey(tableName!))
                        {
                            idMapping.Add(tableName, new Dictionary<ulong, ulong>());
                        }

                        idMapping[tableName][ourId] = productionId;
                    }
                }

                // Start synchronising all history items one by one.
                var historyItemsSynchronised = new List<ulong>();
                foreach (DataRow dataRow in dataTable.Rows)
                {
                    var historyId = Convert.ToUInt64(dataRow["id"]);

                    // If this history item has a conflict and it's not accepted, skip and delete this history record.
                    var conflict = settings.ConflictSettings.SingleOrDefault(setting => setting.Id == historyId);
                    if (conflict != null && conflict.AcceptChange.HasValue && !conflict.AcceptChange.Value)
                    {
                        historyItemsSynchronised.Add(historyId);
                        continue;
                    }

                    var action = dataRow.Field<string>("action").ToUpperInvariant();
                    var tableName = dataRow.Field<string>("tablename") ?? "";
                    var originalObjectId = Convert.ToUInt64(dataRow["item_id"]);
                    var objectId = originalObjectId;
                    var originalItemId = originalObjectId;
                    var itemId = originalObjectId;
                    var field = dataRow.Field<string>("field");
                    var oldValue = dataRow.Field<string>("oldvalue");
                    var newValue = dataRow.Field<string>("newvalue");
                    var languageCode = dataRow.Field<string>("language_code") ?? "";
                    var groupName = dataRow.Field<string>("groupname") ?? "";
                    ulong? linkId = null;
                    ulong? originalLinkId;
                    ulong? originalFileId = null;
                    ulong? fileId = null;
                    var entityType = "";
                    int? linkType = null;
                    int? linkOrdering = null;

                    // Variables for item link changes.
                    var destinationItemId = 0UL;
                    ulong? oldItemId = null;
                    ulong? oldDestinationItemId = null;
                    (string SourceType, string SourceTablePrefix, string DestinationType, string DestinationTablePrefix)? linkData = null;

                    var objectCreatedInBranch = objectsCreatedInBranch.FirstOrDefault(i => i.ObjectId == originalObjectId && String.Equals(i.TableName, tableName, StringComparison.OrdinalIgnoreCase));
                    if (objectCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false})
                    {
                        // This item was created and then deleted in the branch, so we don't need to do anything.
                        historyItemsSynchronised.Add(historyId);
                        continue;
                    }

                    try
                    {
                        // Make sure we have the correct item ID. For some actions the item id is saved in a different column.
                        switch (action)
                        {
                            case "REMOVE_LINK":
                            {
                                destinationItemId = itemId;
                                itemId = Convert.ToUInt64(oldValue);
                                originalItemId = itemId;
                                linkType = Int32.Parse(field);

                                break;
                            }
                            case "UPDATE_ITEMLINKDETAIL":
                            case "CHANGE_LINK":
                            {
                                linkId = itemId;
                                originalLinkId = linkId;

                                // When a link has been changed, it's possible that the ID of one of the items is changed.
                                // It's also possible that this is a new link that the production database didn't have yet (and so the ID of the link will most likely be different).
                                // Therefor we need to find the original item and destination IDs, so that we can use those to update the link in the production database.
                                sqlParameters["linkId"] = itemId;

                                await using (var branchCommand = branchConnection.CreateCommand())
                                {
                                    AddParametersToCommand(sqlParameters, branchCommand);

                                    // Replace wiser_itemlinkdetail with wiser_itemlink because we need to get the source and destination from [prefix]wiser_itemlink, even if this is an update for [prefix]wiser_itemlinkdetail.
                                    branchCommand.CommandText = $"SELECT type, item_id, destination_item_id FROM `{tableName.ReplaceCaseInsensitive(WiserTableNames.WiserItemLinkDetail, WiserTableNames.WiserItemLink)}` WHERE id = ?linkId";
                                    var linkDataTable = new DataTable();
                                    using var branchAdapter = new MySqlDataAdapter(branchCommand);
                                    await branchAdapter.FillAsync(linkDataTable);
                                    if (linkDataTable.Rows.Count == 0)
                                    {
                                        branchCommand.CommandText = $"SELECT type, item_id, destination_item_id FROM `{tableName.ReplaceCaseInsensitive(WiserTableNames.WiserItemLinkDetail, WiserTableNames.WiserItemLink)}{WiserTableNames.ArchiveSuffix}` WHERE id = ?linkId";
                                        await branchAdapter.FillAsync(linkDataTable);
                                        if (linkDataTable.Rows.Count == 0)
                                        {
                                            // This should never happen, but just in case the ID somehow doesn't exist anymore, log a warning and continue on to the next item.
                                            await logService.LogWarning(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Could not find link with id '{itemId}' in database '{branchDatabase}'. Skipping this history record in synchronisation to production.", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                            historyItemsSynchronised.Add(historyId);
                                            continue;
                                        }
                                    }

                                    itemId = Convert.ToUInt64(linkDataTable.Rows[0]["item_id"]);
                                    originalItemId = itemId;
                                    destinationItemId = Convert.ToUInt64(linkDataTable.Rows[0]["destination_item_id"]);
                                    linkType = Convert.ToInt32(linkDataTable.Rows[0]["type"]);
                                }

                                switch (field)
                                {
                                    case "destination_item_id":
                                        oldDestinationItemId = Convert.ToUInt64(oldValue);
                                        destinationItemId = Convert.ToUInt64(newValue);
                                        oldItemId = itemId;
                                        break;
                                    case "item_id":
                                        oldItemId = Convert.ToUInt64(oldValue);
                                        itemId = Convert.ToUInt64(newValue);
                                        originalItemId = itemId;
                                        oldDestinationItemId = destinationItemId;
                                        break;
                                }

                                break;
                            }
                            case "ADD_LINK":
                            {
                                destinationItemId = itemId;
                                itemId = Convert.ToUInt64(newValue);
                                originalItemId = itemId;

                                var split = field.Split(',');
                                linkType = Int32.Parse(split[0]);
                                linkOrdering = split.Length > 1 ? Int32.Parse(split[1]) : 0;

                                break;
                            }
                            case "ADD_FILE":
                            case "DELETE_FILE":
                            {
                                fileId = itemId;
                                originalFileId = fileId;
                                itemId = String.Equals(oldValue, "item_id", StringComparison.OrdinalIgnoreCase) ? UInt64.Parse(newValue) : 0;
                                originalItemId = itemId;
                                linkId = String.Equals(oldValue, "itemlink_id", StringComparison.OrdinalIgnoreCase) ? UInt64.Parse(newValue) : 0;
                                originalLinkId = linkId;

                                if (linkId > 0)
                                {
                                    (itemId, destinationItemId, linkType) = await GetLinkDataAsync(linkId, sqlParameters, tableName, branchConnection, historyItemsSynchronised, historyId);
                                    if (itemId == 0)
                                    {
                                        historyItemsSynchronised.Add(historyId);
                                        continue;
                                    }
                                }

                                break;
                            }
                            case "UPDATE_FILE":
                            {
                                fileId = itemId;
                                originalFileId = fileId;
                                sqlParameters["fileId"] = fileId;

                                await using var branchCommand = branchConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, branchCommand);
                                branchCommand.CommandText = $@"SELECT item_id, itemlink_id FROM `{tableName}` WHERE id = ?fileId
UNION ALL
SELECT item_id, itemlink_id FROM `{tableName}{WiserTableNames.ArchiveSuffix}` WHERE id = ?fileId
LIMIT 1";
                                var fileDataTable = new DataTable();
                                using var adapter = new MySqlDataAdapter(branchCommand);
                                await adapter.FillAsync(fileDataTable);
                                if (fileDataTable.Rows.Count == 0)
                                {
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                itemId = Convert.ToUInt64(fileDataTable.Rows[0]["item_id"]);
                                originalItemId = itemId;
                                linkId = Convert.ToUInt64(fileDataTable.Rows[0]["itemlink_id"]);
                                originalLinkId = linkId;

                                if (linkId > 0)
                                {
                                    (itemId, destinationItemId, linkType) = await GetLinkDataAsync(linkId, sqlParameters, tableName, branchConnection, historyItemsSynchronised, historyId);
                                    if (itemId == 0)
                                    {
                                        historyItemsSynchronised.Add(historyId);
                                        continue;
                                    }
                                }

                                break;
                            }
                            case "DELETE_ITEM":
                            case "UNDELETE_ITEM":
                            {
                                entityType = field;
                                break;
                            }
                        }

                        // Did we map the item ID to something else? Then use that new ID.
                        var originalDestinationItemId = destinationItemId;
                        itemId = GetMappedId(tableName, idMapping, itemId).Value;
                        destinationItemId = GetMappedId(tableName, idMapping, destinationItemId).Value;
                        oldItemId = GetMappedId(tableName, idMapping, oldItemId);
                        oldDestinationItemId = GetMappedId(tableName, idMapping, oldDestinationItemId);
                        linkId = GetMappedId(tableName, idMapping, linkId);
                        fileId = GetMappedId(tableName, idMapping, fileId);
                        objectId = GetMappedId(tableName, idMapping, objectId) ?? 0;
                        var linkSourceItemCreatedInBranch = objectsCreatedInBranch.FirstOrDefault(i => i.ObjectId == originalItemId && String.Equals(i.TableName, $"{linkData?.SourceTablePrefix}{WiserTableNames.WiserItem}", StringComparison.OrdinalIgnoreCase));
                        var linkDestinationItemCreatedInBranch = objectsCreatedInBranch.FirstOrDefault(i => i.ObjectId == originalDestinationItemId && String.Equals(i.TableName, $"{linkData?.DestinationTablePrefix}{WiserTableNames.WiserItem}", StringComparison.OrdinalIgnoreCase));

                        // Figure out the entity type of the item that was updated, so that we can check if we need to do anything with it.
                        // We don't want to synchronise certain entity types, such as users, relations and baskets.
                        if (entityTypes.TryGetValue(itemId, out var type))
                        {
                            entityType = type;
                        }
                        else if (String.IsNullOrWhiteSpace(entityType))
                        {
                            if (action is "ADD_LINK" or "CHANGE_LINK" or "REMOVE_LINK" || (action is "ADD_FILE" or "UPDATE_FILE" or "DELETE_FILE" && linkId > 0 && linkType is > 0))
                            {
                                // Unlock the tables temporarily so that we can call GetEntityTypesOfLinkAsync, which calls wiserItemsService.GetTablePrefixForEntityAsync, since that method doesn't use our custom database connection.
                                await using var productionCommand = productionConnection.CreateCommand();
                                productionCommand.CommandText = "UNLOCK TABLES";
                                await productionCommand.ExecuteNonQueryAsync();

                                await using var branchCommand = branchConnection.CreateCommand();
                                branchCommand.CommandText = "UNLOCK TABLES";
                                await branchCommand.ExecuteNonQueryAsync();
                                linkData = await GetEntityTypesOfLinkAsync(itemId, destinationItemId, linkType.Value, branchConnection, wiserItemsService);
                                if (linkData.HasValue)
                                {
                                    entityType = linkData.Value.SourceType;
                                    entityTypes.Add(itemId, entityType);

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
                                }

                                // Lock the tables again when we're done.
                                await LockTablesAsync(productionConnection, tablesToLock, false);
                                await LockTablesAsync(branchConnection, tablesToLock, true);

                                // If we couldn't find any link data, then most likely one of the items doesn't exist anymore, so skip this history record.
                                // The other reason could be that the link type is not configured (correctly), in that case we also can't do anything, so skip it as well.
                                if (!linkData.HasValue)
                                {
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }
                            }
                            else
                            {
                                // Check if this item is saved in a dedicated table with a certain prefix.
                                var (tablePrefix, isWiserItemChange) = BranchesHelpers.GetTablePrefix(tableName, originalItemId);

                                if (isWiserItemChange && originalItemId > 0)
                                {
                                    sqlParameters["itemId"] = originalItemId;
                                    var itemDataTable = new DataTable();
                                    await using var environmentCommand = branchConnection.CreateCommand();
                                    AddParametersToCommand(sqlParameters, environmentCommand);
                                    environmentCommand.CommandText = $"SELECT entity_type FROM `{tablePrefix}{WiserTableNames.WiserItem}` WHERE id = ?itemId";
                                    using var environmentAdapter = new MySqlDataAdapter(environmentCommand);
                                    await environmentAdapter.FillAsync(itemDataTable);
                                    if (itemDataTable.Rows.Count == 0)
                                    {
                                        // If item doesn't exist, check the archive table, it might have been deleted.
                                        environmentCommand.CommandText = $"SELECT entity_type FROM `{tablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix}` WHERE id = ?itemId";
                                        await environmentAdapter.FillAsync(itemDataTable);
                                        if (itemDataTable.Rows.Count == 0)
                                        {
                                            await logService.LogWarning(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Could not find item with ID '{originalItemId}', so skipping it...", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                            historyItemsSynchronised.Add(historyId);
                                            continue;
                                        }
                                    }

                                    entityType = itemDataTable.Rows[0].Field<string>("entity_type");
                                    entityTypes.Add(itemId, entityType);
                                }
                            }
                        }

                        var entityTypeMergeSettings = settings.Entities.SingleOrDefault(e => String.Equals(e.Type, entityType, StringComparison.OrdinalIgnoreCase)) ?? new EntityMergeSettingsModel();
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

                        // Update the item in the production environment.
                        switch (action)
                        {
                            case "CREATE_ITEM":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Create)
                                {
                                    continue;
                                }

                                if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(originalItemId))
                                {
                                    // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                var newItemId = await GenerateNewIdAsync(tableName, productionConnection, branchConnection);
                                sqlParameters["newId"] = newItemId;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
INSERT INTO `{tableName}` (id, entity_type) VALUES (?newId, '')";
                                await productionCommand.ExecuteNonQueryAsync();

                                // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                                await AddIdMappingAsync(idMapping, tableName, originalItemId, newItemId, branchConnection);

                                break;
                            }
                            case "UPDATE_ITEM" when tableName.EndsWith(WiserTableNames.WiserItemDetail, StringComparison.OrdinalIgnoreCase):
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                sqlParameters["itemId"] = itemId;
                                sqlParameters["key"] = field;
                                sqlParameters["languageCode"] = languageCode;
                                sqlParameters["groupName"] = groupName;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = queryPrefix;
                                if (String.IsNullOrWhiteSpace(newValue))
                                {
                                    productionCommand.CommandText += $@"DELETE FROM `{tableName}`
WHERE item_id = ?itemId
AND `key` = ?key
AND language_code = ?languageCode
AND groupname = ?groupName";
                                }
                                else
                                {
                                    var useLongValue = newValue.Length > 1000;
                                    sqlParameters["value"] = useLongValue ? "" : newValue;
                                    sqlParameters["longValue"] = useLongValue ? newValue : "";

                                    AddParametersToCommand(sqlParameters, productionCommand);
                                    productionCommand.CommandText += $@"INSERT INTO `{tableName}` (language_code, item_id, groupname, `key`, value, long_value)
VALUES (?languageCode, ?itemId, ?groupName, ?key, ?value, ?longValue)
ON DUPLICATE KEY UPDATE groupname = VALUES(groupname), value = VALUES(value), long_value = VALUES(long_value)";
                                }

                                await productionCommand.ExecuteNonQueryAsync();

                                break;
                            }
                            case "UPDATE_ITEM" when tableName.EndsWith(WiserTableNames.WiserItem, StringComparison.OrdinalIgnoreCase):
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                sqlParameters["itemId"] = itemId;
                                sqlParameters["newValue"] = newValue;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
UPDATE `{tableName}` 
SET `{field.ToMySqlSafeValue(false)}` = ?newValue
WHERE id = ?itemId";
                                await productionCommand.ExecuteNonQueryAsync();

                                break;
                            }
                            case "DELETE_ITEM":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Delete)
                                {
                                    continue;
                                }

                                // Unlock the tables temporarily so that we can call wiserItemsService.DeleteAsync, since that method doesn't use our custom database connection.
                                await using var productionCommand = productionConnection.CreateCommand();
                                productionCommand.CommandText = "UNLOCK TABLES";
                                await productionCommand.ExecuteNonQueryAsync();
                                await wiserItemsService.DeleteAsync(itemId, entityType: entityType, skipPermissionsCheck: true, username: username);

                                // Lock the tables again when we're done with deleting.
                                await LockTablesAsync(productionConnection, tablesToLock, false);

                                break;
                            }
                            case "UNDELETE_ITEM":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Delete)
                                {
                                    continue;
                                }

                                // Unlock the tables temporarily so that we can call wiserItemsService.DeleteAsync, since that method doesn't use our custom database connection.
                                await using var productionCommand = productionConnection.CreateCommand();
                                productionCommand.CommandText = "UNLOCK TABLES";
                                await productionCommand.ExecuteNonQueryAsync();
                                await wiserItemsService.DeleteAsync(itemId, entityType: entityType, skipPermissionsCheck: true, username: username, undelete: true);

                                // Lock the tables again when we're done with deleting.
                                await LockTablesAsync(productionConnection, tablesToLock, false);

                                break;
                            }
                            case "ADD_LINK":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                if (linkSourceItemCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false} || linkDestinationItemCreatedInBranch  is {AlsoDeleted: true, AlsoUndeleted: false})
                                {
                                    // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                sqlParameters["itemId"] = itemId;
                                sqlParameters["originalItemId"] = originalItemId;
                                sqlParameters["ordering"] = linkOrdering;
                                sqlParameters["destinationItemId"] = destinationItemId;
                                sqlParameters["originalDestinationItemId"] = originalDestinationItemId;
                                sqlParameters["type"] = linkType ?? 0;

                                // Get the original link ID, so we can map it to the new one.
                                await using (var environmentCommand = branchConnection.CreateCommand())
                                {
                                    AddParametersToCommand(sqlParameters, environmentCommand);
                                    environmentCommand.CommandText = $"SELECT id FROM `{tableName}` WHERE item_id = ?originalItemId AND destination_item_id = ?originalDestinationItemId AND type = ?type";
                                    var getLinkIdDataTable = new DataTable();
                                    using var environmentAdapter = new MySqlDataAdapter(environmentCommand);
                                    await environmentAdapter.FillAsync(getLinkIdDataTable);
                                    if (getLinkIdDataTable.Rows.Count == 0)
                                    {
                                        await logService.LogWarning(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Could not find link ID with itemId = {originalItemId}, destinationItemId = {originalDestinationItemId} and type = {linkType}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                                        historyItemsSynchronised.Add(historyId);
                                        continue;
                                    }

                                    originalLinkId = Convert.ToUInt64(getLinkIdDataTable.Rows[0]["id"]);
                                    linkId = await GenerateNewIdAsync(tableName, productionConnection, branchConnection);
                                }

                                if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(originalLinkId.Value))
                                {
                                    // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                sqlParameters["newId"] = linkId;
                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
INSERT IGNORE INTO `{tableName}` (id, item_id, destination_item_id, ordering, type)
VALUES (?newId, ?itemId, ?destinationItemId, ?ordering, ?type);";
                                await productionCommand.ExecuteNonQueryAsync();

                                // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                                await AddIdMappingAsync(idMapping, tableName, originalLinkId.Value, linkId.Value, branchConnection);

                                break;
                            }
                            case "CHANGE_LINK":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                if (linkSourceItemCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false} || linkDestinationItemCreatedInBranch  is {AlsoDeleted: true, AlsoUndeleted: false})
                                {
                                    // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                sqlParameters["oldItemId"] = oldItemId;
                                sqlParameters["oldDestinationItemId"] = oldDestinationItemId;
                                sqlParameters["newValue"] = newValue;
                                sqlParameters["type"] = linkType ?? 0;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
UPDATE `{tableName}` 
SET `{field.ToMySqlSafeValue(false)}` = ?newValue
WHERE item_id = ?oldItemId
AND destination_item_id = ?oldDestinationItemId
AND type = ?type";
                                await productionCommand.ExecuteNonQueryAsync();
                                break;
                            }
                            case "REMOVE_LINK":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                if (linkSourceItemCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false} || linkDestinationItemCreatedInBranch  is {AlsoDeleted: true, AlsoUndeleted: false})
                                {
                                    // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                sqlParameters["oldItemId"] = oldItemId;
                                sqlParameters["oldDestinationItemId"] = oldDestinationItemId;
                                sqlParameters["type"] = linkType ?? 0;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
DELETE FROM `{tableName}`
WHERE item_id = ?oldItemId
AND destination_item_id = ?oldDestinationItemId
AND type = ?type";
                                await productionCommand.ExecuteNonQueryAsync();
                                break;
                            }
                            case "UPDATE_ITEMLINKDETAIL":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                if (linkSourceItemCreatedInBranch is {AlsoDeleted: true, AlsoUndeleted: false} || linkDestinationItemCreatedInBranch  is {AlsoDeleted: true, AlsoUndeleted: false})
                                {
                                    // One of the items of the link was created and then deleted in the branch, so we don't need to do anything.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                sqlParameters["linkId"] = linkId;
                                sqlParameters["key"] = field;
                                sqlParameters["languageCode"] = languageCode;
                                sqlParameters["groupName"] = groupName;

                                await using var productionCommand = productionConnection.CreateCommand();
                                productionCommand.CommandText = queryPrefix;
                                if (String.IsNullOrWhiteSpace(newValue))
                                {
                                    productionCommand.CommandText += $@"DELETE FROM `{tableName}`
WHERE itemlink_id = ?linkId
AND `key` = ?key
AND language_code = ?languageCode
AND groupname = ?groupName";
                                }
                                else
                                {
                                    var useLongValue = newValue.Length > 1000;
                                    sqlParameters["value"] = useLongValue ? "" : newValue;
                                    sqlParameters["longValue"] = useLongValue ? newValue : "";

                                    productionCommand.CommandText += $@"INSERT INTO `{tableName}` (language_code, itemlink_id, groupname, `key`, value, long_value)
VALUES (?languageCode, ?linkId, ?groupName, ?key, ?value, ?longValue)
ON DUPLICATE KEY UPDATE groupname = VALUES(groupname), value = VALUES(value), long_value = VALUES(long_value)";
                                }

                                AddParametersToCommand(sqlParameters, productionCommand);
                                await productionCommand.ExecuteNonQueryAsync();

                                break;
                            }
                            case "ADD_FILE":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(originalObjectId))
                                {
                                    // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                // oldValue contains either "item_id" or "itemlink_id", to indicate which of these columns is used for the ID that is saved in newValue.
                                var newFileId = await GenerateNewIdAsync(tableName, productionConnection, branchConnection);
                                sqlParameters["fileItemId"] = newValue;
                                sqlParameters["newId"] = newFileId;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
INSERT INTO `{tableName}` (id, `{oldValue.ToMySqlSafeValue(false)}`) 
VALUES (?newId, ?fileItemId)";
                                await productionCommand.ExecuteNonQueryAsync();

                                // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                                await AddIdMappingAsync(idMapping, tableName, originalObjectId, newFileId, branchConnection);

                                break;
                            }
                            case "UPDATE_FILE":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                sqlParameters["fileId"] = fileId;
                                sqlParameters["originalFileId"] = originalFileId;

                                if (String.Equals(field, "content_length", StringComparison.OrdinalIgnoreCase))
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
                                    productionCommand.CommandText = $@"{queryPrefix}
UPDATE `{tableName}`
SET content = ?contents
WHERE id = ?fileId";
                                    await productionCommand.ExecuteNonQueryAsync();
                                }
                                else
                                {
                                    sqlParameters["newValue"] = newValue;

                                    await using var productionCommand = productionConnection.CreateCommand();
                                    AddParametersToCommand(sqlParameters, productionCommand);
                                    productionCommand.CommandText = $@"{queryPrefix}
UPDATE `{tableName}` 
SET `{field.ToMySqlSafeValue(false)}` = ?newValue
WHERE id = ?fileId";
                                    await productionCommand.ExecuteNonQueryAsync();
                                }

                                break;
                            }
                            case "DELETE_FILE":
                            {
                                // Check if the user requested this change to be synchronised.
                                if (!entityTypeMergeSettings.Update)
                                {
                                    continue;
                                }

                                sqlParameters["itemId"] = newValue;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
DELETE FROM `{tableName}`
WHERE `{oldValue.ToMySqlSafeValue(false)}` = ?itemId";
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
                            {
                                // Check if the user requested this change to be synchronised.
                                switch (tableName)
                                {
                                    case WiserTableNames.WiserEntity when entityMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserEntityProperty when entityPropertyMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserQuery when queryMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserModule when moduleMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserDataSelector when dataSelectorMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserPermission when permissionMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserUserRoles when userRoleMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserFieldTemplates when fieldTemplatesMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserLink when linkMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserApiConnection when apiConnectionMergeSettings is not {Create: true}:
                                        continue;
                                    case WiserTableNames.WiserRoles when roleMergeSettings is not {Create: true}:
                                        continue;
                                }

                                if (idMapping.TryGetValue(tableName, out var mapping) && mapping.ContainsKey(originalObjectId))
                                {
                                    // This item was already created in an earlier merge, but somehow the history of that wasn't deleted, so skip it now.
                                    historyItemsSynchronised.Add(historyId);
                                    continue;
                                }

                                var newEntityId = await GenerateNewIdAsync(tableName, productionConnection, branchConnection);
                                sqlParameters["newId"] = newEntityId;
                                sqlParameters["guid"] = Guid.NewGuid().ToString("N");

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);

                                if (tableName.Equals(WiserTableNames.WiserEntity, StringComparison.OrdinalIgnoreCase))
                                {
                                    productionCommand.CommandText = $@"{queryPrefix}
INSERT INTO `{tableName}` (id, `name`) 
VALUES (?newId, '')";
                                }
                                else if (tableName.Equals(WiserTableNames.WiserEntityProperty, StringComparison.OrdinalIgnoreCase))
                                {
                                    // The table wiser_entityproperty has a unique index on (entity_name, property_name), so we need to temporarily generate a unique property name, to prevent duplicate index errors when multiple properties are added in a row.
                                    productionCommand.CommandText = $@"{queryPrefix}
INSERT INTO `{tableName}` (id, `entity_name`, `property_name`) 
VALUES (?newId, 'temp_wts', ?guid)";
                                }
                                else
                                {
                                    productionCommand.CommandText = $@"{queryPrefix}
INSERT INTO `{tableName}` (id) 
VALUES (?newId)";
                                }

                                await productionCommand.ExecuteNonQueryAsync();

                                // Map the item ID from wiser_history to the ID of the newly created item, locally and in database.
                                await AddIdMappingAsync(idMapping, tableName, originalObjectId, newEntityId, branchConnection);

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
                            {
                                // Check if the user requested this change to be synchronised.
                                switch (tableName)
                                {
                                    case WiserTableNames.WiserEntity when entityMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserEntityProperty when entityPropertyMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserQuery when queryMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserModule when moduleMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserDataSelector when dataSelectorMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserPermission when permissionMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserUserRoles when userRoleMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserFieldTemplates when fieldTemplatesMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserLink when linkMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserApiConnection when apiConnectionMergeSettings is not {Update: true}:
                                        continue;
                                    case WiserTableNames.WiserRoles when roleMergeSettings is not {Update: true}:
                                        continue;
                                }

                                sqlParameters["id"] = objectId;
                                sqlParameters["newValue"] = newValue;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
UPDATE IGNORE `{tableName}` 
SET `{field.ToMySqlSafeValue(false)}` = ?newValue
WHERE id = ?id";
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
                            {
                                // Check if the user requested this change to be synchronised.
                                switch (tableName)
                                {
                                    case WiserTableNames.WiserEntity when entityMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserEntityProperty when entityPropertyMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserQuery when queryMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserModule when moduleMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserDataSelector when dataSelectorMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserPermission when permissionMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserUserRoles when userRoleMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserFieldTemplates when fieldTemplatesMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserLink when linkMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserApiConnection when apiConnectionMergeSettings is not {Delete: true}:
                                        continue;
                                    case WiserTableNames.WiserRoles when roleMergeSettings is not {Delete: true}:
                                        continue;
                                }

                                sqlParameters["id"] = objectId;

                                await using var productionCommand = productionConnection.CreateCommand();
                                AddParametersToCommand(sqlParameters, productionCommand);
                                productionCommand.CommandText = $@"{queryPrefix}
DELETE FROM `{tableName}`
WHERE `id` = ?id";
                                await productionCommand.ExecuteNonQueryAsync();

                                break;
                            }
                            default:
                                throw new ArgumentOutOfRangeException(nameof(action), action, $"Unsupported action for history synchronisation: '{action}'");
                        }

                        successfulChanges++;
                        historyItemsSynchronised.Add(historyId);
                    }
                    catch (Exception exception)
                    {
                        await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"An error occurred while trying to synchronise history ID '{historyId}' from '{branchDatabase}' to '{originalDatabase}': {exception}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);
                        errors.Add($"Het is niet gelukt om de wijziging '{action}' voor item '{originalItemId}' over te zetten. De fout was: {exception.Message}");
                    }
                }

                try
                {
                    // Clear wiser_history in the selected environment, so that next time we can just sync all changes again.
                    if (historyItemsSynchronised.Any())
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
                        connectionStringBuilder.UserID = branchQueue.UsernameForManagingBranches;
                        connectionStringBuilder.Password = branchQueue.PasswordForManagingBranches;
                        await databaseConnection.ChangeConnectionStringsAsync(connectionStringBuilder.ConnectionString, connectionStringBuilder.ConnectionString);
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

            return result;
        }

        private static async Task<(ulong SourceItemId, ulong DestinationItemId, int LinkType)> GetLinkDataAsync(ulong? linkId, Dictionary<string, object> sqlParameters, string tableName, MySqlConnection branchConnection, List<ulong> historyItemsSynchronised, ulong historyId)
        {
            sqlParameters["linkId"] = linkId;
            var itemLinkTableName = tableName.ReplaceCaseInsensitive(WiserTableNames.WiserItemFile, WiserTableNames.WiserItemLink);
            await using var branchCommand = branchConnection.CreateCommand();
            AddParametersToCommand(sqlParameters, branchCommand);
            branchCommand.CommandText = $"SELECT item_id, destination_item_id, type FROM `{itemLinkTableName}` WHERE id = ?linkId LIMIT 1";
            var fileDataTable = new DataTable();
            using var adapter = new MySqlDataAdapter(branchCommand);
            await adapter.FillAsync(fileDataTable);
            return fileDataTable.Rows.Count == 0
                ? (0, 0, 0)
                : (Convert.ToUInt64(fileDataTable.Rows[0]["item_id"]), Convert.ToUInt64(fileDataTable.Rows[0]["destination_item_id"]), Convert.ToInt32(fileDataTable.Rows[0]["type"]));
        }

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
                template = await wiserItemsService.GetItemDetailsAsync(branchQueue.MergedBranchTemplateId, userId: userId);
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

            await taskAlertsService.NotifyUserByEmailAsync(userId, addedBy, branchQueue, configurationServiceName, subject, content, replaceData, template?.GetDetailValue("sender_email"), template?.GetDetailValue("sender_name"));
            await taskAlertsService.SendMessageToUserAsync(userId, addedBy, subject, branchQueue, configurationServiceName, replaceData, userId, addedBy);
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

                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
                return result;
            }

            // If the database to be deleted is the same as the currently connected database, then it's most likely the main branch, so stop the deletion.
            if (databaseConnection.ConnectedDatabase == settings.DatabaseName)
            {
                await logService.LogError(logger, LogScopes.RunBody, branchQueue.LogSettings, $"Trying to delete a branch, but it looks to be the main branch and for safety reasons we don't allow the main branch to be deleted. Queue ID was: {queueId}", configurationServiceName, branchQueue.TimeId, branchQueue.Order);

                error = "Trying to delete a branch, but it looks to be the main branch and for safety reasons we don't allow the main branch to be deleted.";
                result.Add("ErrorMessage", error);
                result.Add("Success", false);

                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
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

                await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
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
            await FinishBranchActionAsync(queueId, dataRowWithSettings, branchQueue, configurationServiceName, databaseConnection, wiserItemsService, taskAlertsService, String.IsNullOrWhiteSpace(error) ? new JArray() : new JArray(error), stopwatch, startDate, branchQueue.CreatedBranchTemplateId, DeleteBranchSubject, DeleteBranchTemplate);
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
        /// <returns>The ID of the same item in the production environment.</returns>
        private static ulong? GetMappedId(string tableName, IReadOnlyDictionary<string, Dictionary<ulong, ulong>> idMapping, ulong? id)
        {
            if (id is null or 0)
            {
                return id;
            }

            if (tableName.EndsWith(WiserTableNames.WiserItem, StringComparison.OrdinalIgnoreCase) && idMapping.ContainsKey(tableName) && idMapping[tableName].ContainsKey(id.Value))
            {
                id = idMapping[tableName][id.Value];
            }
            else
            {
                id = idMapping.FirstOrDefault(x => x.Value.ContainsKey(id.Value)).Value?[id.Value] ?? id;
            }

            return id;
        }

        /// <summary>
        /// Get the entity types and table prefixes for both items in a link.
        /// </summary>
        /// <param name="sourceId">The ID of the source item.</param>
        /// <param name="destinationId">The ID of the destination item.</param>
        /// <param name="linkType">The link type number.</param>
        /// <param name="mySqlConnection">The connection to the database.</param>
        /// <param name="wiserItemsService"></param>
        /// <returns>A named tuple with the entity types and table prefixes for both the source and the destination.</returns>
        private async Task<(string SourceType, string SourceTablePrefix, string DestinationType, string DestinationTablePrefix)?> GetEntityTypesOfLinkAsync(ulong sourceId, ulong destinationId, int linkType, MySqlConnection mySqlConnection, IWiserItemsService wiserItemsService)
        {
            var allLinkTypeSettings = (await wiserItemsService.GetAllLinkTypeSettingsAsync()).Where(l => l.Type == linkType);
            await using var command = mySqlConnection.CreateCommand();
            command.Parameters.AddWithValue("sourceId", sourceId);
            command.Parameters.AddWithValue("destinationId", destinationId);

            // It's possible that there are multiple link types that use the same number, so we have to check all of them.
            foreach (var linkTypeSettings in allLinkTypeSettings)
            {
                var sourceTablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(linkTypeSettings.SourceEntityType);
                var destinationTablePrefix = await wiserItemsService.GetTablePrefixForEntityAsync(linkTypeSettings.DestinationEntityType);

                // Check if the source item exists in this table.
                command.CommandText = $@"SELECT entity_type FROM {sourceTablePrefix}{WiserTableNames.WiserItem} WHERE id = ?sourceId
UNION ALL
SELECT entity_type FROM {sourceTablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix} WHERE id = ?sourceId
LIMIT 1";
                var sourceDataTable = new DataTable();
                using var sourceAdapter = new MySqlDataAdapter(command);
                await sourceAdapter.FillAsync(sourceDataTable);
                if (sourceDataTable.Rows.Count == 0 || !String.Equals(sourceDataTable.Rows[0].Field<string>("entity_type"), linkTypeSettings.SourceEntityType))
                {
                    continue;
                }

                // Check if the destination item exists in this table.
                command.CommandText = $@"SELECT entity_type FROM {destinationTablePrefix}{WiserTableNames.WiserItem} WHERE id = ?sourceId
UNION ALL
SELECT entity_type FROM {destinationTablePrefix}{WiserTableNames.WiserItem}{WiserTableNames.ArchiveSuffix} WHERE id = ?sourceId
LIMIT 1";
                var destinationDataTable = new DataTable();
                using var destinationAdapter = new MySqlDataAdapter(command);
                await destinationAdapter.FillAsync(destinationDataTable);
                if (destinationDataTable.Rows.Count == 0 || !String.Equals(destinationDataTable.Rows[0].Field<string>("entity_type"), linkTypeSettings.DestinationEntityType))
                {
                    continue;
                }

                // If we reached this point, it means we found the correct link type and entity types.
                return (linkTypeSettings.SourceEntityType, sourceTablePrefix, linkTypeSettings.DestinationEntityType, destinationTablePrefix);
            }

            return null;
        }

        /// <summary>
        /// Generates a new ID for the specified table. This will get the highest number from both databases and add 1 to that number.
        /// This is to make sure that the new ID will not exist anywhere yet, to prevent later synchronisation problems.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="productionConnection">The connection to the production database.</param>
        /// <param name="environmentConnection">The connection to the environment database.</param>
        /// <returns>The new ID that should be used for the first new item to be inserted into this table.</returns>
        private async Task<ulong> GenerateNewIdAsync(string tableName, MySqlConnection productionConnection, MySqlConnection environmentConnection)
        {
            await using var productionCommand = productionConnection.CreateCommand();
            await using var environmentCommand = environmentConnection.CreateCommand();

            productionCommand.CommandText = $"SELECT MAX(id) AS maxId FROM `{tableName}`";
            environmentCommand.CommandText = $"SELECT MAX(id) AS maxId FROM `{tableName}`";

            var maxProductionId = 0UL;
            var maxEnvironmentId = 0UL;

            await using var productionReader = await productionCommand.ExecuteReaderAsync();
            if (await productionReader.ReadAsync())
            {
                maxProductionId = Convert.ToUInt64(productionReader.GetValue(0));
            }

            await using var environmentReader = await environmentCommand.ExecuteReaderAsync();
            if (await environmentReader.ReadAsync())
            {
                maxEnvironmentId = Convert.ToUInt64(environmentReader.GetValue(0));
            }


            return Math.Max(maxProductionId, maxEnvironmentId) + 1;
        }

        /// <summary>
        /// Add an ID mapping, to map the ID of the environment database to the same item with a different ID in the production database.
        /// </summary>
        /// <param name="idMappings">The dictionary that contains the in-memory mappings.</param>
        /// <param name="tableName">The table that the ID belongs to.</param>
        /// <param name="originalItemId">The ID of the item in the selected environment.</param>
        /// <param name="newItemId">The ID of the item in the production environment.</param>
        /// <param name="environmentConnection">The database connection to the selected environment.</param>
        private async Task AddIdMappingAsync(IDictionary<string, Dictionary<ulong, ulong>> idMappings, string tableName, ulong originalItemId, ulong newItemId, MySqlConnection environmentConnection)
        {
            if (!idMappings.ContainsKey(tableName))
            {
                idMappings.Add(tableName, new Dictionary<ulong, ulong>());
            }

            if (idMappings[tableName].TryGetValue(originalItemId, out var otherId))
            {
                if (otherId == newItemId)
                {
                    // If we had already mapped these IDs together before, we can skip it.
                    return;
                }

                throw new Exception($"Trying to map ID '{newItemId}' to '{originalItemId}', but it was already mapped to '{otherId}'.");
            }

            idMappings[tableName].Add(originalItemId, newItemId);
            await using var environmentCommand = environmentConnection.CreateCommand();
            environmentCommand.CommandText = $@"INSERT INTO `{WiserTableNames.WiserIdMappings}` 
(table_name, our_id, production_id)
VALUES (?tableName, ?ourId, ?productionId)";

            environmentCommand.Parameters.AddWithValue("tableName", tableName);
            environmentCommand.Parameters.AddWithValue("ourId", originalItemId);
            environmentCommand.Parameters.AddWithValue("productionId", newItemId);
            await environmentCommand.ExecuteNonQueryAsync();
        }
    }
}