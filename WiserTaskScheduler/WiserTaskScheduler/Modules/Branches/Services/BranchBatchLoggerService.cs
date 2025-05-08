using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Branches.Models;

namespace WiserTaskScheduler.Modules.Branches.Services;

/// <inheritdoc cref="IBranchBatchLoggerService" />
public class BranchBatchLoggerService(
    IOptions<WtsSettings> wtsOptions,
    ILogger<BranchBatchLoggerService> logger,
    ILogService logService,
    IOptions<WtsSettings> wtsSettings,
    IServiceProvider serviceProvider)
    : BackgroundService, IBranchBatchLoggerService
{
    private readonly ConcurrentQueue<BranchMergeLogModel> mergeLogQueue = new();
    private readonly BatchLoggerOptions options = wtsOptions.Value.BatchLoggerOptions;
    private readonly string logName = $"BranchBatchLoggerService ({Environment.MachineName} - {wtsSettings.Value.Name})";

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    /// <inheritdoc />
    public async Task PrepareMergeLogTableAsync(string connectionString)
    {
        if (String.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        await using var scope = serviceProvider.CreateAsyncScope();
        var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
        await databaseConnection.ChangeConnectionStringsAsync(connectionString);
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        await databaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WiserBranchMergeLog]);

        // Empty the log table before we start, we only want to see the logs of the last merge, otherwise the table will become way too large.
        await databaseConnection.ExecuteAsync($"TRUNCATE TABLE {WiserTableNames.WiserBranchMergeLog}");
    }

    /// <inheritdoc />
    public void LogMergeAction(BranchMergeLogModel logData)
    {
        mergeLogQueue.Enqueue(logData);
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Override StopAsync to wait for the log queue to be completely flushed before shutdown.
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "BatchLogger is stopping. Flushing remaining logs.", logName);
        await FlushUntilEmptyAsync();
        await base.StopAsync(cancellationToken);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Continuously flush log entries until cancellation is requested.
        while (!stoppingToken.IsCancellationRequested)
        {
            await FlushAsync();
            try
            {
                await Task.Delay(options.FlushInterval, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // Ignore cancellation exceptions from Task.Delay.
            }
        }

        // When cancellation is requested, flush any remaining entries.
        await FlushUntilEmptyAsync();
    }

    /// <summary>
    /// Method to flush the log queue until it is empty.
    /// This should be called when the application is stopping to ensure all logs are written before the application is closed.
    /// </summary>
    private async Task FlushUntilEmptyAsync()
    {
        // Loop until the queue is empty or no new logs are added.
        while (!mergeLogQueue.IsEmpty)
        {
            await FlushAsync();
        }
    }

    /// <summary>
    /// Flush the next batch of log entries to the database.
    /// </summary>
    private async Task FlushAsync()
    {
        // Create a list for the next batch of logs.
        var entriesToInsert = new List<BranchMergeLogModel>();
        string branchDatabaseConnectionString = null;

        // Try to dequeue log entries from the queue until we reach the batch size, or the connection string changes, or the queue is empty.
        while (entriesToInsert.Count < options.BatchSize
               && (
                   branchDatabaseConnectionString == null
                   || (mergeLogQueue.TryPeek(out var logEntry) && branchDatabaseConnectionString == logEntry.BranchDatabaseConnectionString)
               )
               && mergeLogQueue.TryDequeue(out logEntry))
        {
            branchDatabaseConnectionString = logEntry.BranchDatabaseConnectionString;
            entriesToInsert.Add(logEntry);
        }

        // If there are no entries to insert, return early.
        if (entriesToInsert.Count == 0)
        {
            return;
        }

        try
        {
            // Use the connection string from the first entry in the batch to create a MySQL connection.
            await using var scope = serviceProvider.CreateAsyncScope();
            var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
            await databaseConnection.ChangeConnectionStringsAsync(entriesToInsert.First().BranchDatabaseConnectionString);

            // Create a multi-row INSERT statement.
            var values = new List<string>();
            for (var i = 0; i < entriesToInsert.Count; i++)
            {
                values.Add($"(?QueueId{i}, ?QueueName{i}, ?BranchId{i}, ?DateTime{i}, ?HistoryId{i}, ?TableName{i}, ?Field{i}, ?Action{i}, ?OldValue{i}, ?NewValue{i}, ?ObjectIdOriginal{i}, ?ObjectIdMapped{i}, ?ItemIdOriginal{i}, ?ItemIdMapped{i}, ?ItemEntityType{i}, ?ItemTableName{i}, ?LinkIdOriginal{i}, ?LinkIdMapped{i}, ?LinkDestinationItemIdOriginal{i}, ?LinkDestinationItemIdMapped{i}, ?LinkDestinationItemEntityType{i}, ?LinkDestinationItemTableName{i}, ?LinkType{i}, ?LinkOrdering{i}, ?LinkTableName{i}, ?ItemDetailIdOriginal{i}, ?ItemDetailIdMapped{i}, ?ItemDetailLanguageCode{i}, ?ItemDetailGroupName{i}, ?FileIdOriginal{i}, ?FileIdMapped{i}, ?UsedMergeSettings{i}, ?UsedConflictSettings{i}, ?ProductionHost{i}, ?ProductionDatabase{i}, ?BranchHost{i}, ?BranchDatabase{i}, ?Status{i}, ?Message{i})");
                databaseConnection.AddParameter($"QueueId{i}", entriesToInsert[i].QueueId);
                databaseConnection.AddParameter($"QueueName{i}", entriesToInsert[i].QueueName);
                databaseConnection.AddParameter($"BranchId{i}", entriesToInsert[i].BranchId);
                databaseConnection.AddParameter($"DateTime{i}", entriesToInsert[i].DateTime);
                databaseConnection.AddParameter($"HistoryId{i}", entriesToInsert[i].HistoryId);
                databaseConnection.AddParameter($"TableName{i}", entriesToInsert[i].TableName);
                databaseConnection.AddParameter($"Field{i}", entriesToInsert[i].Field);
                databaseConnection.AddParameter($"Action{i}", entriesToInsert[i].Action);
                databaseConnection.AddParameter($"OldValue{i}", entriesToInsert[i].OldValue ?? String.Empty);
                databaseConnection.AddParameter($"NewValue{i}", entriesToInsert[i].NewValue ?? String.Empty);
                databaseConnection.AddParameter($"ObjectIdOriginal{i}", entriesToInsert[i].ObjectIdOriginal);
                databaseConnection.AddParameter($"ObjectIdMapped{i}", entriesToInsert[i].ObjectIdMapped);
                databaseConnection.AddParameter($"ItemIdOriginal{i}", entriesToInsert[i].ItemIdOriginal);
                databaseConnection.AddParameter($"ItemIdMapped{i}", entriesToInsert[i].ItemIdMapped);
                databaseConnection.AddParameter($"ItemEntityType{i}", entriesToInsert[i].ItemEntityType);
                databaseConnection.AddParameter($"ItemTableName{i}", entriesToInsert[i].ItemTableName);
                databaseConnection.AddParameter($"LinkIdOriginal{i}", entriesToInsert[i].LinkIdOriginal);
                databaseConnection.AddParameter($"LinkIdMapped{i}", entriesToInsert[i].LinkIdMapped);
                databaseConnection.AddParameter($"LinkDestinationItemIdOriginal{i}", entriesToInsert[i].LinkDestinationItemIdOriginal);
                databaseConnection.AddParameter($"LinkDestinationItemIdMapped{i}", entriesToInsert[i].LinkDestinationItemIdMapped);
                databaseConnection.AddParameter($"LinkDestinationItemEntityType{i}", entriesToInsert[i].LinkDestinationItemEntityType);
                databaseConnection.AddParameter($"LinkDestinationItemTableName{i}", entriesToInsert[i].LinkDestinationItemTableName);
                databaseConnection.AddParameter($"LinkType{i}", entriesToInsert[i].LinkType);
                databaseConnection.AddParameter($"LinkOrdering{i}", entriesToInsert[i].LinkOrdering);
                databaseConnection.AddParameter($"LinkTableName{i}", entriesToInsert[i].LinkTableName);
                databaseConnection.AddParameter($"ItemDetailIdOriginal{i}", entriesToInsert[i].ItemDetailIdOriginal);
                databaseConnection.AddParameter($"ItemDetailIdMapped{i}", entriesToInsert[i].ItemDetailIdMapped);
                databaseConnection.AddParameter($"ItemDetailLanguageCode{i}", entriesToInsert[i].ItemDetailLanguageCode);
                databaseConnection.AddParameter($"ItemDetailGroupName{i}", entriesToInsert[i].ItemDetailGroupName);
                databaseConnection.AddParameter($"FileIdOriginal{i}", entriesToInsert[i].FileIdOriginal);
                databaseConnection.AddParameter($"FileIdMapped{i}", entriesToInsert[i].FileIdMapped);
                databaseConnection.AddParameter($"UsedMergeSettings{i}", entriesToInsert[i].UsedMergeSettings == null ? null : JsonConvert.SerializeObject(entriesToInsert[i].UsedMergeSettings));
                databaseConnection.AddParameter($"UsedConflictSettings{i}", entriesToInsert[i].UsedConflictSettings == null ? null : JsonConvert.SerializeObject(entriesToInsert[i].UsedConflictSettings));
                databaseConnection.AddParameter($"ProductionHost{i}", entriesToInsert[i].ProductionHost);
                databaseConnection.AddParameter($"ProductionDatabase{i}", entriesToInsert[i].ProductionDatabase);
                databaseConnection.AddParameter($"BranchHost{i}", entriesToInsert[i].BranchHost);
                databaseConnection.AddParameter($"BranchDatabase{i}", entriesToInsert[i].BranchDatabase);
                databaseConnection.AddParameter($"Status{i}", entriesToInsert[i].Status.ToString("G"));
                databaseConnection.AddParameter($"Message{i}", entriesToInsert[i].Message);
            }

            var query = $"""
                         INSERT INTO {WiserTableNames.WiserBranchMergeLog} 
                         (
                             branch_queue_id,
                             branch_queue_name,
                             branch_id,
                             date_time,
                             history_id,
                             table_name,
                             field,
                             action,
                             old_value,
                             new_value,
                             object_id_original,
                             object_id_mapped,   
                             item_id_original,
                             item_id_mapped,
                             item_entity_type,
                             item_table_name,
                             link_id_original,
                             link_id_mapped,
                             link_destination_item_id_original,
                             link_destination_item_id_mapped,
                             link_destination_item_entity_type,
                             link_destination_item_table_name,
                             link_type,
                             link_ordering,
                             link_table_name,
                             item_detail_id_original,
                             item_detail_id_mapped,
                             item_detail_language_code,
                             item_detail_group_name,
                             file_id_original,
                             file_id_mapped,
                             used_merge_settings,
                             used_conflict_settings,
                             production_host,
                             production_database,
                             branch_host,
                             branch_database,
                             status,
                             message
                         )
                         VALUES {String.Join($",{Environment.NewLine}\t", values)}
                         """;

            await databaseConnection.ExecuteAsync(query);
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"An error occurred during log flush: {exception}", logName);
        }
    }
}