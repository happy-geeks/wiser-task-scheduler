﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Newtonsoft.Json;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Branches.Models;

namespace WiserTaskScheduler.Modules.Branches.Services;

/// <inheritdoc cref="IBranchBatchLoggerService" />
public class BranchBatchLoggerService(IOptions<WtsSettings> wtsOptions, ILogger<BranchBatchLoggerService> logger) : BackgroundService, IBranchBatchLoggerService
{
    private readonly ConcurrentQueue<BranchMergeLogModel> mergeLogQueue = new();
    private readonly BatchLoggerOptions options = wtsOptions.Value.BatchLoggerOptions;

    /// <inheritdoc />
    public void LogMergeAction(BranchMergeLogModel logData)
    {
        mergeLogQueue.Enqueue(logData);
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

    // Override StopAsync to wait for the log queue to be completely flushed before shutdown.
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("BatchLogger is stopping. Flushing remaining logs.");
        await FlushUntilEmptyAsync();
        await base.StopAsync(cancellationToken);
    }

    private async Task FlushUntilEmptyAsync()
    {
        // Loop until the queue is empty or no new logs are added.
        while (!mergeLogQueue.IsEmpty)
        {
            await FlushAsync();
        }
    }

    private async Task FlushAsync()
    {
        var entriesToInsert = new List<BranchMergeLogModel>();

        while (entriesToInsert.Count < options.BatchSize && mergeLogQueue.TryDequeue(out var logEntry))
        {
            entriesToInsert.Add(logEntry);
        }

        if (entriesToInsert.Count == 0)
        {
            return;
        }

        try
        {
            await using var mySqlConnection = new MySqlConnection(entriesToInsert.First().BranchDatabaseConnectionString);
            await mySqlConnection.OpenAsync();
            await using var mySqlCommand = mySqlConnection.CreateCommand();

            // Create a multi-row INSERT statement.
            var values = new List<string>();
            for (var i = 0; i < entriesToInsert.Count; i++)
            {
                values.Add($"(?QueueId{i}, ?QueueName{i}, ?BranchId{i}, ?DateTime{i}, ?HistoryId{i}, ?TableName{i}, ?Field{i}, ?Action{i}, ?OldValue{i}, ?NewValue{i}, ?ObjectIdOriginal{i}, ?ObjectIdMapped{i}, ?ItemIdOriginal{i}, ?ItemIdMapped{i}, ?ItemTableName{i}, ?LinkIdOriginal{i}, ?LinkIdMapped{i}, ?LinkDestinationItemIdOriginal{i}, ?LinkDestinationItemIdMapped{i}, ?LinkDestinationItemTableName{i}, ?LinkType{i}, ?ItemDetailIdOriginal{i}, ?ItemDetailIdMapped{i}, ?FileIdOriginal{i}, ?FileIdMapped{i}, ?UsedMergeSettings{i}, ?UsedConflictSettings{i}, ?ProductionHost{i}, ?ProductionDatabase{i}, ?BranchHost{i}, ?BranchDatabase{i}, ?Status{i}, ?Message{i})");
                mySqlCommand.Parameters.AddWithValue($"QueueId{i}", entriesToInsert[i].QueueId);
                mySqlCommand.Parameters.AddWithValue($"QueueName{i}", entriesToInsert[i].QueueName);
                mySqlCommand.Parameters.AddWithValue($"BranchId{i}", entriesToInsert[i].BranchId);
                mySqlCommand.Parameters.AddWithValue($"DateTime{i}", entriesToInsert[i].DateTime);
                mySqlCommand.Parameters.AddWithValue($"HistoryId{i}", entriesToInsert[i].HistoryId);
                mySqlCommand.Parameters.AddWithValue($"TableName{i}", entriesToInsert[i].TableName);
                mySqlCommand.Parameters.AddWithValue($"Field{i}", entriesToInsert[i].Field);
                mySqlCommand.Parameters.AddWithValue($"Action{i}", entriesToInsert[i].Action);
                mySqlCommand.Parameters.AddWithValue($"OldValue{i}", entriesToInsert[i].OldValue);
                mySqlCommand.Parameters.AddWithValue($"NewValue{i}", entriesToInsert[i].NewValue);
                mySqlCommand.Parameters.AddWithValue($"ObjectIdOriginal{i}", entriesToInsert[i].ObjectIdOriginal);
                mySqlCommand.Parameters.AddWithValue($"ObjectIdMapped{i}", entriesToInsert[i].ObjectIdMapped);
                mySqlCommand.Parameters.AddWithValue($"ItemIdOriginal{i}", entriesToInsert[i].ItemIdOriginal);
                mySqlCommand.Parameters.AddWithValue($"ItemIdMapped{i}", entriesToInsert[i].ItemIdMapped);
                mySqlCommand.Parameters.AddWithValue($"ItemTableName{i}", entriesToInsert[i].ItemTableName);
                mySqlCommand.Parameters.AddWithValue($"LinkIdOriginal{i}", entriesToInsert[i].LinkIdOriginal);
                mySqlCommand.Parameters.AddWithValue($"LinkIdMapped{i}", entriesToInsert[i].LinkIdMapped);
                mySqlCommand.Parameters.AddWithValue($"LinkDestinationItemIdOriginal{i}", entriesToInsert[i].LinkDestinationItemIdOriginal);
                mySqlCommand.Parameters.AddWithValue($"LinkDestinationItemIdMapped{i}", entriesToInsert[i].LinkDestinationItemIdMapped);
                mySqlCommand.Parameters.AddWithValue($"LinkDestinationItemTableName{i}", entriesToInsert[i].LinkDestinationItemTableName);
                mySqlCommand.Parameters.AddWithValue($"LinkType{i}", entriesToInsert[i].LinkType);
                mySqlCommand.Parameters.AddWithValue($"ItemDetailIdOriginal{i}", entriesToInsert[i].ItemDetailIdOriginal);
                mySqlCommand.Parameters.AddWithValue($"ItemDetailIdMapped{i}", entriesToInsert[i].ItemDetailIdMapped);
                mySqlCommand.Parameters.AddWithValue($"FileIdOriginal{i}", entriesToInsert[i].FileIdOriginal);
                mySqlCommand.Parameters.AddWithValue($"FileIdMapped{i}", entriesToInsert[i].FileIdMapped);
                mySqlCommand.Parameters.AddWithValue($"UsedMergeSettings{i}", entriesToInsert[i].UsedMergeSettings == null ? String.Empty : JsonConvert.SerializeObject(entriesToInsert[i].UsedMergeSettings));
                mySqlCommand.Parameters.AddWithValue($"UsedConflictSettings{i}", entriesToInsert[i].UsedConflictSettings == null ? String.Empty : JsonConvert.SerializeObject(entriesToInsert[i].UsedConflictSettings));
                mySqlCommand.Parameters.AddWithValue($"ProductionHost{i}", entriesToInsert[i].ProductionHost);
                mySqlCommand.Parameters.AddWithValue($"ProductionDatabase{i}", entriesToInsert[i].ProductionDatabase);
                mySqlCommand.Parameters.AddWithValue($"BranchHost{i}", entriesToInsert[i].BranchHost);
                mySqlCommand.Parameters.AddWithValue($"BranchDatabase{i}", entriesToInsert[i].BranchDatabase);
                mySqlCommand.Parameters.AddWithValue($"Status{i}", entriesToInsert[i].Status.ToString("G"));
                mySqlCommand.Parameters.AddWithValue($"Message{i}", entriesToInsert[i].Message);
            }

            mySqlCommand.CommandText = $"""
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
                                            item_table_name,
                                            link_id_original,
                                            link_id_mapped,
                                            link_destination_item_id_original,
                                            link_destination_item_id_mapped,
                                            link_destination_item_table_name,
                                            link_type,
                                            item_detail_id_original,
                                            item_detail_id_mapped,
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
                                        VALUES {String.Join(Environment.NewLine, values)}
                                        """;

            await mySqlCommand.ExecuteNonQueryAsync();
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Error during log flush");
        }
    }
}