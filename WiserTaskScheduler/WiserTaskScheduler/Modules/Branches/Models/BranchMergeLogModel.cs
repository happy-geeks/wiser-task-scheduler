using System;

#nullable enable

namespace WiserTaskScheduler.Modules.Branches.Models;

/// <summary>
/// A model to store information that can then be added to the branch merge log table.
/// </summary>
public class BranchMergeLogModel(int queueId, string queueName, int branchId, ulong historyId, string tableName, string action)
{
    /// <summary>
    /// The ID of the merge action from the wiser_branches_queue table in the production database.
    /// </summary>
    public int QueueId { get; set; } = queueId;

    /// <summary>
    /// The name of the merge action from the wiser_branches_queue table in the production database.
    /// </summary>
    public string QueueName { get; set; } = queueName;

    /// <summary>
    /// The tenant ID of the branch, from the easy_customers table of the main Wiser database.
    /// </summary>
    public int BranchId { get; set; } = branchId;

    /// <summary>
    /// The date and time that the current action was executed.
    /// </summary>
    public DateTime DateTime { get; set; } = DateTime.Now;

    /// <summary>
    /// The ID from the wiser_history table in the branch database that was being merged.
    /// </summary>
    public ulong HistoryId { get; set; } = historyId;

    /// <summary>
    /// The table name as it was stored in the wiser_history table of the branch database.
    /// </summary>
    public string TableName { get; set; } = tableName;

    /// <summary>
    /// The action as it was stored in the wiser_history table of the branch database.
    /// </summary>
    public string Action { get; set; } = action;

    /// <summary>
    /// The value as it was before the change.
    /// </summary>
    public object? OldValue { get; set; }

    /// <summary>
    /// The value that it was changed to in the branch database, which we attempted to merge to the production database.
    /// </summary>
    public object? NewValue { get; set; }

    /// <summary>
    /// The id of the object in the branch database. This is the original value of the column `item_id` from wiser_history.
    /// </summary>
    public ulong ObjectIdOriginal { get; set; }

    /// <summary>
    /// The id of the object in the production database.
    /// </summary>
    public ulong ObjectIdMapped { get; set; }
}