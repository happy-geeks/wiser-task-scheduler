using System;
using System.Text;
using System.Text.Json.Serialization;
using GeeksCoreLibrary.Modules.Branches.Models;
using MySqlConnector;
using WiserTaskScheduler.Modules.Branches.Enums;
using WiserTaskScheduler.Modules.Branches.Services;

#nullable enable

namespace WiserTaskScheduler.Modules.Branches.Models;

/// <summary>
/// A model to store information that can then be added to the branch merge log table.
/// </summary>
public class BranchMergeLogModel(int queueId, string queueName, int branchId, ulong historyId, string tableName, string action, string field, MySqlConnectionStringBuilder productionDatabaseConnectionString, MySqlConnectionStringBuilder branchDatabaseConnectionString)
{
    #region Basic information

    /// <summary>
    /// The ID of the merge action from the wiser_branches_queue table in the production database.
    /// </summary>
    public int QueueId { get; } = queueId;

    /// <summary>
    /// The name of the merge action from the wiser_branches_queue table in the production database.
    /// </summary>
    public string QueueName { get; } = queueName;

    /// <summary>
    /// The tenant ID of the branch, from the easy_customers table of the main Wiser database.
    /// </summary>
    public int BranchId { get; } = branchId;

    /// <summary>
    /// The date and time that the current action was executed.
    /// </summary>
    public DateTime DateTime { get; } = DateTime.UtcNow;

    /// <summary>
    /// The ID from the wiser_history table in the branch database that was being merged.
    /// </summary>
    public ulong HistoryId { get; } = historyId;

    /// <summary>
    /// The table name as it was stored in the wiser_history table of the branch database.
    /// </summary>
    public string TableName { get; } = tableName;

    /// <summary>
    /// The action as it was stored in the wiser_history table of the branch database.
    /// </summary>
    public string Action { get; } = action;

    /// <summary>
    /// The field name as it was stored in the wiser_history table of the branch database.
    /// This can be the `key` from wiser_itemdetail tables, or a column name of any other table.
    /// </summary>
    public string Field { get; set; } = field;

    /// <summary>
    /// The value as it was before the change.
    /// </summary>
    public string? OldValue { get; set; }

    /// <summary>
    /// The value that it was changed to in the branch database, which we attempted to merge to the production database.
    /// </summary>
    public string? NewValue { get; set; }

    /// <summary>
    /// The id of the object in the branch database. This is the original value of the column `item_id` from wiser_history.
    /// </summary>
    public ulong ObjectIdOriginal { get; set; }

    /// <summary>
    /// The id of the object in the production database.
    /// </summary>
    public ulong ObjectIdMapped { get; set; }

    /// <summary>
    /// The connection string to the branch database.
    /// This is used for logging the merge action to the branch database, via the <see cref="BranchBatchLoggerService"/>.
    /// </summary>
    [JsonIgnore]
    internal string BranchDatabaseConnectionString { get; } = branchDatabaseConnectionString.ConnectionString;

    #endregion

    #region Item information

    /// <summary>
    /// If this change was for a Wiser item or something related to a Wiser item (such as a link), then this will contain the ID of the Wiser item in the branch database.
    /// If this is a change for a link, this is the ID of the source item.
    /// </summary>
    public ulong ItemIdOriginal { get; set; }

    /// <summary>
    /// If this change was for a Wiser item or something related to a Wiser item (such as a link), then this will contain the ID of the Wiser item in the production database.
    /// If this is a change for a link, this is the ID of the source item.
    /// </summary>
    public ulong ItemIdMapped { get; set; }

    /// <summary>
    /// If this change was for a Wiser item or something related to a Wiser item (such as a link), then this will contain the entity type of that item, as we found it.
    /// If this is a change for a link, this is the entity type of the source item.
    /// </summary>
    public string ItemEntityType { get; set; } = String.Empty;

    /// <summary>
    /// If this change was for a Wiser item or something related to a Wiser item (such as a link), then this will contain the full name of the `wiser_item` table that we used.
    /// If this is a change for a link, this is the table of the source item.
    /// </summary>
    public string ItemTableName { get; set; } = String.Empty;

    #endregion

    #region Link information

    /// <summary>
    /// If this is a change for a link, this is the value of the `id` column of `wiser_itemlink`, in the branch database.
    /// </summary>
    public ulong LinkIdOriginal { get; set; }

    /// <summary>
    /// If this is a change for a link, this is the value of the `id` column of `wiser_itemlink`, in the production database.
    /// </summary>
    public ulong LinkIdMapped { get; set; }

    /// <summary>
    /// If this is a change for a link, this is the ID of the destination Wiser item of that link, in the branch database.
    /// </summary>
    public ulong LinkDestinationItemIdOriginal { get; set; }

    /// <summary>
    /// If this is a change for a link, this is the ID of the destination Wiser item of that link, in the production database.
    /// </summary>
    public ulong LinkDestinationItemIdMapped { get; set; }

    /// <summary>
    /// If this is a change for a link, this is the entity type of the destination Wiser item of that link.
    /// </summary>
    public string LinkDestinationItemEntityType { get; set; } = String.Empty;

    /// <summary>
    /// If this is a change for a link, this is the table name of the destination Wiser item of that link.
    /// </summary>
    public string LinkDestinationItemTableName { get; set; } = String.Empty;

    /// <summary>
    /// If this is a change for a link, then this is the value of the `type` column of `wiser_itemlink`.
    /// </summary>
    public int LinkType { get; set; }

    /// <summary>
    /// If this is a change for a link, then this is the value of the `ordering` column of `wiser_itemlink`.
    /// </summary>
    public int LinkOrdering { get; set; }

    /// <summary>
    /// If this is a change for a link, or something related to a link, then this is the full table name of the link table that we used.
    /// </summary>
    public string LinkTableName { get; set; } = String.Empty;

    #endregion

    #region Item detail information

    /// <summary>
    /// If this is a change for a Wiser item detail, this will contain the value of the `id` column of `wiser_itemdetail`, in the branch database.
    /// </summary>
    public ulong ItemDetailIdOriginal { get; set; }

    /// <summary>
    /// If this is a change for a Wiser item detail, this will contain the value of the `id` column of `wiser_itemdetail`, in the branch database.
    /// </summary>
    public ulong ItemDetailIdMapped { get; set; }

    /// <summary>
    /// If this is a change for a Wiser item detail, this will contain the value of the `language_code` column of `wiser_itemdetail`, in the branch database.
    /// </summary>
    public string ItemDetailLanguageCode { get; set; } = String.Empty;

    /// <summary>
    /// If this is a change for a Wiser item detail, this will contain the value of the `groupname` column of `wiser_itemdetail`, in the branch database.
    /// </summary>
    public string ItemDetailGroupName { get; set; } = String.Empty;

    #endregion

    #region Item file information

    /// <summary>
    /// If this change was for a file in the database, then this will contain the value of the `id` column of `wiser_itemfile` in the branch database.
    /// </summary>
    public ulong FileIdOriginal { get; set; }

    /// <summary>
    /// If this change was for a file in the database, then this will contain the value of the `id` column of `wiser_itemfile` in the production database.
    /// </summary>
    public ulong FileIdMapped { get; set; }

    #endregion

    #region Extra information for debugging

    /// <summary>
    /// The merge settings that were used for this specific action/object.
    /// </summary>
    public ObjectMergeSettingsModel? UsedMergeSettings { get; set; }

    /// <summary>
    /// The conflict settings that were used for this specific action/object.
    /// </summary>
    public MergeConflictModel? UsedConflictSettings { get; set; }

    /// <summary>
    /// The hostname of the production database server.
    /// </summary>
    public string ProductionHost { get; } = productionDatabaseConnectionString.Server;

    /// <summary>
    /// The name of the production database schema.
    /// </summary>
    public string ProductionDatabase { get; } = productionDatabaseConnectionString.Database;

    /// <summary>
    /// The hostname of the branch database server.
    /// </summary>
    public string BranchHost { get; } = branchDatabaseConnectionString.Server;

    /// <summary>
    /// The name of the branch database schema.
    /// </summary>
    public string BranchDatabase { get; } = branchDatabaseConnectionString.Database;

    /// <summary>
    /// The merge status of this object.
    /// </summary>
    public ObjectActionMergeStatuses Status { get; set; } = ObjectActionMergeStatuses.None;

    /// <summary>
    /// This will contain debug information that explains what happened, where information was taken from etc. If the merge failed, it will explain the reason and/or contain the error message. If the merge was skipped, it will explain why.
    /// </summary>
    public StringBuilder MessageBuilder { get; } = new();

    /// <summary>
    /// The message that was built in the MessageBuilder. This is a string representation of the MessageBuilder.
    /// </summary>
    public string Message => MessageBuilder.ToString();

    #endregion
}