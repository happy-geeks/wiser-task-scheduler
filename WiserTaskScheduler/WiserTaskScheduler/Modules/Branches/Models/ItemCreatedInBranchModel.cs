namespace WiserTaskScheduler.Modules.Branches.Models;

/// <summary>
/// A simple model for keeping track of items that have been created and deleted in the branch.
/// </summary>
internal class ItemCreatedInBranchModel
{
    /// <summary>
    /// Gets or sets the ID of the item.
    /// </summary>
    public ulong ItemId { get; set; }

    /// <summary>
    /// Gets or sets whether or not the item was deleted.
    /// </summary>
    public bool AlsoDeleted { get; set; }

    /// <summary>
    /// Gets or sets whether or not the item was undeleted.
    /// </summary>
    public bool AlsoUndeleted { get; set; }
}