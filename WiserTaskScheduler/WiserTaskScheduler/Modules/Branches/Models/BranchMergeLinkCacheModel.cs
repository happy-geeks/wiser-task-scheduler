namespace WiserTaskScheduler.Modules.Branches.Models;

public class BranchMergeLinkCacheModel
{
    public ulong Id { get; set; }
    public ulong? ItemId { get; set; }
    public ulong? DestinationItemId { get; set; }
    public int? Type { get; set; }
    public bool IsDeleted { get; set; }
}