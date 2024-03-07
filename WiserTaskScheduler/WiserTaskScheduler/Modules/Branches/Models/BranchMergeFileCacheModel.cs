namespace WiserTaskScheduler.Modules.Branches.Models;

public class BranchMergeFileCacheModel
{
    public ulong Id { get; set; }
    public ulong? ItemId { get; set; }
    public ulong? LinkId { get; set; }
    public bool IsDeleted { get; set; }
}