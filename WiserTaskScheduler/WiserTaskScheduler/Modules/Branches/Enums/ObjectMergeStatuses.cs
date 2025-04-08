namespace WiserTaskScheduler.Modules.Branches.Enums;

public enum ObjectMergeStatuses
{
    /// <summary>
    /// The initial status, before any action has been taken.
    /// </summary>
    None,

    /// <summary>
    /// The object was successfully merged.
    /// </summary>
    Merged,

    /// <summary>
    /// The object was skipped because we either didn't have enough information to merge it, or it was deemed unnecessary to merge.
    /// </summary>
    Skipped,

    /// <summary>
    /// The object was not merged due to an unexpected problem.
    /// </summary>
    Failed
}