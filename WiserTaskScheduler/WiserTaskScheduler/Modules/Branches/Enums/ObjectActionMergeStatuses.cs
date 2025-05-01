namespace WiserTaskScheduler.Modules.Branches.Enums;

public enum ObjectActionMergeStatuses
{
    /// <summary>
    /// The initial status, before any action has been taken.
    /// </summary>
    None,

    /// <summary>
    /// The action was successfully merged.
    /// </summary>
    Merged,

    /// <summary>
    /// The action was skipped because we either didn't have enough information to merge it, or because the user indicated that they want to skip it.
    /// </summary>
    Skipped,

    /// <summary>
    /// The action was skipped and removed from the history, because it was deemed unnecessary to merge.
    /// This could be because the object has been deleted from the branch, without ever having existed in production.
    /// There could also be several other reasons. See the "message" column of the logs for the reason why this was done.
    /// </summary>
    SkippedAndRemoved,

    /// <summary>
    /// The action was not merged due to an unexpected problem.
    /// </summary>
    Failed
}