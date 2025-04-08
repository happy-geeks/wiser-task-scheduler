using WiserTaskScheduler.Modules.Branches.Models;

namespace WiserTaskScheduler.Modules.Branches.Interfaces;

/// <summary>
/// A service for logging branch merge actions in batches, via a queue.
/// This will handle the queue automatically, you only need to use the log methods of this service.
/// </summary>
public interface IBranchBatchLoggerService
{
    /// <summary>
    /// Log a merge action to the database.
    /// This will add the log to a queue and then handle that queue periodically,
    /// so that we don't slow down the database too much with a lot of insert queries.
    /// </summary>
    /// <param name="logData"></param>
    /// <returns></returns>
    void LogMergeAction(BranchMergeLogModel logData);
}