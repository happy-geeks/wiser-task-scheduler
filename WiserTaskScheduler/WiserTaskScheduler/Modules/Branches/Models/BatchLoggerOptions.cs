using System;

namespace WiserTaskScheduler.Modules.Branches.Models;

/// <summary>
/// Configuration options for batch logging.
/// </summary>
public class BatchLoggerOptions
{
    /// <summary>
    /// The amount of logs to insert into the database for a single batch.
    /// </summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>
    /// The amount of time to wait before flushing the next batch of logs to the database.
    /// </summary>
    public TimeSpan FlushInterval { get; set; } = new(0, 0, 5);
}