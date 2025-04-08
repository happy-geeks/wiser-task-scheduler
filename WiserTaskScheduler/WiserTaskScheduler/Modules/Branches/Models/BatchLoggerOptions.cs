using System;

namespace WiserTaskScheduler.Modules.Branches.Models;

public class BatchLoggerOptions
{
    public int BatchSize { get; set; } = 50;
    public TimeSpan FlushInterval { get; set; } = new(0, 0, 5);
}