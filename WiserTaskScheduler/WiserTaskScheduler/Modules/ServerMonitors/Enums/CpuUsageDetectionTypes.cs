namespace WiserTaskScheduler.Modules.ServerMonitors.Enums;

/// <summary>
/// Enum to decide which CPU usage dectection type needs to be used.
/// </summary>
public enum CpuUsageDetectionTypes
{
    /// <summary>
    /// This options picks the array count which counts the cpu values within an array.
    /// </summary>
    ArrayCount,

    /// <summary>
    /// This option picks the array counter option which uses an counter.
    /// </summary>
    Counter
}