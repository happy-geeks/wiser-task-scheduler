namespace WiserTaskScheduler.Modules.ServerMonitors.Enums;

/// <summary>
/// Enum to decide which hardware component needs to be monitored.
/// </summary>
public enum ServerMonitorTypes
{
    /// <summary>
    /// This option chooses the CPU.
    /// </summary>
    Cpu,

    /// <summary>
    /// This option chooses the hard drive.
    /// </summary>
    Drive,

    /// <summary>
    /// This option chooses the RAM.
    /// </summary>
    Ram,

    /// <summary>
    /// This option chooses the network interface.
    /// </summary>
    Network
}