using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.ServerMonitors.Enums;

namespace WiserTaskScheduler.Modules.ServerMonitors.Models;

[XmlType("ServerMonitor")]
public class ServerMonitorModel : ActionModel
{
    /// <summary>
    /// Gets or sets the Threshold to use for the monitor check.
    /// </summary>
    public int Threshold { get; set; }

    /// <summary>
    /// Gets or sets the name of the disk to read the info from.
    /// </summary>
    public string DriveName { get; set; }

    /// <summary>
    /// Gets or sets the name of the Network interface to read the info from.
    /// </summary>
    public string NetworkInterfaceName { get; set; }

    /// <summary>
    /// Gets or sets the type of Server Monitor.
    /// </summary>
    public ServerMonitorTypes ServerMonitorType { get; set; }

    /// <summary>
    /// Gets or sets the type of Usage detection for the CPU Monitor.
    /// </summary>
    public CpuUsageDetectionTypes CpuUsageDetectionType { get; set; }

    /// <summary>
    /// Gets or sets the email to send the warning to.
    /// </summary>
    public string EmailAddressForWarning { get; set; }

    /// <summary>
    /// Gets or sets the size of the array for the Array Count option
    /// </summary>
    public int CpuArrayCountSize { get; set; }

    /// <summary>
    /// Gets or sets the size of the count for the CPU counter option.
    /// </summary>
    public int CpuCounterSize { get; set; }
}