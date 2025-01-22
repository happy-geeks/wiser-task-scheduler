using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.RunSchemes.Enums;

namespace WiserTaskScheduler.Modules.RunSchemes.Models;

/// <summary>
/// A model for the run scheme.
/// </summary>
[XmlType("RunScheme")]
public class RunSchemeModel
{
    /// <summary>
    /// Gets or sets the ID in the database.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the type.
    /// </summary>
    [Required]
    public RunSchemeTypes Type { get; set; }

    /// <summary>
    /// Gets or sets the time ID.
    /// </summary>
    [Required]
    public int TimeId { get; set; }

    /// <summary>
    /// Gets or sets the name of the action.
    /// </summary>
    public string Action { get; set; }

    /// <summary>
    /// Gets or sets if the run time needs to be run immediately.
    /// </summary>
    public bool RunImmediately { get; set; } = false;

    /// <summary>
    /// Gets or sets the delay for <see cref="RunSchemeTypes"/>.Continuous.
    /// </summary>
    [XmlIgnore]
    public TimeSpan Delay { get; set; }

    /// <summary>
    /// Gets or sets <see cref="Delay"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("Delay")]
    public string DelayString
    {
        get => XmlConvert.ToString(Delay);
        set => Delay = String.IsNullOrWhiteSpace(value) ? TimeSpan.Zero : value.StartsWith("P") ? XmlConvert.ToTimeSpan(value) : TimeSpan.Parse(value);
    }

    /// <summary>
    /// Gets or sets the time the run needs to start at <see cref="RunSchemeTypes"/>.Continuous.
    /// </summary>
    [XmlIgnore]
    public TimeSpan StartTime { get; set; }

    /// <summary>
    /// Gets or sets <see cref="StartTime"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("StartTime")]
    public string StartTimeString
    {
        get => XmlConvert.ToString(StartTime);
        set => StartTime = String.IsNullOrWhiteSpace(value) ? TimeSpan.Zero : value.StartsWith("P") ? XmlConvert.ToTimeSpan(value) : TimeSpan.Parse(value);
    }

    /// <summary>
    /// Gets or sets the time the run steeds to stop at <see cref="RunSchemeTypes"/>.Continuous.
    /// </summary>
    [XmlIgnore]
    public TimeSpan StopTime { get; set; }

    /// <summary>
    /// Gets or sets <see cref="StopTime"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("StopTime")]
    public string StopTimeString
    {
        get => XmlConvert.ToString(StopTime);
        set => StopTime = String.IsNullOrWhiteSpace(value) ? TimeSpan.Zero : value.StartsWith("P") ? XmlConvert.ToTimeSpan(value) : TimeSpan.Parse(value);
    }

    /// <summary>
    /// Gets or sets if the weekend needs to be skipped.
    /// </summary>
    public bool SkipWeekend { get; set; }

    /// <summary>
    /// Gets or sets what days need to be skipped.
    /// </summary>
    [XmlIgnore]
    public int[] SkipDays { get; set; }

    /// <summary>
    /// Gets or sets <see cref="SkipDays"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("SkipDays")]
    public string SkipDaysString
    {
        get => SkipDays == null ? "" : String.Join(',', SkipDays);
        set => SkipDays = String.IsNullOrWhiteSpace(value) ? SkipDays = [] : value.Split(',').Select(Int32.Parse).ToArray();
    }

    /// <summary>
    /// Gets or sets the day of the week for <see cref="RunSchemeTypes"/>.Weekly.
    /// </summary>
    public int DayOfWeek { get; set; }

    /// <summary>
    /// Gets or sets the day of the month for <see cref="RunSchemeTypes"/>.Monthly.
    /// </summary>
    public int DayOfMonth { get; set; } = 1;

    /// <summary>
    /// Gets or sets the hour for <see cref="RunSchemeTypes"/>.Daily, <see cref="RunSchemeTypes"/>.Weekly and <see cref="RunSchemeTypes"/>.Monthly.
    /// </summary>
    [XmlIgnore]
    public TimeSpan Hour { get; set; }

    /// <summary>
    /// Gets or sets <see cref="Hour"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("Hour")]
    public string HourString
    {
        get => XmlConvert.ToString(Hour);
        set => Hour = String.IsNullOrWhiteSpace(value) ? TimeSpan.Zero : value.StartsWith("P") ? XmlConvert.ToTimeSpan(value) : TimeSpan.Parse(value);
    }

    /// <summary>
    /// Gets or sets the settings for the logger.
    /// </summary>
    public LogSettings LogSettings { get; set; }
}