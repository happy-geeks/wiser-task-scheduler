﻿using System;
using System.ComponentModel;
using System.Xml;
using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.CleanupWiserHistory.Models;

[XmlType("CleanupWiserHistory")]
public class CleanupWiserHistoryModel : ActionModel
{
    /// <summary>
    /// Gets or sets the name of the entity.
    /// </summary>
    public string EntityName { get; set; }

    /// <summary>
    /// Gets or sets if all entities need to be cleaned instead of a specific one.
    /// </summary>
    public bool CleanupAllEntities { get; set; }

    /// <summary>
    /// Gets or sets the time the history of the given entity needs to be stored.
    /// </summary>
    [XmlIgnore]
    public TimeSpan TimeToStore { get; set; }

    /// <summary>
    /// Gets or sets <see cref="TimeToStore"/> from a XML file.
    /// </summary>
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [XmlElement("TimeToStore")]
    public string TimeToStoreString
    {
        get => XmlConvert.ToString(TimeToStore);
        set => TimeToStore = String.IsNullOrWhiteSpace(value) ? TimeSpan.Zero : value.StartsWith("P") ? XmlConvert.ToTimeSpan(value) : TimeSpan.Parse(value);
    }

    /// <summary>
    /// Gets or sets if the cleaned tables need to be optimized afterwards.
    /// </summary>
    public bool OptimizeTablesAfterCleanup { get; set; } = true;
}