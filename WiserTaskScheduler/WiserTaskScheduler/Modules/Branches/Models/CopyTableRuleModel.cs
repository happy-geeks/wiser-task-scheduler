using System.Xml.Serialization;
using WiserTaskScheduler.Modules.Branches.Enums;

namespace WiserTaskScheduler.Modules.Branches.Models;

[XmlType("CopyTableRule")]
public class CopyTableRuleModel
{
    /// <summary>
    /// Gets or sets the name of the table to set the specific copy rules for.
    /// </summary>
    public string TableName { get; set; }

    /// <summary>
    /// Gets or sets the specific copy rules for the table.
    /// </summary>
    public CopyTypes CopyType { get; set; } = CopyTypes.Structure;
}