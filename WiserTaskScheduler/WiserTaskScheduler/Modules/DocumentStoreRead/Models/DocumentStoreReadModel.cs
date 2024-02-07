using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.DocumentStoreRead.Models;

[XmlType("DocumentStoreRead")]
public class DocumentStoreReadModel : ActionModel
{
    /// <summary>
    /// Gets or sets the mame of the entity that will be read from the document store
    /// Leave empty if not targeting a specific entity
    /// </summary>
    public string EntityName { get; set; }
    
    /// <summary>
    /// Gets or sets what published Environment the item needs to be set as
    /// </summary>
    public int? PublishedEnvironmentToSet { get; set; } = null;
    
}