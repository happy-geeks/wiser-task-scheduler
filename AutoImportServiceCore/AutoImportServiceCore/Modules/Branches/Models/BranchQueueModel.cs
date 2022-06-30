using System.Xml.Serialization;
using AutoImportServiceCore.Core.Models;

namespace AutoImportServiceCore.Modules.Branches.Models
{
    /// <summary>
    /// A model for processing the branch queue.
    /// </summary>
    [XmlType("BranchQueue")]
    public class BranchQueueModel : ActionModel { }
}