using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.WiserImports.Models;

[XmlType("WiserImport")]
public class WiserImportModel : ActionModel
{
    /// <summary>
    /// Gets or sets the ID of the mail template to get information from.
    /// This is for sending notifications about the status of the import, to the user that started the import.
    /// </summary>
    public ulong TemplateId { get; set; }

    /// <summary>
    /// Gets or sets the connection string, if left null the connection string set in the configuration will be used.
    /// </summary>
    public string ConnectionString { get; set; }
}