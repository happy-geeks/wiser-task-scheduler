using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;

namespace WiserTaskScheduler.Modules.Body.Models;

/// <summary>
/// A model to add a PDF that has to be merged.
/// </summary>
[XmlType("Pdf")]
public class MergePdfsModel
{
    /// <summary>
    /// Gets or sets the Wiser item ID, which holds the PDF file that has to be merged.
    /// </summary>
    [Required]
    public string WiserItemId { get; set; }

    /// <summary>
    /// Gets or sets the property name of the PDF file stored with the Wiser item.
    /// </summary>
    [Required]
    public string PropertyName { get; set; }
}