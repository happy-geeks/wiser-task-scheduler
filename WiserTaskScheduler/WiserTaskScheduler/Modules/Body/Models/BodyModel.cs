using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;

namespace WiserTaskScheduler.Modules.Body.Models;

/// <summary>
/// A model to add a body.
/// </summary>
[XmlType("Body")]
public class BodyModel
{
    /// <summary>
    /// Gets or sets the type of the content of the body.
    /// </summary>
    [Required]
    public string ContentType { get; set; }

    /// <summary>
    /// Generate PDF from HTML and save the PDF
    /// </summary>
    public bool GeneratePdf { get; set; } = false;
        
    /// <summary>
    /// Get or sets the PDF's that have to be merged.
    /// </summary>
    [XmlArray("MergePdfs")]
    [XmlArrayItem(typeof(MergePdfsModel))]
    public MergePdfsModel[] MergePdfs { get; set; }

    /// <summary>
    /// Gets or sets the body parts.
    /// </summary>
    [Required]
    [XmlArray("BodyParts")]
    [XmlArrayItem(typeof(BodyPartModel))]
    public BodyPartModel[] BodyParts { get; set; }
}