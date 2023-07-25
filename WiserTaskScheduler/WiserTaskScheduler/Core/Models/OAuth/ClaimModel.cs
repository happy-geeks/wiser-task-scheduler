using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;

namespace WiserTaskScheduler.Core.Models.OAuth;

/// <summary>
/// Represents a claim for a JWT token.
/// </summary>
[XmlType("Claim")]
public class ClaimModel
{
    /// <summary>
    /// Gets or sets the name of the claim.
    /// </summary>
    [Required]
    public string Name { get; set; }

    /// <summary>
    /// Gets or sets the value of the claim.
    /// </summary>
    [Required]
    public string Value { get; set; }

    /// <summary>
    /// The data type of the value.
    /// </summary>
    public string DataType { get; set; }
}