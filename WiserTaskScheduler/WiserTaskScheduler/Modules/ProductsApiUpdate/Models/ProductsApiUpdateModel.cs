namespace WiserTaskScheduler.Modules.ProductsApiUpdate.Models;

using WiserTaskScheduler.Core.Models;
using System.Xml.Serialization;
using System.ComponentModel.DataAnnotations;

/// <summary>
/// A model for a productsapiupdate.
/// </summary>
[XmlType("ProductsApiUpdate")]
public class ProductsApiUpdateModel : ActionModel
{
    /// <summary>
    /// Gets or sets the url were the product api is running.
    /// </summary>
    [Required]
    public string ServerUrl { get; set; }

    /// <summary>
    /// Gets or sets the OAuth service to get the access token from.
    /// </summary>
    [Required]
    public string OAuth { get; set; }
}