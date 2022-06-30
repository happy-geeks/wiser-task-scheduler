using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using AutoImportServiceCore.Core.Models;

namespace AutoImportServiceCore.Modules.Queries.Models
{
    /// <summary>
    /// A model for a query.
    /// </summary>
    [XmlType("Query")]
    public class QueryModel : ActionModel
    {
        /// <summary>
        /// Gets or sets the query to execute.
        /// </summary>
        [Required]
        public string Query { get; set; }

        public CharacterEncodingModel CharacterEncoding { get; set; } = new CharacterEncodingModel();
    }
}
