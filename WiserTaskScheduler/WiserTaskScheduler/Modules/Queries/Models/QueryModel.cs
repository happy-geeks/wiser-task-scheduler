using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.Queries.Models
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

        /// <summary>
        /// Gets or sets the seconds the query has before timeout.
        /// </summary>
        public int Timeout { get; set; } = 30;

        public CharacterEncodingModel CharacterEncoding { get; set; } = new CharacterEncodingModel();
    }
}
