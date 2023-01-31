using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;

namespace WiserTaskScheduler.Modules.HttpApis.Models
{
    /// <summary>
    /// A model to add Headers to a HTTP API call.
    /// </summary>
    [XmlType("Header")]
    public class HeaderModel
    {
        /// <summary>
        /// Gets or sets the name of the header property to set.
        /// </summary>
        [Required]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the value of the header property to set.
        /// </summary>
        [Required]
        public string Value { get; set; }

        /// <summary>
        /// Gets or sets the result set to use for replacements.
        /// </summary>
        public string UseResultSet { get; set; }
    }
}
