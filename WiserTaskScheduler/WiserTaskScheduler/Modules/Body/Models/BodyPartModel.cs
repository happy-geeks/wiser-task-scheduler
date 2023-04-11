using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;

namespace WiserTaskScheduler.Modules.Body.Models
{
    /// <summary>
    /// A model for a part of the body.
    /// </summary>
    [XmlType("BodyPart")]
    public class BodyPartModel
    {
        /// <summary>
        /// Gets or sets the text of the body.
        /// </summary>
        [Required]
        public string Text { get; set; }

        /// <summary>
        /// Gets or sets if the body part is added once or for each row in the result set.
        /// </summary>
        public bool SingleItem { get; set; } = true;

        /// <summary>
        /// Gets or sets the result set to use for this specific part.
        /// </summary>
        public string UseResultSet { get; set; }

        /// <summary>
        /// Gets or sets if the body part needs to use the forced index.
        /// </summary>
        public bool ForceIndex { get; set; }

        /// <summary>
        /// Gets or sets whether the logic snippets (<c>[if]...[else]...[endif]</c>) should be evaluated. 
        /// </summary>
        public bool EvaluateLogicSnippets { get; set; }
    }
}
