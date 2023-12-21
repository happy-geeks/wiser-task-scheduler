using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Models;

namespace WiserTaskScheduler.Modules.GenerateFiles.Models
{
    [XmlType("GenerateFile")]
    public class GenerateFileModel : ActionModel
    {
        /// <summary>
        /// Gets or sets the location where the file need to be saved to.
        /// </summary>
        public string FileLocation { get; set; }

        /// <summary>
        /// Gets or sets the name of the file.
        /// </summary>
        public string FileName { get; set; }

        /// <summary>
        /// Gets or sets if a single file needs to be generated.
        /// </summary>
        public bool SingleFile { get; set; } = true;

        /// <summary>
        /// Gets or sets the body to save in the file.
        /// </summary>
        public BodyModel Body { get; set; }
        
        /// <summary>
        /// Item ID when generated file must be saved to wiser_itemfile table
        /// </summary>
        public string ItemId { get; set; }

        /// <summary>
        /// Item Link ID when generated file must be saved to wiser_itemfile table
        /// </summary>
        public string ItemLinkId { get; set; }

        /// <summary>
        /// Property name when generated file must be saved to wiser_itemfile table
        /// </summary>
        public string PropertyName { get; set; }
    }
}
