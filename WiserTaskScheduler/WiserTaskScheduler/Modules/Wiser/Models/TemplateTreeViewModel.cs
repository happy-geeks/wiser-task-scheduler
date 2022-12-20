using System.Collections.Generic;

namespace WiserTaskScheduler.Modules.Wiser.Models
{
    public class TemplateTreeViewModel
    {
        /// <summary>
        /// Gets or sets the id of the template.
        /// </summary>
        public int TemplateId { get; set; }

        /// <summary>
        /// Gets or sets the name of the template.
        /// </summary>
        public string TemplateName { get; set; }

        /// <summary>
        /// Gets or sets if it is a folder.
        /// </summary>
        public bool IsFolder { get; set; }

        /// <summary>
        /// Gets or sets if it has children.
        /// </summary>
        public bool HasChildren { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="TemplateSettingsModel"/> if it is a template.
        /// </summary>
        public TemplateSettingsModel TemplateSettings { get; set; }

        /// <summary>
        /// Gets or sets the children if any.
        /// </summary>
        public List<TemplateTreeViewModel> ChildNodes { get; set; }
    }
}