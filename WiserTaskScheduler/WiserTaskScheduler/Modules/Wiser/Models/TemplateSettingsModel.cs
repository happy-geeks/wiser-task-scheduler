namespace WiserTaskScheduler.Modules.Wiser.Models
{
    /// <summary>
    /// A model for all settings of a template.
    /// </summary>
    public class TemplateSettingsModel
    {
        /// <summary>
        /// Gets or sets the id of the template.
        /// </summary>
        public int TemplateId { get; set; }

        /// <summary>
        /// Gets or sets the name of the template.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the content of the template.
        /// </summary>
        public string EditorValue { get; set; }

        /// <summary>
        /// Gets or sets the version of the template.
        /// </summary>
        public int Version { get; set; }
    }
}