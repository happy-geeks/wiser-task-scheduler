using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.ImportFiles.Models
{
    [XmlType("ImportFile")]
    public class ImportFileModel : ActionModel
    {
        /// <summary>
        /// Gets or sets the full path to the file.
        /// </summary>
        public string FilePath { get; set; }

        /// <summary>
        /// Gets or sets the separator to split a line on.
        /// \t is a supported value for tab-separated files.
        /// </summary>
        public string Separator { get; set; } = ",";

        /// <summary>
        /// Gets or sets if the first row of the file are field names.
        /// If true columns can be requested by field name, if false the index need to be given to request a column.
        /// </summary>
        public bool HasFieldNames { get; set; } = true;

        /// <summary>
        /// Gets or sets if a single file is imported.
        /// </summary>
        public bool SingleFile { get; set; } = true;
    }
}
