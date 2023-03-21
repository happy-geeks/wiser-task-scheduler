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
        /// The value "\t" can be used for tab-separated files.
        /// </summary>
        public string Separator { get; set; } = ",";

        /// <summary>
        /// Gets or sets if the first row of the file are field names.
        /// If set to true, then columns can be requested by field name. Otherwise the index needs to be given to request a column.
        /// </summary>
        public bool HasFieldNames { get; set; } = true;

        /// <summary>
        /// Gets or sets whether a single file is imported (true) or multiple files based on a dataset (false).
        /// </summary>
        public bool SingleFile { get; set; } = true;
    }
}
