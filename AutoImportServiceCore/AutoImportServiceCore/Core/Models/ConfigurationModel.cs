using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using AutoImportServiceCore.Modules.Communications.Models;
using AutoImportServiceCore.Modules.GenerateFiles.Models;
using AutoImportServiceCore.Modules.HttpApis.Models;
using AutoImportServiceCore.Modules.ImportFiles.Models;
using AutoImportServiceCore.Modules.Queries.Models;
using AutoImportServiceCore.Modules.RunSchemes.Models;

namespace AutoImportServiceCore.Core.Models
{
    /// <summary>
    /// A model for the configuration.
    /// </summary>
    [XmlRoot("Configuration")]
    public class ConfigurationModel
    {
        /// <summary>
        /// Gets or sets the service name.
        /// </summary>
        [Required]
        public string ServiceName { get; set; }

        /// <summary>
        /// Version will be automatically set, either to the template version or the ticks of the last write time for a local file.
        /// </summary>
        public long Version { get; set; }

        /// <summary>
        /// Gets or sets the connection string that is used for queries.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the log settings.
        /// </summary>
        public LogSettings LogSettings { get; set; }

        /// <summary>
        /// Gets or sets the run scheme.
        /// </summary>
        [Required]
        [XmlArray("RunSchemes")]
        [XmlArrayItem(typeof(RunSchemeModel))]
        public RunSchemeModel[] RunSchemes { get; set; }

        /// <summary>
        /// Gets or sets the queries.
        /// </summary>
        [XmlArray("Queries")]
        [XmlArrayItem(typeof(QueryModel))]
        public QueryModel[] Queries { get; set; }

        /// <summary>
        /// Gets or sets the HTTP APIs.
        /// </summary>
        [XmlArray("HttpApis")]
        [XmlArrayItem(typeof(HttpApiModel))]
        public HttpApiModel[] HttpApis { get; set; }

        /// <summary>
        /// Gets or sets the generate files.
        /// </summary>
        [XmlArray("GenerateFiles")]
        [XmlArrayItem(typeof(GenerateFileModel))]
        public GenerateFileModel[] GenerateFileModels { get; set; }

        /// <summary>
        /// Gets or sets the import files.
        /// </summary>
        [XmlArray("ImportFiles")]
        [XmlArrayItem(typeof(ImportFileModel))]
        public ImportFileModel[] ImportFileModels { get; set; }

        /// <summary>
        /// Gets or sets the communications.
        /// </summary>
        [XmlArray("Communications")]
        [XmlArrayItem(typeof(CommunicationModel))]
        public CommunicationModel[] Communications { get; set; }
    }
}
