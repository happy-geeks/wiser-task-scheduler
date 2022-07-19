using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using AutoImportServiceCore.Modules.CleanupItems.Models;
using AutoImportServiceCore.Modules.Branches.Models;
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
        /// Gets or sets the run schemes that have been placed in the group.
        /// </summary>
        [Required]
        [XmlArray("RunSchemes")]
        [XmlArrayItem(typeof(RunSchemeModel))]
        public RunSchemeModel[] RunSchemeGroup { get; set; }

        /// <summary>
        /// Gets or sets the run schemes that have been placed outside the group..
        /// </summary>
        [XmlElement("RunScheme")]
        public RunSchemeModel[] RunSchemes { get; set; }

        /// <summary>
        /// Gets or sets the queries that have been placed in the group.
        /// </summary>
        [XmlArray("Queries")]
        [XmlArrayItem(typeof(QueryModel))]
        public QueryModel[] QueryGroup { get; set; }

        /// <summary>
        /// Gets or sets the queries that have been placed outside the group.
        /// </summary>
        [XmlElement("Query")]
        public QueryModel[] Queries { get; set; }

        /// <summary>
        /// Gets or sets the HTTP APIs that have been placed in the group.
        /// </summary>
        [XmlArray("HttpApis")]
        [XmlArrayItem(typeof(HttpApiModel))]
        public HttpApiModel[] HttpApiGroup { get; set; }

        /// <summary>
        /// Gets or sets the HTTP APIs that have been placed outside the group.
        /// </summary>
        [XmlElement("HttpApi")]
        public HttpApiModel[] HttpApis { get; set; }

        /// <summary>
        /// Gets or sets the generate files that have been placed in the group.
        /// </summary>
        [XmlArray("GenerateFiles")]
        [XmlArrayItem(typeof(GenerateFileModel))]
        public GenerateFileModel[] GenerateFileGroup { get; set; }

        /// <summary>
        /// Gets or sets the generate files that have been placed outside the group.
        /// </summary>
        [XmlElement("GenerateFile")]
        public GenerateFileModel[] GenerateFiles { get; set; }

        /// <summary>
        /// Gets or sets the import files that have been placed in the group.
        /// </summary>
        [XmlArray("ImportFiles")]
        [XmlArrayItem(typeof(ImportFileModel))]
        public ImportFileModel[] ImportFileGroup { get; set; }

        /// <summary>
        /// Gets or sets the import files that have been placed outside the group.
        /// </summary>
        [XmlElement("ImportFile")]
        public ImportFileModel[] ImportFiles { get; set; }

        /// <summary>
        /// Gets or sets the items to be cleaned from an entity that have been placed in the group.
        /// </summary>
        [XmlArray("CleanupItems")]
        [XmlArrayItem(typeof(CleanupItemModel))]
        public CleanupItemModel[] CleanupItemGroup { get; set; }

        /// <summary>
        /// Gets or sets the items to be cleaned from an entity that have been placed outside the group.
        /// </summary>
        [XmlElement("CleanupItem")]
        public CleanupItemModel[] CleanupItems { get; set; }

        /// <summary>
        /// Gets or sets the communications that have been placed in the group.
        /// </summary>
        [XmlArray("Communications")]
        [XmlArrayItem(typeof(CommunicationModel))]
        public CommunicationModel[] CommunicationGroup { get; set; }

        /// <summary>
        /// Gets or sets the communications that have been placed outside the group.
        /// </summary>
        [XmlElement("Communication")]
        public CommunicationModel[] Communications { get; set; }

        /// <summary>
        /// Gets or sets the branch queue models.
        /// </summary>
        [XmlElement("BranchQueue")]
        public BranchQueueModel BranchQueueModel { get; set; }

        /// <summary>
        /// Get all run schemes that are defined in this configuration.
        /// Will combine the run schemes inside and outside the group.
        /// </summary>
        /// <returns>Returns all the run schemes in this configuration.</returns>
        public List<RunSchemeModel> GetAllRunSchemes()
        {
            var runSchemes = new List<RunSchemeModel>();
            if (RunSchemeGroup != null)
            {
                runSchemes.AddRange(RunSchemeGroup);
            }

            if (RunSchemes != null)
            {
                runSchemes.AddRange(RunSchemes);
            }

            return runSchemes;
        }
    }
}
