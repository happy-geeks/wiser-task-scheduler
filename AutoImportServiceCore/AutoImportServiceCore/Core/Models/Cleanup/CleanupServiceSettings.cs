using AutoImportServiceCore.Core.Workers;
using AutoImportServiceCore.Modules.RunSchemes.Enums;
using AutoImportServiceCore.Modules.RunSchemes.Models;

namespace AutoImportServiceCore.Core.Models.Cleanup
{
    public class CleanupServiceSettings
    {
        /// <summary>
        /// Gets or sets the paths where files are written to. If set it will delete all files in the folders that are older than <see cref="NumberOfDaysToStore"/>.
        /// </summary>
        public string[] FileFolderPaths { get; set; }

        /// <summary>
        /// Gets or sets the number of days logs need to be kept.
        /// </summary>
        public int NumberOfDaysToStore { get; set; } = 14;

        /// <summary>
        /// Gets or sets the run scheme for the <see cref="CleanupWorker"/>.
        /// </summary>
        public RunSchemeModel RunScheme { get; set; } = new RunSchemeModel()
        {
            Type = RunSchemeTypes.Daily
        };
    }
}
