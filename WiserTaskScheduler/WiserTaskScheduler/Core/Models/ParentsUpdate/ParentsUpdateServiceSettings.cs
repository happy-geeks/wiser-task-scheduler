using System;
using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Enums;
using WiserTaskScheduler.Modules.RunSchemes.Models;

namespace WiserTaskScheduler.Core.Models.ParentsUpdate
{
    public class ParentsUpdateServiceSettings
    {
        /// <summary>
        /// Gets or sets the run scheme for the <see cref="UpdateParentsWorker"/>.
        /// </summary>
        public RunSchemeModel RunScheme { get; set; } = new RunSchemeModel()
        {
            Type = RunSchemeTypes.Continuous,
            Delay = TimeSpan.FromSeconds(60)
        };
        
        /// <summary>
        /// Gets or sets the array of databases that need to receive a parent update as well
        /// note: these databases need to be in the same cluster
        /// </summary>
        public string[] AdditionalDatabases { get; set; }

        /// <summary>
        /// Gets or sets the setting for performing a optimize table command every X times
        /// set to 0 for never performing optimize default is 100
        /// </summary>
        public int PerformOptimizeEveryXtimes { get; set; } = 100;

        /// <summary>
        /// Gets or sets the log level for the parent service
        /// </summary>
        public LogSettings LogSettings { get; set; } = new LogSettings()
        {
            LogMinimumLevel = LogLevel.None
        };
    }
}
