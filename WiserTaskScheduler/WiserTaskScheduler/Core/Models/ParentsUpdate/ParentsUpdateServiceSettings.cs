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
        /// Gets or sets the run scheme for the <see cref="ParentsUpdateWorker"/>.
        /// </summary>
        public RunSchemeModel RunScheme { get; set; } = new RunSchemeModel()
        {
            Type = RunSchemeTypes.Continuous,
            Delay = TimeSpan.FromSeconds(60)
        };

        public LogSettings LogSettings { get; set; } = new LogSettings()
        {
            LogMinimumLevel = LogLevel.None
        };
    }
}
