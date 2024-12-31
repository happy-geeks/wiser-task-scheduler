using System;
using Microsoft.Extensions.Logging;
using WiserTaskScheduler.Modules.Branches.Models;
using WiserTaskScheduler.Modules.RunSchemes.Enums;
using WiserTaskScheduler.Modules.RunSchemes.Models;

namespace WiserTaskScheduler.Core.Models.AutoProjectDeploy;

public class AutoProjectDeploySettings
{
    public bool IsEnabled { get; set; }

    /// <summary>
    /// Gets or sets the branch queue settings for the auto project deploy service.
    /// </summary>
    public BranchQueueModel BranchQueue { get; set; } = new BranchQueueModel();

    /// <summary>
    /// Gets or sets the run scheme settings for the auto project deploy service.
    /// </summary>
    public RunSchemeModel RunScheme { get; set; } = new RunSchemeModel()
    {
        Type = RunSchemeTypes.Continuous,
        Delay = new TimeSpan(0, 5, 0),
        RunImmediately = true
    };
}