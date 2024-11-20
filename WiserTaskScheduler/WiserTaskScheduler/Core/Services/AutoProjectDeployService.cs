using System;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Services;

public class AutoProjectDeployService : IAutoProjectDeployService, ISingletonService
{
    private readonly WtsSettings wtsSettings;
    private readonly GclSettings gclSettings;
    private readonly IServiceProvider serviceProvider;
    private readonly string logName;

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    public AutoProjectDeployService(IOptions<WtsSettings> wtsSettings, IOptions<GclSettings> gclSettings, IServiceProvider serviceProvider)
    {
        this.wtsSettings = wtsSettings.Value;
        this.gclSettings = gclSettings.Value;
        this.serviceProvider = serviceProvider;

        logName = $"AutoProjectDeploy ({Environment.MachineName})";
    }

    /// <inheritdoc />
    public Task ManageAutoProjectDeploy()
    {
        // TODO: Get settings from the database

        // TODO: Check if the next update is in the future, if not return

        // TODO: Check if configurations need to be paused and wait till that moment

        // TODO: Pause configurations if needed

        // TODO: Wait till the update

        // TODO: Deploy the update

        // TODO: Resume configurations

        throw new NotImplementedException();
    }

    private Task<bool> CheckIfGithubActionsSucceededAsync(DateTime checkFrom, DateTime checkTill, TimeSpan interval)
    {
        // TODO: Get actions from Github to see if the action succeeded
        throw new NotImplementedException();
    }
}