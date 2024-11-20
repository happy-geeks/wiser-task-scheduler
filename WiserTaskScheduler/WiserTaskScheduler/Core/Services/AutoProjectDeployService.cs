using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Core.Services;

public class AutoProjectDeployService : IAutoProjectDeployService, ISingletonService
{
    private readonly WtsSettings wtsSettings;
    private readonly GclSettings gclSettings;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogService logService;
    private readonly ILogger<AutoProjectDeployService> logger;
    private readonly string logName;

    private const string BranchSettingsEntityType = "branch_settings";
    private const string DefaultBranchSettingsId = "default_branch_settings";

    private const string BranchNameProperty = "branch_name";
    private const string BranchMergeTemplateProperty = "branch_merge_template";
    private const string EmailForStatusUpdatesProperty = "email_for_status_updates";
    private const string GitHubAccessTokenProperty = "github_access_token";
    private const string GitHubAccessTokenExpiresProperty = "github_access_token_expires";
    private const string GitHubRepositoryProperty = "github_repository";
    private const string GitHubOrganizationProperty = "github_organization";
    private const string ConfigurationsToPauseProperty = "configurations_to_pause";
    private const string ConfigurationsPauseDatetimeProperty = "configurations_pause_datetime";
    private const string DeployStartDatetimeProperty = "deploy_start_datetime";

    private static readonly TimeSpan MaximumThreadSleepTime = new TimeSpan(6, 0, 0);

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    /// <summary>
    /// Creates a new instance of <see cref="AutoProjectDeployService" />.
    /// </summary>
    public AutoProjectDeployService(IOptions<WtsSettings> wtsSettings, IOptions<GclSettings> gclSettings, IServiceProvider serviceProvider, ILogService logService, ILogger<AutoProjectDeployService> logger)
    {
        this.wtsSettings = wtsSettings.Value;
        this.gclSettings = gclSettings.Value;
        this.serviceProvider = serviceProvider;
        this.logService = logService;
        this.logger = logger;

        logName = $"AutoProjectDeploy ({Environment.MachineName})";
    }

    /// <inheritdoc />
    public async Task ManageAutoProjectDeployAsync()
    {
        // TODO: I found the "CreateAsyncScope" randomly. Normally we use "CreateScope" instead. But after reading the descriptions of the two, I think "CreateAsyncScope" is the better one. Not sure though, so check if that's actually true and if it actually works.
        await using var scope = serviceProvider.CreateAsyncScope();
        var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
        var wiserDashboardService = scope.ServiceProvider.GetRequiredService<IWiserDashboardService>();
        var allServices = await wiserDashboardService.GetServicesAsync(false);

        // Get the settings from the database.
        var branchSettings = await wiserItemsService.GetItemDetailsAsync(uniqueId: DefaultBranchSettingsId, entityType: BranchSettingsEntityType, skipPermissionsCheck: true);
        var branchName = branchSettings.GetDetailValue(BranchNameProperty);
        var branchMergeTemplate = branchSettings.GetDetailValue<int>(BranchMergeTemplateProperty);
        var emailsForStatusUpdates = branchSettings.GetDetailValue(EmailForStatusUpdatesProperty)?.Split(';');
        var githubAccessToken = branchSettings.GetDetailValue(GitHubAccessTokenProperty)?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
        var githubAccessTokenExpires = branchSettings.GetDetailValue<DateTime>(GitHubAccessTokenExpiresProperty);
        var githubRepository = branchSettings.GetDetailValue(GitHubRepositoryProperty);
        var githubOrganization = branchSettings.GetDetailValue(GitHubOrganizationProperty);
        var configurationsToPause = branchSettings.GetDetailValue(ConfigurationsToPauseProperty)?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(Int32.Parse).ToList() ?? new List<int>();
        var currentDateTime = DateTime.Now;

        // If the start date is in the past, we should stop.
        if (!DateTime.TryParse(branchSettings.GetDetailValue(DeployStartDatetimeProperty), out var deployStartDatetime) || deployStartDatetime < currentDateTime)
        {
            return;
        }

        // Check if configurations need to be paused and wait till that moment.
        if (configurationsToPause.Any())
        {
            // If there are configurations that need to be paused, but the pause datetime is in the past, we should stop.
            if (DateTime.TryParse(branchSettings.GetDetailValue(ConfigurationsPauseDatetimeProperty), out var configurationsPauseDatetime) && configurationsPauseDatetime < currentDateTime)
            {
                return;
            }

            // Pausing configurations should always be done first, otherwise there's no point in pausing them.
            // So if the pause datetime is later than the deployment start datetime, we should stop.
            if (configurationsPauseDatetime > deployStartDatetime)
            {
                await logService.LogError(logger, LogScopes.RunBody, wtsSettings.AutoProjectDeploySettings.LogSettings, "The automatic deployment could not be executed, because the settings are invalid (the configurations pause datetime is set later than the deployment start datetime).", logName);
                return;
            }

            // This is to prevent the thread from sleeping for too long.
            var timeToWaitToPauzeConfigurations = configurationsPauseDatetime - currentDateTime;
            if (timeToWaitToPauzeConfigurations > MaximumThreadSleepTime)
            {
                return;
            }

            // Wait till the configurations need to be paused.
            // TODO: Pass cancellation token from somewhere? MainWorker/MainService? I don't know.
            await TaskHelpers.WaitAsync(timeToWaitToPauzeConfigurations, default);

            // Pause configurations/services.
            foreach (var configurationId in configurationsToPause)
            {
                var service = allServices.SingleOrDefault(s => s.Id == configurationId);
                if (service == null)
                {
                    await logService.LogWarning(logger, LogScopes.RunBody, wtsSettings.AutoProjectDeploySettings.LogSettings, $"The service with ID {configurationId} is set to be paused before automatic project deploy, but this service could not be found.", logName);
                    continue;
                }

                await wiserDashboardService.UpdateServiceAsync(service.Configuration, service.TimeId, paused: true, state: "paused");
            }
        }

        // Wait till the start datetime of the deployment.
        // TODO: Pass cancellation token from somewhere? MainWorker/MainService? I don't know.
        await TaskHelpers.WaitAsync(deployStartDatetime - DateTime.Now, default);

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