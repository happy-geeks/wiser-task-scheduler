using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Branches.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Core.Services;

public class AutoProjectDeployService : IAutoProjectDeployService, ISingletonService
{
    private readonly WtsSettings wtsSettings;
    private readonly GclSettings gclSettings;
    private readonly IServiceProvider serviceProvider;
    private readonly IHttpClientService httpClientService;
    private readonly IWiserService wiserService;
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
    public AutoProjectDeployService(IOptions<WtsSettings> wtsSettings, IOptions<GclSettings> gclSettings, IServiceProvider serviceProvider, IHttpClientService httpClientService, IWiserService wiserService, ILogService logService, ILogger<AutoProjectDeployService> logger)
    {
        this.wtsSettings = wtsSettings.Value;
        this.gclSettings = gclSettings.Value;
        this.serviceProvider = serviceProvider;
        this.httpClientService = httpClientService;
        this.wiserService = wiserService;
        this.logService = logService;
        this.logger = logger;

        logName = $"AutoProjectDeploy ({Environment.MachineName})";
    }

    /// <inheritdoc />
    public async Task ManageAutoProjectDeployAsync()
    {
        // Set the action to process automatic deploy branches instead of the normal branches. This object is always used for this flow.
        wtsSettings.AutoProjectDeploySettings.BranchQueue.ProcessAutomaticDeployBranches = true;
        
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
        var gitHubAccessToken = branchSettings.GetDetailValue(GitHubAccessTokenProperty)?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
        var gitHubAccessTokenExpires = branchSettings.GetDetailValue<DateTime>(GitHubAccessTokenExpiresProperty);
        var gitHubRepository = branchSettings.GetDetailValue(GitHubRepositoryProperty);
        var gitHubOrganization = branchSettings.GetDetailValue(GitHubOrganizationProperty);
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
            var timeToWaitToPauseConfigurations = configurationsPauseDatetime - currentDateTime;
            if (timeToWaitToPauseConfigurations > MaximumThreadSleepTime)
            {
                return;
            }

            // Wait till the configurations need to be paused.
            // TODO: Pass cancellation token from somewhere? MainWorker/MainService? I don't know.
            await TaskHelpers.WaitAsync(timeToWaitToPauseConfigurations, default);

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
        var gitHubBaseUrl = $"https://api.github.com/repos/{gitHubOrganization}/{gitHubRepository}/actions";
        
        // Disable the website according to the GitHub workflow to start the deployment.
        if (!await DisableWebsiteAsync(gitHubBaseUrl, gitHubAccessToken))
        {
            // TODO: Enable website again in case multiple servers are running and not all of them failed.
            await logService.LogError(logger, LogScopes.RunBody, wtsSettings.AutoProjectDeploySettings.LogSettings, "The automatic deployment could not be executed, because the GitHub workflow failed to disable the website.", logName);
            return;
        }
        
        var branchQueueService = scope.ServiceProvider.GetRequiredService<IBranchQueueService>();
        var productionDatabaseConnection = scope.ServiceProvider.GetService<IDatabaseConnection>();
        
        var mergeBranchSettings = await GetMergeBranchSettingsAsync(productionDatabaseConnection, branchMergeTemplate);
        var connectionStringBuilder = branchQueueService.GetConnectionStringBuilderForBranch(mergeBranchSettings, mergeBranchSettings.DatabaseName);
        
        var branchDatabaseConnection = scope.ServiceProvider.GetService<IDatabaseConnection>();
        
        await branchDatabaseConnection.ChangeConnectionStringsAsync(connectionStringBuilder.ConnectionString);

        // Make a backup of Wiser history before the merge. If the table does already exist, it will be dropped and recreated.
        await BackupWiserHistoryAsync(branchDatabaseConnection, "before_merge");
        
        // TODO: Merge the branch
        var branchResult = await ((IActionsService)branchQueueService).Execute(wtsSettings.AutoProjectDeploySettings.BranchQueue, [], logName);
        
        // Make a backup of Wiser history after the merge. If the table does already exist, it will be dropped and recreated.
        await BackupWiserHistoryAsync(branchDatabaseConnection, "after_merge");

        if (!branchResult.TryGetValue("Success", out var success) || !(bool) success)
        {
            await logService.LogError(logger, LogScopes.RunBody, wtsSettings.AutoProjectDeploySettings.LogSettings, "The automatic deployment could not be executed, because the branch merge failed.", logName);
            // TODO: send mail
            return;
        }
        
        var publishResult = await PublishWiserCommitsAsync();

        if (publishResult != HttpStatusCode.OK)
        {
            // TODO: Send error mail.
        }
        
        // TODO: Merge staging into main on GitHub

        // TODO: Resume configurations

        throw new NotImplementedException();
    }

    /// <summary>
    /// Disables the website according to the GitHub workflow to start the deployment.
    /// </summary>
    /// <param name="gitHubBaseUrl">The base URL to use for API calls to the GitHub API.</param>
    /// <param name="gitHubAccessToken">The access token to use for the GitHub API.</param>
    /// <returns></returns>
    private async Task<bool> DisableWebsiteAsync(string gitHubBaseUrl, string gitHubAccessToken)
    {
        var currentUtcTime = DateTime.UtcNow;
        var httpRequest = new HttpRequestMessage(HttpMethod.Post, $"{gitHubBaseUrl}/disable-website.yml/dispatches");
        httpRequest.Headers.Add("Authorization", $"Bearer {gitHubAccessToken}");
        httpRequest.Headers.Add("Accept", "application/vnd.github+json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
        httpRequest.Content = JsonContent.Create(new
        {
            @ref = "main"
        });
        
        await httpClientService.Client.SendAsync(httpRequest);

        return await CheckIfGithubWorkflowSucceededAsync(currentUtcTime, DateTime.Now.AddMinutes(15), new TimeSpan(0, 0, 1), gitHubBaseUrl, gitHubAccessToken);
    }
    
    /// <summary>
    /// Gets the merge branch settings from the database based on the selected template.
    /// </summary>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="branchMergeTemplate"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    private async Task<MergeBranchSettingsModel> GetMergeBranchSettingsAsync(IDatabaseConnection databaseConnection, int branchMergeTemplate)
    {
        databaseConnection.AddParameter("BranchMergeTemplate", branchMergeTemplate);
        var query = $"SELECT data FROM {WiserTableNames.WiserBranchesQueue} WHERE id = @BranchMergeTemplate;";
        
        var dataTable = await databaseConnection.GetAsync(query);
        if (dataTable.Rows.Count == 0 || dataTable.Rows[0]["data"] == DBNull.Value)
        {
            throw new NotImplementedException();
        }
        
        var data = dataTable.Rows[0]["data"].ToString();
        return JsonConvert.DeserializeObject<MergeBranchSettingsModel>(data!);
    }

    private async Task BackupWiserHistoryAsync(IDatabaseConnection databaseConnection, string backupTableSuffix)
    {
        return;
        // TODO: Connect to branch database for backups. await databaseConnection.ChangeConnectionStringsAsync();
        
        var query = $"""
                     DROP TABLE IF EXISTS _{WiserTableNames.WiserHistory}_{backupTableSuffix};
                     CREATE TABLE _{WiserTableNames.WiserHistory}_{backupTableSuffix} AS TABLE {WiserTableNames.WiserHistory};
                     INSERT INTO _{WiserTableNames.WiserHistory}_{backupTableSuffix} SELECT * FROM {WiserTableNames.WiserHistory};
                     """;
        
        await databaseConnection.ExecuteAsync(query);
    }

    /// <summary>
    /// Checks if the GitHub workflow succeeded.
    /// </summary>
    /// <param name="checkFromUtcTime">The time in UTC from when to request workflows.</param>
    /// <param name="checkTillMachineTime">The time until the check needs to be executed before considering it a fail. This is local machine time.</param>
    /// <param name="interval">The interval between checks.</param>
    /// <param name="gitHubBaseUrl">The base URL to use for API calls to the GitHub API.</param>
    /// <param name="gitHubAccessToken">The access token to use for the GitHub API.</param>
    /// <returns>Returns true if the workflows have all been completed, false if at least one failed or something went wrong.</returns>
    private async Task<bool> CheckIfGithubWorkflowSucceededAsync(DateTime checkFromUtcTime, DateTime checkTillMachineTime, TimeSpan interval, string gitHubBaseUrl, string gitHubAccessToken)
    {
        // Wait before first check to give the runner(s) time to start.
        await Task.Delay(interval);

        // Check if the Github actions succeeded.
        while (DateTime.Now <= checkTillMachineTime)
        {
            var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"{gitHubBaseUrl}/runs?branch=main&created>={checkFromUtcTime:yyyy-MM-ddTHH:mm:ssZ}}}");
            httpRequest.Headers.Add("Authorization", $"Bearer {gitHubAccessToken}");
            httpRequest.Headers.Add("Accept", "application/vnd.github+json");
            httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
            
            var response = await httpClientService.Client.SendAsync(httpRequest);

            if (!response.IsSuccessStatusCode)
            {
                await logService.LogError(logger, LogScopes.RunBody, wtsSettings.AutoProjectDeploySettings.LogSettings, $"Failed to get the Github actions to check if the actions succeeded, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'.", logName);
                return false;
            }
            
            var body = await response.Content.ReadAsStringAsync();
            var result = JToken.Parse(body);
            var workflowRuns = result["workflow_runs"] as JArray;

            // Wait till all the runs are completed. If no runs are found yet the Github API might still be processing it.
            if (!workflowRuns.Any() || workflowRuns.All(wr => wr["status"].Value<string>() != "completed"))
            {
                await Task.Delay(interval);
                continue;
            }
            
            return workflowRuns.All(wr => wr["conclusion"].Value<string>() == "success");
        }

        return false;
    }
    
    /// <summary>
    /// Publishes the Wiser commits to the production environment.
    /// </summary>
    /// <returns>Returns the status code from the Wiser API.</returns>
    private async Task<HttpStatusCode> PublishWiserCommitsAsync()
    {
        var accessToken = await wiserService.GetAccessTokenAsync();
        var wiserApiBaseUrl = $"{wtsSettings.Wiser.WiserApiUrl}/api/v3/version-control";
        
        var commitsToPublish = await GetWiserCommitsToPublishAsync(accessToken, wiserApiBaseUrl);
        
        var httpRequest = new HttpRequestMessage(HttpMethod.Put, $"{wiserApiBaseUrl}/deploy");
        httpRequest.Headers.Add("Authorization", $"Bearer {accessToken}");
        httpRequest.Headers.Add("Accept", "application/json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
        httpRequest.Content = JsonContent.Create(new
        {
            environment = "Live",
            commitIds = String.Join(',', commitsToPublish)
        });
        
        var response = await httpClientService.Client.SendAsync(httpRequest);
        return response.StatusCode;
    }

    /// <summary>
    /// Get all commits in Wiser that need to be published and are currently published to the acceptance environment.
    /// </summary>
    /// <param name="accessToken">The access token for the Wiser API.</param>
    /// <param name="wiserApiBaseUrl">The base URL to the Wiser API.</param>
    /// <returns>Returns a list with all commit IDs that need to be published.</returns>
    private async Task<List<int>> GetWiserCommitsToPublishAsync(string accessToken, string wiserApiBaseUrl)
    {
        var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"{wiserApiBaseUrl}/not-completed-commits");
        httpRequest.Headers.Add("Authorization", $"Bearer {accessToken}");
        httpRequest.Headers.Add("Accept", "application/json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
        
        var response = await httpClientService.Client.SendAsync(httpRequest);

        if (!response.IsSuccessStatusCode)
        {
            throw new NotImplementedException();
        }
        
        var body = await response.Content.ReadAsStringAsync();
        var result = JArray.Parse(body);
        return result.Where(x => (bool) x["isAcceptance"]).Select(x => (int)x["id"]).ToList();
    }
}