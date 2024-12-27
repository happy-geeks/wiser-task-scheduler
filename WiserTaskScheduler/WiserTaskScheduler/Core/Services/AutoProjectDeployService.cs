using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Branches.Models;
using GeeksCoreLibrary.Modules.Communication.Enums;
using GeeksCoreLibrary.Modules.Communication.Models;
using IGclCommunicationsService = GeeksCoreLibrary.Modules.Communication.Interfaces.ICommunicationsService;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.WiserDashboard.Models;
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
    private static readonly int DefaultGitHubWorkflowTimeout = 15;
    private static readonly TimeSpan DefaultGitHubWorkflowCheckInterval = new TimeSpan(0, 1, 0);

    private List<KeyValuePair<string, TimeSpan>> stepTimes = new List<KeyValuePair<string, TimeSpan>>();
    private DateTime[] lastBranchSettingsUpdateTimes = new DateTime[2];

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
    public async Task ManageAutoProjectDeployAsync(CancellationToken stoppingToken)
    {
        stepTimes.Clear();
        var emailsForStatusUpdates = Array.Empty<string>();
        
        await using var scope = serviceProvider.CreateAsyncScope();
        
        try
        {
            var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
            var branchSettings = await wiserItemsService.GetItemDetailsAsync(uniqueId: DefaultBranchSettingsId, entityType: BranchSettingsEntityType, skipPermissionsCheck: true);
            emailsForStatusUpdates = branchSettings.GetDetailValue(EmailForStatusUpdatesProperty)?.Split(';');
            
            // Use the last update time to prevent setting errors from being triggered multiple times.
            lastBranchSettingsUpdateTimes[0] = lastBranchSettingsUpdateTimes[1];
            lastBranchSettingsUpdateTimes[1] = branchSettings.ChangedOn;
            
            await RunAutoProjectDeployAsync(scope, branchSettings, stoppingToken);
        }
        catch (Exception exception)
        {
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"An error occurred while trying to run the automatic project deployment:{Environment.NewLine}{exception.Message}", logName);
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "An error occurred while trying to run the automatic project deployment. Please check the logs for more information.");
        }
    }
    
    /// <summary>
    /// Run the automatic project deployment.
    /// </summary>
    /// <param name="scope">A <see cref="IServiceScope"/> to use to request services.</param>
    /// <param name="branchSettings">The settings for the deployment.</param>
    /// /// <param name="stoppingToken">The <see cref="CancellationToken"/> used for the current background service to indicate if it is being stopped.</param>
    private async Task RunAutoProjectDeployAsync(IServiceScope scope, WiserItemModel branchSettings, CancellationToken stoppingToken)
    {
        var wiserDashboardService = scope.ServiceProvider.GetRequiredService<IWiserDashboardService>();
        var allServices = await wiserDashboardService.GetServicesAsync(false);

        // Get the settings from the database.
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
        
        // Validate the settings to determine if the automatic deployment can be executed.
        if (!await ValidateSettingsAsync(scope, branchSettings, gitHubAccessTokenExpires, configurationsToPause, currentDateTime, deployStartDatetime, emailsForStatusUpdates, branchMergeTemplate))
        {
            return;
        }

        // If the time to pause or update is to far in the future we should stop and wait till a better moment. Once passed this point the provided settings are locked in.
        if ((configurationsToPause.Any() && DateTime.TryParse(branchSettings.GetDetailValue(ConfigurationsPauseDatetimeProperty), out var configurationsPauseDatetime) && configurationsPauseDatetime - currentDateTime > MaximumThreadSleepTime) || deployStartDatetime - currentDateTime > MaximumThreadSleepTime)
        {
            return;
        }

        // Pause configurations/services that need to be paused before the deployment. Any errors are logged and notified about inside the method.
        if (!await PauseConfigurationsAsync(branchSettings, scope, configurationsToPause, wiserDashboardService, currentDateTime, deployStartDatetime, emailsForStatusUpdates, allServices, stoppingToken))
        {
            return;
        }

        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Wait till the actual deployment starts at {deployStartDatetime:yyyy-MM-dd HH:mm:ss}.", logName);
        // Wait till the start datetime of the deployment.
        await TaskHelpers.WaitAsync(deployStartDatetime - DateTime.Now, stoppingToken);
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "The automatic deployment has started.", logName);
        
        var deployStopWatch = new Stopwatch();
        deployStopWatch.Start();
        
        var gitHubBaseUrl = $"https://api.github.com/repos/{gitHubOrganization}/{gitHubRepository}";
        
        // Disable the website according to the GitHub workflow to start the deployment.
        if (!await DispatchGitHubWorkflowEventAsync(gitHubBaseUrl, gitHubAccessToken, "disable-website", stoppingToken))
        {
            // If one or more of the GitHub workflows failed, we try to enable them again.
            if (!await DispatchGitHubWorkflowEventAsync(gitHubBaseUrl, gitHubAccessToken, "enable-website", stoppingToken))
            {
                deployStopWatch.Stop();
                stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
                await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, "The automatic deployment could not be executed, because the GitHub workflow failed to disable the website and could not enable it again.", logName);
                await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be executed, because the GitHub workflow failed to disable the website and could not enable it again. The website may still be in maintenance mode and manual actions are required.");
                return;
            }
            
            deployStopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, "The automatic deployment could not be executed, because the GitHub workflow failed to disable the website.", logName);
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be executed, because the GitHub workflow failed to disable the website. The website has been enabled again, a new attempt needs to be configured.");
            return;
        }
        
        var productionDatabaseConnection = scope.ServiceProvider.GetService<IDatabaseConnection>();
        var branchQueueService = scope.ServiceProvider.GetRequiredService<IBranchQueueService>();
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "Initializing the branch queue service.", logName);
        await ((IActionsService)branchQueueService).InitializeAsync(new ConfigurationModel()
        {
            ConnectionString = gclSettings.ConnectionString
        }, null);
        
        var mergeBranchSettings = await GetMergeBranchSettingsAsync(productionDatabaseConnection, branchMergeTemplate);
        if (mergeBranchSettings == null)
        {
            deployStopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, "The automatic deployment could not be executed, because the merge branch settings could not be retrieved.", logName);
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be executed, because the merge branch settings could not be retrieved. The website is still in maintenance mode and manual actions are required.");
            return;
        }
        
        mergeBranchSettings.StartOn = deployStartDatetime;
        var connectionStringBuilder = branchQueueService.GetConnectionStringBuilderForBranch(mergeBranchSettings, mergeBranchSettings.DatabaseName);
        
        await using var branchScope = serviceProvider.CreateAsyncScope();
        var branchDatabaseConnection = branchScope.ServiceProvider.GetService<IDatabaseConnection>();
        await branchDatabaseConnection.ChangeConnectionStringsAsync(connectionStringBuilder.ConnectionString);

        // Make a backup of Wiser history before the merge. If the table does already exist, it will be dropped and recreated.
        await BackupWiserHistoryAsync(branchDatabaseConnection, "before_merge");
        
        // Prepare the branch merge based on the template and start the merge process.
        wtsSettings.AutoProjectDeploy.BranchQueue.AutomaticDeployBranchQueueId = await PrepareBranchMergeAsync(productionDatabaseConnection, deployStartDatetime, mergeBranchSettings);
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "Starting the branch merge.", logName);
        var branchResult = await ((IActionsService)branchQueueService).Execute(wtsSettings.AutoProjectDeploy.BranchQueue, [], logName);
        
        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>("Branch merge", stopWatch.Elapsed));
        
        // Make a backup of Wiser history after the merge. If the table does already exist, it will be dropped and recreated.
        await BackupWiserHistoryAsync(branchDatabaseConnection, "after_merge");

        if (!(bool)branchResult.SelectToken("Results[0].Success"))
        {
            deployStopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"The automatic deployment could not be completed, because the branch merge failed. See the '{WiserTableNames.WiserBranchesQueue}' table for more information.", logName);
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be completed, because the branch merge failed. The website is still in maintenance mode and manual actions are required.");
            return;
        }
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "The branch merge has been completed successfully.", logName);
        
        var publishResult = await PublishWiserCommitsAsync();

        if (publishResult != HttpStatusCode.NoContent)
        {
            deployStopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
            // The log is inside the <see cref="PublishWiserCommitsAsync"/> method.
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be completed, because the Wiser commits could not be published. The website is still in maintenance mode and manual actions are required.");
            return;
        }
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "The Wiser commits have been published successfully.", logName);
        
        // Merge staging into main on GitHub
        var currentUtcTime = DateTime.UtcNow;
        var gitHubMergeResult = await MergeGitHubBranchAsync(gitHubBaseUrl, gitHubAccessToken);

        if (gitHubMergeResult != HttpStatusCode.Created && gitHubMergeResult != HttpStatusCode.NoContent)
        {
            deployStopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
            // The log is inside the <see cref="MergeGitHubBranchAsync"/> method.
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be completed, because the staging branch could not be merged into the main branch on GitHub. The website is still in maintenance mode and manual actions are required.");
            return;
        }

        if (gitHubMergeResult == HttpStatusCode.Created)
        {
            await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "The staging branch has been merged into the main branch on GitHub.", logName);
            
            if (!await CheckIfGithubWorkflowSucceededAsync(currentUtcTime, DateTime.Now.AddMinutes(DefaultGitHubWorkflowTimeout), DefaultGitHubWorkflowCheckInterval, gitHubBaseUrl, gitHubAccessToken, stoppingToken))
            {
                deployStopWatch.Stop();
                stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
                await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, "The automatic deployment could not be completed, because the GitHub workflow failed to merge the staging branch into the main branch.", logName);
                await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be completed, because the GitHub workflow failed to merge the staging branch into the main branch. The website is still in maintenance mode and manual actions are required.");
                return;
            }
        }
        else
        {
            await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "There were no commits to merge from staging to main. The GitHub deployment workflow is skipped.", logName);
        }

        await SetConfigurationsPauseStateAsync(configurationsToPause, wiserDashboardService, allServices, false);
        
        if (!await DispatchGitHubWorkflowEventAsync(gitHubBaseUrl, gitHubAccessToken, "enable-website", stoppingToken))
        {
            deployStopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, "The automatic deployment could not be completed, because the GitHub workflow failed to enable the website.", logName);
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment failed", "The automatic deployment could not be completed, because the GitHub workflow failed to enable the website. The website is still in maintenance mode and manual actions are required.");
            return;
        }
        
        deployStopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>("Total deployment", deployStopWatch.Elapsed));
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "The automatic deployment has been completed successfully.", logName);
        await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment succeeded", "The automatic deployment has been completed successfully. The website is now live and available for visitors.");
    }

    /// <summary>
    /// Validate the settings to determine if the automatic deployment can be executed.
    /// </summary>
    /// <param name="scope">A <see cref="IServiceScope"/> to use to request services.</param>
    /// <param name="branchSettings">The settings for the deployment.</param>
    /// <param name="gitHubAccessTokenExpires"></param>
    /// <param name="configurationsToPause">The configurations that need to be paused during deployment.</param>
    /// <param name="currentDateTime">The current date time to compare settings with.</param>
    /// <param name="deployStartDatetime">The time the deployment needs to start to validate the pause time.</param>
    /// <param name="emailsForStatusUpdates">The email addresses that need to receive an email when something went wrong.</param>
    /// <param name="branchMergeTemplate">The ID of the branch template that needs to be used for the merge.</param>
    /// <returns>Returns true if all checks have passed successfully.</returns>
    private async Task<bool> ValidateSettingsAsync(IServiceScope scope, WiserItemModel branchSettings, DateTime gitHubAccessTokenExpires, List<int> configurationsToPause, DateTime currentDateTime, DateTime deployStartDatetime, string[] emailsForStatusUpdates, int branchMergeTemplate)
    {
        var errors = new List<string>();
        
        if (deployStartDatetime > gitHubAccessTokenExpires)
        {
            errors.Add("The GitHub access token will be expired before the deploy can start.");
        }

        if (configurationsToPause.Any())
        {
            // If there are configurations that need to be paused, but the pause datetime is in the past, we should stop.
            if (DateTime.TryParse(branchSettings.GetDetailValue(ConfigurationsPauseDatetimeProperty), out var configurationsPauseDatetime) && configurationsPauseDatetime < currentDateTime)
            {
                errors.Add("The configurations pause datetime is set in the past.");
            }

            // Pausing configurations should always be done first, otherwise there's no point in pausing them.
            // So if the pause datetime is later than the deployment start datetime, we should stop.
            if (configurationsPauseDatetime > deployStartDatetime)
            {
                errors.Add("The configurations pause datetime is set later than the deployment start datetime.");
            }
        }
        
        // Check if the selected branch template is for the correct branch.
        var productionDatabaseConnection = scope.ServiceProvider.GetService<IDatabaseConnection>();
        var mergeBranchSettings = await GetMergeBranchSettingsAsync(productionDatabaseConnection, branchMergeTemplate);
        if (mergeBranchSettings == null)
        {
            errors.Add("The merge branch settings could not be retrieved.");
        }
        
        var branchName = branchSettings.GetDetailValue(BranchNameProperty);
        if (!mergeBranchSettings?.DatabaseName.EndsWith(branchName, StringComparison.OrdinalIgnoreCase) ?? true)
        {
            errors.Add("The selected branch template is not compatible with the provided branch to merge.");
        }

        if (!errors.Any())
        {
            return true;
        }

        // Only log the errors if the settings have changed since the last check.
        if (lastBranchSettingsUpdateTimes[0] != lastBranchSettingsUpdateTimes[1])
        {
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"The automatic deployment can not be executed due to incorrect settings.{Environment.NewLine}\t{String.Join($"{Environment.NewLine}\t", errors)}", logName);
            await SendMailAsync(scope, emailsForStatusUpdates, $"{wtsSettings.Wiser.Subdomain}: Automatic deployment incorrect configured", $"The automatic deployment can not be executed due to incorrect settings.<br/><ul><li>{String.Join("</li><li>", errors)}</li></ul>");
        }
        
        return false;
    }

    /// <summary>
    /// Pause configurations if needed before the deployment.
    /// </summary>
    /// <param name="branchSettings">The settings for the deployment.</param>
    /// <param name="scope">A <see cref="IServiceScope"/> to use to request services.</param>
    /// <param name="configurationsToPause">The configurations that need to be paused during deployment.</param>
    /// <param name="wiserDashboardService">The <see cref="IWiserDashboardService"/> to process changes to the configurations state.</param>
    /// <param name="currentDateTime">The current date time to compare settings with.</param>
    /// <param name="deployStartDatetime">The time the deployment needs to start to validate the pause time.</param>
    /// <param name="emailsForStatusUpdates">The email addresses that need to receive an email when something went wrong.</param>
    /// <param name="allServices">The WTS services running for this project.</param>
    /// <param name="stoppingToken">The <see cref="CancellationToken"/> used for the current background service to indicate if it is being stopped.</param>
    /// <returns>Returns true if nothing went wrong to indicate the deployment flow can continue.</returns>
    private async Task<bool> PauseConfigurationsAsync(WiserItemModel branchSettings, IServiceScope scope, List<int> configurationsToPause, IWiserDashboardService wiserDashboardService, DateTime currentDateTime, DateTime deployStartDatetime, string[] emailsForStatusUpdates, List<Service> allServices, CancellationToken stoppingToken)
    {
        // Check if configurations need to be paused and wait till that moment.
        if (!configurationsToPause.Any())
        {
            return true;
        }
        
        var configurationsPauseDatetime = DateTime.Parse(branchSettings.GetDetailValue(ConfigurationsPauseDatetimeProperty));
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Wait till the the configurations need to be paused at {configurationsPauseDatetime:yyyy-MM-dd HH:mm:ss}.", logName);
        // Wait till the configurations need to be paused.
        await TaskHelpers.WaitAsync(configurationsPauseDatetime - currentDateTime, stoppingToken);

        await SetConfigurationsPauseStateAsync(configurationsToPause, wiserDashboardService, allServices, true);

        return true;
    }

    /// <summary>
    /// Set the configurations to the paused or active state.
    /// </summary>
    /// <param name="configurationsToPause">The configurations that need to be paused during deployment.</param>
    /// <param name="wiserDashboardService">The <see cref="IWiserDashboardService"/> to process changes to the configurations state.</param>
    /// <param name="allServices">The WTS services running for this project.</param>
    /// <param name="pause">If the configurations need to be paused or set back to active.</param>
    private async Task SetConfigurationsPauseStateAsync(List<int> configurationsToPause, IWiserDashboardService wiserDashboardService, List<Service> allServices, bool pause)
    {
        if (!configurationsToPause.Any())
        {
            return;
        }
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
    
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Setting the configurations to the {(pause ? "paused" : "active")} state. Services to pause according to the {WiserTableNames.WtsServices} IDs: {String.Join(',', configurationsToPause)}", logName);
        
        // Resume configurations/services that have been paused for the deployment.
        foreach (var configurationId in configurationsToPause)
        {
            var service = allServices.SingleOrDefault(s => s.Id == configurationId);
            if (service == null)
            {
                await logService.LogWarning(logger, LogScopes.RunBody, LogSettings, $"The service with ID {configurationId} is set to be paused before automatic project deploy, but this service could not be found.", logName);
                continue;
            }

            await wiserDashboardService.UpdateServiceAsync(service.Configuration, service.TimeId, paused: pause, state: pause ? "paused" : "active");
        }
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"The configurations have been set to the {(pause ? "paused" : "active")} state.", logName);
        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>($"{(pause ? "Pause" : "Resume")} configurations", stopWatch.Elapsed));
    }

    /// <summary>
    /// Dispatch an event to start a GitHub workflow.
    /// </summary>
    /// <param name="gitHubBaseUrl">The base URL to use for API calls to the GitHub API.</param>
    /// <param name="gitHubAccessToken">The access token to use for the GitHub API.</param>
    /// <param name="workflowName">The name of the workflow to dispatch the event for.</param>
    /// <param name="stoppingToken">The <see cref="CancellationToken"/> used for the current background service to indicate if it is being stopped.</param>
    /// <returns></returns>
    private async Task<bool> DispatchGitHubWorkflowEventAsync(string gitHubBaseUrl, string gitHubAccessToken, string workflowName, CancellationToken stoppingToken)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Dispatching the event to start the GitHub workflow '{workflowName}'.", logName);
        var currentUtcTime = DateTime.UtcNow;
        var httpRequest = new HttpRequestMessage(HttpMethod.Post, $"{gitHubBaseUrl}/actions/workflows/{workflowName}.yml/dispatches");
        httpRequest.Headers.Add("Authorization", $"Bearer {gitHubAccessToken}");
        httpRequest.Headers.Add("Accept", "application/vnd.github+json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
        httpRequest.Content = JsonContent.Create(new
        {
            @ref = "main"
        });
        
        await httpClientService.Client.SendAsync(httpRequest);
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"The event to start the GitHub workflow '{workflowName}' has been dispatched.", logName);

        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>($"Dispatched GitHub workflow '{workflowName}'", stopWatch.Elapsed));
        
        return await CheckIfGithubWorkflowSucceededAsync(currentUtcTime, DateTime.Now.AddMinutes(DefaultGitHubWorkflowTimeout), DefaultGitHubWorkflowCheckInterval, gitHubBaseUrl, gitHubAccessToken, stoppingToken);
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
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "Getting the merge branch settings.", logName);

        databaseConnection.AddParameter("BranchMergeTemplate", branchMergeTemplate);
        var query = $"SELECT data FROM {WiserTableNames.WiserBranchesQueue} WHERE id = @BranchMergeTemplate;";
        
        var dataTable = await databaseConnection.GetAsync(query);
        if (dataTable.Rows.Count == 0 || dataTable.Rows[0]["data"] == DBNull.Value)
        {
            return null;
        }
        
        var data = dataTable.Rows[0]["data"].ToString();
        var result = JsonConvert.DeserializeObject<MergeBranchSettingsModel>(data!);
        
        // Clear template data to prepare for the merge.
        result.TemplateId = 0;
        result.IsTemplate = false;
        result.TemplateName = null;

        return result;
    }

    /// <summary>
    /// Makes a backup of the Wiser history table.
    /// </summary>
    /// <param name="databaseConnection">The database connection for the database where the backup needs to be made.</param>
    /// <param name="backupTableSuffix">The suffix for the backup table.</param>
    private async Task BackupWiserHistoryAsync(IDatabaseConnection databaseConnection, string backupTableSuffix)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Making a backup of the Wiser history table with suffix '{backupTableSuffix}'.", logName);
        
        var query = $"""
                     DROP TABLE IF EXISTS _{WiserTableNames.WiserHistory}_{backupTableSuffix};
                     CREATE TABLE _{WiserTableNames.WiserHistory}_{backupTableSuffix} AS TABLE {WiserTableNames.WiserHistory};
                     INSERT INTO _{WiserTableNames.WiserHistory}_{backupTableSuffix} SELECT * FROM {WiserTableNames.WiserHistory};
                     """;
        
        await databaseConnection.ExecuteAsync(query);
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"The backup of the Wiser history table with suffix '{backupTableSuffix}' has been made.", logName);
        
        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>($"Backup Wiser history table with suffix '{backupTableSuffix}'", stopWatch.Elapsed));
    }

    /// <summary>
    /// Prepares the branch merge based on the provided template.
    /// </summary>
    /// <param name="databaseConnection">The database connection where the merge needs to be prepared, e.g. the production database.</param>
    /// <param name="deployStartDatetime">The date and time the automatic deployment started.</param>
    /// <param name="mergeBranchSettings">The settings for the merge, based on the original template.</param>
    private async Task<int> PrepareBranchMergeAsync(IDatabaseConnection databaseConnection, DateTime deployStartDatetime, MergeBranchSettingsModel mergeBranchSettings)
    {
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Preparing the branch merge based on branch template ID {mergeBranchSettings.Id}.", logName);
        databaseConnection.AddParameter("name", $"Auto-deployment - {deployStartDatetime:yyyy-MM-dd HH:mm:ss}");
        databaseConnection.AddParameter("branchId", mergeBranchSettings.Id);
        databaseConnection.AddParameter("data", JsonConvert.SerializeObject(mergeBranchSettings));
        
        var query = $"""
                     INSERT INTO {WiserTableNames.WiserBranchesQueue} (name, branch_id, action, data, added_by, user_id, is_for_automatic_deploy)
                     VALUES (@name, @branchId, 'merge', @data, 'Wiser Task Scheduler', 0, 1);
                     
                     SELECT LAST_INSERT_ID() AS id;
                     """;
        
        var dataTable = await databaseConnection.GetAsync(query);
        var result = Convert.ToInt32(dataTable.Rows[0]["id"]);
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"The branch merge has been prepared under ID {result}.", logName);
        
        return result;
    }

    /// <summary>
    /// Checks if the GitHub workflow succeeded.
    /// </summary>
    /// <param name="checkFromUtcTime">The time in UTC from when to request workflows.</param>
    /// <param name="checkTillMachineTime">The time until the check needs to be executed before considering it a fail. This is local machine time.</param>
    /// <param name="interval">The interval between checks.</param>
    /// <param name="gitHubBaseUrl">The base URL to use for API calls to the GitHub API.</param>
    /// <param name="gitHubAccessToken">The access token to use for the GitHub API.</param>
    /// <param name="stoppingToken">The <see cref="CancellationToken"/> used for the current background service to indicate if it is being stopped.</param>
    /// <returns>Returns true if the workflows have all been completed, false if at least one failed or something went wrong.</returns>
    private async Task<bool> CheckIfGithubWorkflowSucceededAsync(DateTime checkFromUtcTime, DateTime checkTillMachineTime, TimeSpan interval, string gitHubBaseUrl, string gitHubAccessToken, CancellationToken stoppingToken)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        // Wait before first check to give the runner(s) time to start.
        await Task.Delay(interval, stoppingToken);

        // Check if the GitHub actions succeeded.
        while (DateTime.Now <= checkTillMachineTime)
        {
            await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Checking if the GitHub actions have succeeded between UTC {checkFromUtcTime:HH:mm:ss} and server time {checkTillMachineTime:HH:mm:ss}.", logName);
            var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"{gitHubBaseUrl}/actions/runs?branch=main&created={checkFromUtcTime:yyyy-MM-ddTHH:mm:ssZ}..{checkFromUtcTime.AddDays(1):yyyy-MM-ddTHH:mm:ssZ}");
            httpRequest.Headers.Add("Authorization", $"Bearer {gitHubAccessToken}");
            httpRequest.Headers.Add("Accept", "application/vnd.github+json");
            httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
            
            var response = await httpClientService.Client.SendAsync(httpRequest);

            if (!response.IsSuccessStatusCode)
            {
                await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"Failed to get the Github actions to check if the actions succeeded, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'.", logName);
                stopWatch.Stop();
                stepTimes.Add(new KeyValuePair<string, TimeSpan>("Check GitHub workflow succeeded", stopWatch.Elapsed));
                return false;
            }
            
            var body = await response.Content.ReadAsStringAsync();
            var result = JToken.Parse(body);
            var workflowRuns = result["workflow_runs"] as JArray;

            // Wait till all the runs are completed. If no runs are found yet the Github API might still be processing it.
            if (workflowRuns == null || !workflowRuns.Any() || workflowRuns.All(wr => wr["status"].Value<string>() != "completed"))
            {
                await Task.Delay(interval, stoppingToken);
                continue;
            }
            
            stopWatch.Stop();
            stepTimes.Add(new KeyValuePair<string, TimeSpan>("Check GitHub workflow succeeded", stopWatch.Elapsed));
            return workflowRuns.All(wr => wr["conclusion"].Value<string>() == "success");
        }

        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>("Check GitHub workflow succeeded", stopWatch.Elapsed));
        return false;
    }
    
    /// <summary>
    /// Publishes the Wiser commits to the production environment.
    /// </summary>
    /// <returns>Returns the status code from the Wiser API.</returns>
    private async Task<HttpStatusCode> PublishWiserCommitsAsync()
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "Retrieving access token for the Wiser API.", logName);
        var accessToken = await wiserService.GetAccessTokenAsync();
        var wiserApiBaseUrl = $"{wtsSettings.Wiser.WiserApiUrl}{(wtsSettings.Wiser.WiserApiUrl.EndsWith('/') ? "" : "/")}api/v3/version-control";
        
        var (commitsToPublish, retrievedCommitsStatusCode) = await GetWiserCommitsToPublishAsync(accessToken, wiserApiBaseUrl);
        
        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>("Retrieve Wiser commits to publish", stopWatch.Elapsed));
        
        if (retrievedCommitsStatusCode != HttpStatusCode.OK)
        {
            return retrievedCommitsStatusCode;
        }
        
        stopWatch.Restart();
        
        var httpRequest = new HttpRequestMessage(HttpMethod.Put, $"{wiserApiBaseUrl}/deploy");
        httpRequest.Headers.Add("Authorization", $"Bearer {accessToken}");
        httpRequest.Headers.Add("Accept", "application/json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
        httpRequest.Content = JsonContent.Create(new
        {
            environment = "Live",
            commitIds = commitsToPublish
        });
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Publishing the following Wiser commits to the production environment: {String.Join(',', commitsToPublish)}", logName);
        var response = await httpClientService.Client.SendAsync(httpRequest);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"Failed to publish the Wiser commits to the production environment, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'. The following body was returned:{Environment.NewLine}{body}", logName);
        }

        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>("Publish Wiser commits", stopWatch.Elapsed));
        return response.StatusCode;
    }

    /// <summary>
    /// Get all commits in Wiser that need to be published and are currently published to the acceptance environment.
    /// </summary>
    /// <param name="accessToken">The access token for the Wiser API.</param>
    /// <param name="wiserApiBaseUrl">The base URL to the Wiser API.</param>
    /// <returns>Returns a list with all commit IDs that need to be published.</returns>
    private async Task<(List<int> commitIds, HttpStatusCode statusCode)> GetWiserCommitsToPublishAsync(string accessToken, string wiserApiBaseUrl)
    {
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "Retrieving the commits from Wiser that need to be published.", logName);
        
        var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"{wiserApiBaseUrl}/not-completed-commits");
        httpRequest.Headers.Add("Authorization", $"Bearer {accessToken}");
        httpRequest.Headers.Add("Accept", "application/json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");
        
        var response = await httpClientService.Client.SendAsync(httpRequest);
        var body = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"Failed to get the commits from Wiser that need to be published, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'. The following body was returned:{Environment.NewLine}{body}", logName);
            return (null, response.StatusCode);
        }
        var result = JArray.Parse(body);
        return (result.Where(x => (bool) x["isAcceptance"]).Select(x => (int)x["id"]).ToList(), response.StatusCode);
    }

    /// <summary>
    /// Merges the staging branch into the main branch on GitHub.
    /// </summary>
    /// <param name="gitHubBaseUrl">The base URL to the GitHub API.</param>
    /// <param name="gitHubAccessToken">The access token for GitHub to authenticate with.</param>
    /// <returns>Returns the <see cref="HttpStatusCode"/> from the GitHub API.</returns>
    private async Task<HttpStatusCode> MergeGitHubBranchAsync(string gitHubBaseUrl, string gitHubAccessToken)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, "Merging the staging branch into the main branch on GitHub.", logName);
        
        var httpRequest = new HttpRequestMessage(HttpMethod.Post, $"{gitHubBaseUrl}/merges");
        httpRequest.Headers.Add("Authorization", $"Bearer {gitHubAccessToken}");
        httpRequest.Headers.Add("Accept", "application/json");
        httpRequest.Headers.Add("User-Agent", "Wiser Task Scheduler");

        httpRequest.Content = JsonContent.Create(new
        {
            @base = "main",
            head = "staging",
            commit_message = "Automatic deployment using the WTS"
        });
        
        var response = await httpClientService.Client.SendAsync(httpRequest);
        
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            await logService.LogCritical(logger, LogScopes.RunBody, LogSettings, $"Failed to merge the staging branch into the main branch on GitHub, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'. The following body was returned:{Environment.NewLine}{body}", logName);
        }
        
        stopWatch.Stop();
        stepTimes.Add(new KeyValuePair<string, TimeSpan>("Merge staging branch into main branch on GitHub", stopWatch.Elapsed));
        
        return response.StatusCode;
    }

    /// <summary>
    /// Sends an email to the provided recipients with the subject and body to give an update about the automatic deployment.
    /// </summary>
    /// <param name="scope">A <see cref="IServiceScope"/> to use to get the <see cref="IGclCommunicationsService"/> to send the email.</param>
    /// <param name="recipients">The recipients to receive the email.</param>
    /// <param name="subject">The subject for the email.</param>
    /// <param name="body">The body/content of the email.</param>
    private async Task SendMailAsync(IServiceScope scope, string[] recipients, string subject, string body)
    {
        try
        {
            if (recipients.Length == 0)
            {
                await logService.LogWarning(logger, LogScopes.RunBody, LogSettings, "No email addresses provided to send the email to.", logName);
                return;
            }
            
            await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Sending an email with the subject '{subject}'.", logName);

            if (stepTimes.Any())
            {
                body += $"<br/><br/>The following steps took place during the automatic deployment:<br/><table><tr><th>Action:</th><th>Time:</th></tr>{String.Join("", stepTimes.Select(x => $"<tr><td>{x.Key}</td><td>{x.Value}</td></tr>"))}</table>";
            }
            
            var communicationsService = scope.ServiceProvider.GetRequiredService<IGclCommunicationsService>();
            var receivers = recipients.Select(email => new CommunicationReceiverModel() {Address = email}).ToList();

            var email = new SingleCommunicationModel()
            {
                Type = CommunicationTypes.Email,
                Receivers = receivers,
                Cc = new List<string>(),
                Bcc = new List<string>(),
                Subject = subject,
                Content = body,
                Sender = gclSettings.SmtpSettings.SenderEmailAddress,
                SenderName = gclSettings.SmtpSettings.SenderName
            };

            await communicationsService.SendEmailDirectlyAsync(email, gclSettings.SmtpSettings);
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Failed to send an email to '{recipients}' with the subject '{subject}' due to exception:{Environment.NewLine}{exception}.", logName);
        }
    }
}