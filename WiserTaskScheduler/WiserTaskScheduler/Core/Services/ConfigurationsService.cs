using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Services;

/// <summary>
/// A service for a configuration.
/// </summary>
public class ConfigurationsService(
    ILogService logService,
    ILogger<ConfigurationsService> logger,
    IActionsServiceFactory actionsServiceFactory,
    IErrorNotificationService errorNotificationService,
    IDatabaseHelpersService databaseHelpersService,
    IOptions<WtsSettings> wtsSettings)
    : IConfigurationsService, IScopedService
{
    private readonly WtsSettings wtsSettings = wtsSettings.Value;

    private readonly SortedList<int, ActionModel> actions = new();
    private readonly Dictionary<string, IActionsService> actionsServices = new();

    private string configurationServiceName;
    private int timeId;
    private string serviceFailedNotificationEmails;

    private readonly HashSet<string> tablesToOptimize = [];

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    /// <inheritdoc />
    public string Name { get; set; }

    /// <inheritdoc />
    public bool HasAction => actions.Any();

    /// <inheritdoc />
    public async Task ExtractActionsFromConfigurationAsync(int timeIdValue, ConfigurationModel configuration)
    {
        configurationServiceName = configuration.ServiceName;
        timeId = timeIdValue;
        serviceFailedNotificationEmails = configuration.ServiceFailedNotificationEmails;
        var allActions = GetAllActionsFromConfiguration(configuration);

        foreach (var action in allActions.Where(action => action.TimeId == timeIdValue))
        {
            action.LogSettings ??= LogSettings;
            actions.Add(action.Order, action);

            if (actionsServices.ContainsKey(action.GetType().ToString()))
            {
                continue;
            }

            var actionsService = actionsServiceFactory.GetActionsServiceForAction(action);
            await actionsService.InitializeAsync(configuration, tablesToOptimize);
            actionsServices.Add(action.GetType().ToString(), actionsService);
        }

        if (!actions.Any())
        {
            await logService.LogWarning(logger, LogScopes.StartAndStop, LogSettings, $"{configurationServiceName} has no actions for time ID '{timeIdValue}'. Please make sure the time ID of the run scheme and actions are filled in correctly.", configurationServiceName, timeIdValue);
            return;
        }

        await logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"{configurationServiceName} has {actions.Count} action(s) for time ID '{timeIdValue}'.", configurationServiceName, timeIdValue);
    }

    /// <summary>
    /// Get all the provided action sets if they exist in a single list.
    /// </summary>
    /// <returns></returns>
    private List<ActionModel> GetAllActionsFromConfiguration(ConfigurationModel configuration)
    {
        // ReSharper disable CoVariantArrayConversion
        var actionSets = new List<ActionModel[]>
        {
            configuration.QueryGroup,
            configuration.Queries,
            configuration.HttpApiGroup,
            configuration.HttpApis,
            configuration.GenerateFileGroup,
            configuration.GenerateFiles,
            configuration.ImportFileGroup,
            configuration.ImportFiles,
            configuration.CleanupItemGroup,
            configuration.CleanupItems,
            configuration.CommunicationGroup,
            configuration.Communications,
            configuration.WiserImportGroup,
            configuration.WiserImports,
            configuration.ServerMonitorsGroup,
            configuration.ServerMonitor,
            configuration.FtpGroup,
            configuration.Ftps,
            configuration.CleanupWiserHistoryGroup,
            configuration.CleanupWiserHistories,
            configuration.GenerateCommunicationGroup,
            configuration.GenerateCommunications,
            configuration.DocumentStoreReadersGroup,
            configuration.DocumentStoreReader,
            configuration.SlackMessageGroup,
            configuration.SlackMessages,
            configuration.ScriptsGroup,
            configuration.Scripts
        };

        var allActions = new List<ActionModel>();

        if (actions == null)
        {
            return allActions;
        }

        foreach (var actionSet in actionSets.Where(actionSet => actionSet != null))
        {
            allActions.AddRange(actionSet);
        }

        if (configuration.BranchQueueModel != null)
        {
            allActions.Add(configuration.BranchQueueModel);
        }

        return allActions;
    }

    /// <inheritdoc />
    public async Task<bool> IsValidConfigurationAsync(ConfigurationModel configuration)
    {
        var conflicts = 0;

        // Check for duplicate run scheme time ids.
        var runSchemeTimeIds = configuration.GetAllRunSchemes().Select(runScheme => runScheme.TimeId).ToList();

        var duplicateTimeIds = runSchemeTimeIds.GroupBy(id => id).Where(id => id.Count() > 1).Select(id => id.Key).ToList();

        if (duplicateTimeIds.Count > 0)
        {
            conflicts++;
            await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Configuration '{configuration.ServiceName}' has duplicate run scheme time ids: {String.Join(", ", duplicateTimeIds)}", configuration.ServiceName);
        }

        // Check for duplicate order in a single time id.
        var allActions = GetAllActionsFromConfiguration(configuration);

        foreach (var runSchemeTimeId in runSchemeTimeIds)
        {
            var duplicateOrders = allActions.Where(action => action.TimeId == runSchemeTimeId).GroupBy(action => action.Order).Where(action => action.Count() > 1).Select(action => action.Key).ToList();

            if (duplicateOrders.Count <= 0)
            {
                continue;
            }

            conflicts++;
            await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Configuration '{configuration.ServiceName}' has duplicate orders within run scheme {runSchemeTimeId}. Orders: {String.Join(", ", duplicateOrders)}", configuration.ServiceName, runSchemeTimeId);
        }

        return conflicts == 0;
    }

    /// <inheritdoc />
    public async Task ExecuteAsync()
    {
        var resultSets = new JObject();
        var currentOrder = 0;
        var stopwatch = new Stopwatch();
        tablesToOptimize.Clear();

        try
        {
            foreach (var action in actions)
            {
                stopwatch.Reset();

                currentOrder = action.Value.Order;

                if (await SkipAction(resultSets, action.Value))
                {
                    continue;
                }

                stopwatch.Start();
                var resultSet = await actionsServices[action.Value.GetType().ToString()].Execute(action.Value, resultSets, configurationServiceName);

                if (!String.IsNullOrWhiteSpace(action.Value.ResultSetName))
                {
                    resultSets.Add(action.Value.ResultSetName, resultSet);
                }

                stopwatch.Stop();
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Action finished in {stopwatch.Elapsed}", configurationServiceName, timeId, action.Value.Order);
            }

            if (tablesToOptimize.Count != 0)
            {
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Optimizing tables: {String.Join(',', tablesToOptimize)}", configurationServiceName, timeId);
                await databaseHelpersService.OptimizeTablesAsync(tablesToOptimize.ToArray());
            }
        }
        catch (Exception exception)
        {
            await logService.LogCritical(logger, LogScopes.StartAndStop, LogSettings, $"Aborted {configurationServiceName} due to exception in time ID '{timeId}' and order '{currentOrder}', will try again next time. Exception {exception}", configurationServiceName, timeId, currentOrder);
            await errorNotificationService.NotifyOfErrorByEmailAsync(serviceFailedNotificationEmails, $"Service '{configurationServiceName}' with time ID '{timeId}' of Wiser Task Scheduler '{wtsSettings.Name}' failed.", $"Wiser Task Scheduler '{Environment.MachineName} - {wtsSettings.Name}' failed during the executing of service '{configurationServiceName}' with time ID '{timeId}' and has therefore been aborted. Please check the logs for more details. A new attempt will be made during the next run.", LogSettings, LogScopes.RunStartAndStop, configurationServiceName);
        }
    }

    /// <summary>
    /// Check if the action needs to be skipped due to constraints when it is allowed to run.
    /// </summary>
    /// <param name="resultSets">The result sets of the previous actions within the run to reference.</param>
    /// <param name="action">The action to perform the check on.</param>
    /// <returns></returns>
    private async Task<bool> SkipAction(JObject resultSets, ActionModel action)
    {
        if (!String.IsNullOrWhiteSpace(action.OnlyWithStatusCode))
        {
            var parts = action.OnlyWithStatusCode.Split(",");

            try
            {
                var defaultValue = String.Empty;
                var defaultValueIndex = parts[0].IndexOf('?');
                if (defaultValueIndex >= 0)
                {
                    defaultValue = parts[0][defaultValueIndex..];
                    parts[0] = parts[0][..defaultValueIndex];
                }

                var statusCode = ReplacementHelper.GetValue($"{parts[0]}.StatusCode{defaultValue}", ReplacementHelper.EmptyRows, resultSets, false);

                if (statusCode != parts[1])
                {
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Skipped action because status code was '{statusCode}'.", configurationServiceName, action.TimeId, action.Order);
                    return true;
                }
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Failed to validate status code, skipping action. Exception: {e}", configurationServiceName, action.TimeId, action.Order);
                return true;
            }
        }

        if (!String.IsNullOrWhiteSpace(action.OnlyWithSuccessState))
        {
            var parts = action.OnlyWithSuccessState.Split(",");

            try
            {
                var defaultValue = String.Empty;
                var defaultValueIndex = parts[0].IndexOf('?');
                if (defaultValueIndex >= 0)
                {
                    defaultValue = parts[0][defaultValueIndex..];
                    parts[0] = parts[0][..defaultValueIndex];
                }

                var state = ReplacementHelper.GetValue($"{parts[0]}.Success{defaultValue}", ReplacementHelper.EmptyRows, resultSets, false);

                if (state != parts[1])
                {
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Skipped action because success state was '{state}'.", configurationServiceName, action.TimeId, action.Order);
                    return true;
                }
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Failed to validate action success, skipping action. Exception: {e}", configurationServiceName, action.TimeId, action.Order);
                return true;
            }
        }

        if (!String.IsNullOrWhiteSpace(action.OnlyWithValue))
        {
            var parts = action.OnlyWithValue.Split(",");

            try
            {
                var defaultValue = String.Empty;
                var defaultValueIndex = parts[0].IndexOf('?');
                if (defaultValueIndex >= 0)
                {
                    defaultValue = parts[0][defaultValueIndex..];
                    parts[0] = parts[0][..defaultValueIndex];
                }

                var state = ReplacementHelper.GetValue($"{parts[0]}{defaultValue}", ReplacementHelper.EmptyRows, resultSets, false);

                if (parts.Length < 2)
                {
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, "Skipped action because no value was provided to check state against (did you forget a comma?).", configurationServiceName, action.TimeId, action.Order);
                    return true;
                }

                if (state != parts[1])
                {
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Skipped action because success state was '{state}' and not {parts[1]}.", configurationServiceName, action.TimeId, action.Order);
                    return true;
                }
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Failed to validate action value state, skipping action. Exception: {e}", configurationServiceName, action.TimeId, action.Order);
                return true;
            }
        }

        return false;
    }
}