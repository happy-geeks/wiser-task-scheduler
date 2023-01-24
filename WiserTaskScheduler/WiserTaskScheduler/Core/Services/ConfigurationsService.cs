﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Services
{
    /// <summary>
    /// A service for a configuration.
    /// </summary>
    public class ConfigurationsService : IConfigurationsService, IScopedService
    {
        private readonly ILogService logService;
        private readonly ILogger<ConfigurationsService> logger;
        private readonly IActionsServiceFactory actionsServiceFactory;
        private readonly IErrorNotificationService errorNotificationService;

        private readonly SortedList<int, ActionModel> actions;
        private readonly Dictionary<string, IActionsService> actionsServices;

        private string configurationServiceName;
        private int timeId;
        private string serviceFailedNotificationEmails;

        /// <inheritdoc />
        public LogSettings LogSettings { get; set; }

        /// <inheritdoc />
        public string Name { get; set; }

        /// <summary>
        /// Creates a new instance of <see cref="ConfigurationsService"/>.
        /// </summary>
        /// <param name="logService">The service to use for logging.</param>
        /// <param name="logger"></param>
        /// <param name="actionsServiceFactory"></param>
        public ConfigurationsService(ILogService logService, ILogger<ConfigurationsService> logger, IActionsServiceFactory actionsServiceFactory, IErrorNotificationService errorNotificationService)
        {
            this.logService = logService;
            this.logger = logger;
            this.actionsServiceFactory = actionsServiceFactory;
            this.errorNotificationService = errorNotificationService;

            actions = new SortedList<int, ActionModel>();
            actionsServices = new Dictionary<string, IActionsService>();
        }

        /// <inheritdoc />
        public async Task ExtractActionsFromConfigurationAsync(int timeId, ConfigurationModel configuration)
        {
            configurationServiceName = configuration.ServiceName;
            this.timeId = timeId;
            serviceFailedNotificationEmails = configuration.ServiceFailedNotificationEmails;
            var allActions = GetAllActionsFromConfiguration(configuration);

            foreach (ActionModel action in allActions.Where(action => action.TimeId == timeId))
            {
                action.LogSettings ??= LogSettings;
                actions.Add(action.Order, action);

                if (!actionsServices.ContainsKey(action.GetType().ToString()))
                {
                    var actionsService = actionsServiceFactory.GetActionsServiceForAction(action);
                    await actionsService.InitializeAsync(configuration);
                    actionsServices.Add(action.GetType().ToString(), actionsService);
                }
            }

            await logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"{Name} has {actions.Count} action(s).", configurationServiceName, timeId);
        }

        /// <summary>
        /// Get all the provided action sets if they exist in a single list.
        /// </summary>
        /// <returns></returns>
        private List<ActionModel> GetAllActionsFromConfiguration(ConfigurationModel configuration)
        {
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
                configuration.CleanupWiserHistoryGroup,
                configuration.CleanupWiserHistories
            };

            var allActions = new List<ActionModel>();

            if (actions == null)
            {
                return allActions;
            }

            foreach (var actionSet in actionSets)
            {
                if (actionSet != null)
                {
                    allActions.AddRange(actionSet);
                }
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
            var runSchemeTimeIds = new List<int>();
            
            foreach (var runScheme in configuration.GetAllRunSchemes())
            {
                runSchemeTimeIds.Add(runScheme.TimeId);
            }

            var duplicateTimeIds = runSchemeTimeIds.GroupBy(id => id).Where(id => id.Count() > 1).Select(id => id.Key).ToList();

            if (duplicateTimeIds.Count > 0)
            {
                conflicts++;
                await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Configuration '{configuration.ServiceName}' has duplicate run scheme time ids: {String.Join(", ", duplicateTimeIds)}", configuration.ServiceName);
            }

            // Check for duplicate order in a single time id.
            var allActions = GetAllActionsFromConfiguration(configuration);

            foreach (var timeId in runSchemeTimeIds)
            {
                var duplicateOrders = allActions.Where(action => action.TimeId == timeId).GroupBy(action => action.Order).Where(action => action.Count() > 1).Select(action => action.Key).ToList();

                if (duplicateOrders.Count > 0)
                {
                    conflicts++;
                    await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Configuration '{configuration.ServiceName}' has duplicate orders within run scheme {timeId}. Orders: {String.Join(", ", duplicateOrders)}", configuration.ServiceName, timeId);
                }
            }

            return conflicts == 0;
        }

        /// <inheritdoc />
        public async Task ExecuteAsync()
        {
            var resultSets = new JObject();
            var currentOrder = 0;
            var stopwatch = new Stopwatch();

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
            }
            catch (Exception e)
            {
                await logService.LogCritical(logger, LogScopes.StartAndStop, LogSettings, $"Aborted {configurationServiceName} due to exception in time ID '{timeId}' and order '{currentOrder}', will try again next time. Exception {e}", configurationServiceName, timeId, currentOrder);
                await errorNotificationService.NotifyOfErrorByEmailAsync(serviceFailedNotificationEmails, $"Service '{configurationServiceName}' with time ID '{timeId}' failed.", $"Wiser Task Scheduler failed during the executing of service '{configurationServiceName}' with time ID '{timeId}' and has therefore been aborted. Please check the logs for more details. A new attempt will be made during the next run.", LogSettings, LogScopes.RunStartAndStop, configurationServiceName);
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
                    var statusCode = (string) ResultSetHelper.GetCorrectObject<JObject>($"{parts[0]}", ReplacementHelper.EmptyRows, resultSets)["StatusCode"];
                    
                    if (statusCode != parts[1])
                    {
                        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Skipped action because status code was '{statusCode}'.", configurationServiceName, action.TimeId, action.Order);
                        return true;
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Failed to validate status code, skipping action. Exception: {e}", configurationServiceName, action.TimeId, action.Order);
                    return true;
                }
            }

            if (!String.IsNullOrWhiteSpace(action.OnlyWithSuccessState))
            {
                var parts = action.OnlyWithSuccessState.Split(",");

                try
                {
                    var state = (string) ResultSetHelper.GetCorrectObject<JObject>($"{parts[0]}", ReplacementHelper.EmptyRows, resultSets)["Success"];
                    
                    if (state != parts[1])
                    {
                        await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Skipped action because success state was '{state}'.", configurationServiceName, action.TimeId, action.Order);
                        return true;
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Failed to validate action success, skipping action. Exception: {e}", configurationServiceName, action.TimeId, action.Order);
                    return true;
                }
            }

            return false;
        }
    }
}
