using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Helpers;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Core.Services
{
    /// <summary>
    /// A service for a configuration.
    /// </summary>
    public class ConfigurationsService : IConfigurationsService, IScopedService
    {
        private readonly ILogService logService;
        private readonly ILogger<ConfigurationsService> logger;
        private readonly IActionsServiceFactory actionsServiceFactory;

        private readonly SortedList<int, ActionModel> actions;
        private readonly Dictionary<string, IActionsService> actionsServices;

        private string configurationServiceName;

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
        public ConfigurationsService(ILogService logService, ILogger<ConfigurationsService> logger, IActionsServiceFactory actionsServiceFactory)
        {
            this.logService = logService;
            this.logger = logger;
            this.actionsServiceFactory = actionsServiceFactory;

            actions = new SortedList<int, ActionModel>();
            actionsServices = new Dictionary<string, IActionsService>();
        }

        /// <inheritdoc />
        public void ExtractActionsFromConfiguration(int timeId, ConfigurationModel configuration)
        {
            configurationServiceName = configuration.ServiceName;
            var allActions = GetAllActionsFromConfiguration(configuration);

            foreach (ActionModel action in allActions.Where(action => action.TimeId == timeId))
            {
                action.LogSettings ??= LogSettings;
                actions.Add(action.Order, action);

                if (!actionsServices.ContainsKey(action.GetType().ToString()))
                {
                    var actionsService = actionsServiceFactory.GetActionsServiceForAction(action);
                    actionsService.Initialize(configuration);
                    actionsServices.Add(action.GetType().ToString(), actionsService);
                }
            }

            logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"{Name} has {actions.Count} action(s).", configurationServiceName, timeId);
        }

        /// <summary>
        /// Get all the provided action sets if they exist in a single list.
        /// </summary>
        /// <returns></returns>
        private List<ActionModel> GetAllActionsFromConfiguration(ConfigurationModel configuration)
        {
            var actionSets = new List<ActionModel[]>
            {
                configuration.Queries,
                configuration.HttpApis,
                configuration.GenerateFileModels,
                configuration.ImportFileModels,
                configuration.Communications
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

            return allActions;
        }

        /// <inheritdoc />
        public bool IsValidConfiguration(ConfigurationModel configuration)
        {
            var conflicts = 0;

            // Check for duplicate run scheme time ids.
            var runSchemeTimeIds = new List<int>();

            foreach (var runScheme in configuration.RunSchemes)
            {
                runSchemeTimeIds.Add(runScheme.TimeId);
            }

            var duplicateTimeIds = runSchemeTimeIds.GroupBy(id => id).Where(id => id.Count() > 1).Select(id => id.Key).ToList();

            if (duplicateTimeIds.Count > 0)
            {
                conflicts++;
                logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Configuration '{configuration.ServiceName}' has duplicate run scheme time ids: {String.Join(", ", duplicateTimeIds)}", configuration.ServiceName);
            }

            // Check for duplicate order in a single time id.
            var allActions = GetAllActionsFromConfiguration(configuration);

            foreach (var timeId in runSchemeTimeIds)
            {
                var duplicateOrders = allActions.Where(action => action.TimeId == timeId).GroupBy(action => action.Order).Where(action => action.Count() > 1).Select(action => action.Key).ToList();

                if (duplicateOrders.Count > 0)
                {
                    conflicts++;
                    logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Configuration '{configuration.ServiceName}' has duplicate orders within run scheme {timeId}. Orders: {String.Join(", ", duplicateOrders)}", configuration.ServiceName, timeId);
                }
            }

            return conflicts == 0;
        }

        /// <inheritdoc />
        public async Task ExecuteAsync()
        {
            var resultSets = new JObject();

            foreach (var action in actions)
            {
                if (SkipAction(resultSets, action.Value))
                {
                    continue;
                }

                var resultSet = await actionsServices[action.Value.GetType().ToString()].Execute(action.Value, resultSets, configurationServiceName);

                if (!String.IsNullOrWhiteSpace(action.Value.ResultSetName))
                {
                    resultSets.Add(action.Value.ResultSetName, resultSet);
                }
            }
        }

        /// <summary>
        /// Check if the action needs to be skipped due to constraints when it is allowed to run.
        /// </summary>
        /// <param name="resultSets">The result sets of the previous actions within the run to reference.</param>
        /// <param name="action">The action to perform the check on.</param>
        /// <returns></returns>
        private bool SkipAction(JObject resultSets, ActionModel action)
        {
            if (!String.IsNullOrWhiteSpace(action.OnlyWithStatusCode))
            {
                var parts = action.OnlyWithStatusCode.Split(",");

                if ((string)ResultSetHelper.GetCorrectObject<JObject>(parts[0], ReplacementHelper.EmptyRows, resultSets)["StatusCode"] != parts[1])
                {
                    return true;
                }
            }

            if (!String.IsNullOrWhiteSpace(action.OnlyWithSuccessState))
            {
                var parts = action.OnlyWithSuccessState.Split(",");

                if ((string) ResultSetHelper.GetCorrectObject<JObject>(parts[0], ReplacementHelper.EmptyRows, resultSets)["Success"] != parts[1])
                {
                    return true;
                }
            }

            return false;
        }
    }
}
