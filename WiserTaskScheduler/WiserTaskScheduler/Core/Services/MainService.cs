﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Serialization;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Models.OAuth;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Models;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Core.Services
{
    /// <summary>
    /// A service to manage all WTS configurations that are provided by Wiser.
    /// </summary>
    public class MainService : IMainService, ISingletonService
    {
        private const string LogName = "MainService";

        private readonly WtsSettings wtsSettings;
        private readonly GclSettings gclSettings;
        private readonly IServiceProvider serviceProvider;
        private readonly IWiserService wiserService;
        private readonly IOAuthService oAuthService;
        private readonly IWiserDashboardService wiserDashboardService;
        private readonly ILogService logService;
        private readonly ILogger<MainService> logger;
        private readonly IErrorNotificationService errorNotificationService;

        private readonly ConcurrentDictionary<string, ActiveConfigurationModel> activeConfigurations;

        private long oAuthConfigurationVersion;
        
        private bool updatedServiceTable;

        /// <inheritdoc />
        public LogSettings LogSettings { get; set; }

        /// <summary>
        /// Creates a new instance of <see cref="MainService"/>.
        /// </summary>
        public MainService(IOptions<WtsSettings> wtsSettings, IOptions<GclSettings> gclSettings, IServiceProvider serviceProvider, IWiserService wiserService, IOAuthService oAuthService, IWiserDashboardService wiserDashboardService, ILogService logService, ILogger<MainService> logger, IErrorNotificationService errorNotificationService)
        {
            this.wtsSettings = wtsSettings.Value;
            this.gclSettings = gclSettings.Value;
            this.serviceProvider = serviceProvider;
            this.wiserService = wiserService;
            this.oAuthService = oAuthService;
            this.wiserDashboardService = wiserDashboardService;
            this.logService = logService;
            this.logger = logger;
            this.errorNotificationService = errorNotificationService;

            activeConfigurations = new ConcurrentDictionary<string, ActiveConfigurationModel>();
        }

        /// <inheritdoc />
        public async Task ManageConfigurations()
        {
            using var scope = serviceProvider.CreateScope();
            
            // Update service table if it has not already been done since launch. The table definitions can only change when the WTS restarts with a new update.
            if (!updatedServiceTable)
            {
                var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
                await databaseHelpersService.CheckAndUpdateTablesAsync(new List<string> {WiserTableNames.WtsServices});
                updatedServiceTable = true;
            }
            
            var configurations = await GetConfigurationsAsync();

            if (configurations == null)
            {
                return;
            }

            foreach (var configuration in configurations)
            {
                if (activeConfigurations.ContainsKey(configuration.ServiceName))
                {
                    // If configuration is already running on the same version skip it.
                    if (activeConfigurations[configuration.ServiceName].Version == configuration.Version)
                    {
                        continue;
                    }

                    // If the configuration is already running but on a different version stop the current active one.
                    var configurationStopTasks = await StopConfigurationAsync(configuration.ServiceName);
                    await WaitTillConfigurationsStoppedAsync(configurationStopTasks);
                    activeConfigurations.TryRemove(new KeyValuePair<string, ActiveConfigurationModel>(configuration.ServiceName, activeConfigurations[configuration.ServiceName]));
                }

                activeConfigurations.TryAdd(configuration.ServiceName, new ActiveConfigurationModel()
                {
                    Version = configuration.Version,
                    WorkerPerTimeId = new ConcurrentDictionary<int, ConfigurationsWorker>()
                });
                
                foreach (var runScheme in configuration.GetAllRunSchemes())
                {
                    runScheme.LogSettings ??= configuration.LogSettings ?? LogSettings;
                    
                    if (runScheme.Id == 0)
                    {
                        var existingService = await wiserDashboardService.GetServiceAsync(configuration.ServiceName, runScheme.TimeId);
                        if (existingService == null)
                        {
                            await wiserDashboardService.CreateServiceAsync(configuration.ServiceName, runScheme.TimeId);
                        }
                    }
                    
                    await wiserDashboardService.UpdateServiceAsync(configuration.ServiceName, runScheme.TimeId, runScheme.Action, runScheme.Type.ToString().ToLower(), state: "active", templateId: configuration.TemplateId);

                    var thread = new Thread(() => StartConfigurationAsync(runScheme, configuration));
                    thread.Start();
                }
            }
            
            await StopRemovedConfigurationsAsync(configurations);

            await StartExtraRunsAsync();
        }

        private async Task SetOAuthConfigurationAsync(string oAuthConfiguration)
        {
            var serializer = new XmlSerializer(typeof(OAuthConfigurationModel));
            using var reader = new StringReader(oAuthConfiguration);
            OAuthConfigurationModel configuration = null;

            try
            {
                configuration = (OAuthConfigurationModel)serializer.Deserialize(reader);
            }
            catch (InvalidOperationException e)
            {
                await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Could not parse OAuth configuration due to exception {e}", "OAuth");
                await errorNotificationService.NotifyOfErrorByEmailAsync(wtsSettings.ServiceFailedNotificationEmails, $"OAuth configuration of '{wtsSettings.Name}' could not be parsed.", $"Wiser Task Scheduler '{wtsSettings.Name}' could not parse OAuth configuration. Please check the logs for more details.", LogSettings, LogScopes.StartAndStop, "OAuth");
            }

            if (configuration != null)
            {
                configuration.LogSettings ??= LogSettings;
                await oAuthService.SetConfigurationAsync(configuration);
            }
        }

        private async Task StopRemovedConfigurationsAsync(List<ConfigurationModel> configurations)
        {
            foreach (var activeConfiguration in activeConfigurations)
            {
                if (configurations.Any(configuration => configuration.ServiceName.Equals(activeConfiguration.Key)))
                {
                    continue;
                }

                var configurationStopTasks = await StopConfigurationAsync(activeConfiguration.Key);
                await WaitTillConfigurationsStoppedAsync(configurationStopTasks);
                activeConfigurations.TryRemove(new KeyValuePair<string, ActiveConfigurationModel>(activeConfiguration.Key, activeConfigurations[activeConfiguration.Key]));
            }
        }

        /// <inheritdoc />
        public async Task StopAllConfigurationsAsync()
        {
            var configurationStopTasks = new List<(ConfigurationsWorker worker, Task task)>();

            foreach (var configuration in activeConfigurations)
            {
                configurationStopTasks.AddRange(await StopConfigurationAsync(configuration.Key));
            }

            await WaitTillConfigurationsStoppedAsync(configurationStopTasks);
        }

        /// <summary>
        /// Call the stop method on all workers for the specified configuration.
        /// </summary>
        /// <param name="configurationName">The name of the configuration that needs to be stopped.</param>
        /// <returns>Returns a list with the configurations that are stopping and the tasks to wait for.</returns>
        private async Task<List<(ConfigurationsWorker worker, Task task)>> StopConfigurationAsync(string configurationName)
        {
            var configurationStopTasks = new List<(ConfigurationsWorker worker, Task task)>();
            var cancellationToken = new CancellationToken();

            // If the configuration is already running but on a different version stop the current active one.
            foreach (var worker in activeConfigurations[configurationName].WorkerPerTimeId)
            {
                await logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"Stopping configuration {configurationName} with time ID {worker.Key} (version {activeConfigurations[configurationName].Version}).", configurationName, worker.Key);
                configurationStopTasks.Add((worker.Value, worker.Value.StopAsync(cancellationToken)));
            }

            return configurationStopTasks;
        }

        private async Task WaitTillConfigurationsStoppedAsync(List<(ConfigurationsWorker worker, Task task)> configurationStopTasks)
        {
            for (var i = 0; i < configurationStopTasks.Count; i++)
            {
                await configurationStopTasks[i].task;
                configurationStopTasks[i].worker.Dispose();

                await logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"Stopped configuration {configurationStopTasks[i].worker.Configuration.ServiceName} with time ID {configurationStopTasks[i].worker.RunScheme.TimeId}.", configurationStopTasks[i].worker.Configuration.ServiceName, configurationStopTasks[i].worker.RunScheme.TimeId);
                await logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"Stopped {i + 1}/{configurationStopTasks.Count} configurations workers.", LogName);
            }
        }

        /// <summary>
        /// Retrieve all configurations.
        /// </summary>
        /// <returns>Returns the configurations</returns>
        private async Task<List<ConfigurationModel>> GetConfigurationsAsync()
        {
            var configurations = new List<ConfigurationModel>();
            
            if (String.IsNullOrWhiteSpace(wtsSettings.MainService.LocalConfiguration))
            {
                var wiserConfigurations = await wiserService.RequestConfigurations();

                if (wiserConfigurations == null)
                {
                    return null;
                }

                foreach (var wiserConfiguration in wiserConfigurations)
                {
                    // Decrypt configurations if they have been encrypted.
                    if (!String.IsNullOrWhiteSpace(wiserConfiguration.EditorValue) && !wiserConfiguration.EditorValue.StartsWith("{") && !wiserConfiguration.EditorValue.StartsWith("<"))
                    {
                        wiserConfiguration.EditorValue = wiserConfiguration.EditorValue.DecryptWithAes(gclSettings.DefaultEncryptionKey, useSlowerButMoreSecureMethod: true);
                    }

                    if (String.IsNullOrWhiteSpace(wiserConfiguration.EditorValue)) continue;

                    wiserConfiguration.EditorValue = ReplaceCredentials(wiserConfiguration.EditorValue);

                    if (wiserConfiguration.EditorValue.StartsWith("<OAuthConfiguration>"))
                    {
                        if (String.IsNullOrWhiteSpace(wtsSettings.MainService.LocalOAuthConfiguration))
                        {
                            if (wiserConfiguration.Version != oAuthConfigurationVersion)
                            {
                                await SetOAuthConfigurationAsync(wiserConfiguration.EditorValue);
                                oAuthConfigurationVersion = wiserConfiguration.Version;
                            }
                        }

                        continue;
                    }

                    ConfigurationModel configuration = null;

                    try
                    {
                        configuration = await DeserializeConfigurationAsync(wiserConfiguration.EditorValue, wiserConfiguration.Name);
                    }
                    catch (InvalidOperationException e)
                    {
                        await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Could not parse configuration {wiserConfiguration.Name} due to exception {e}", wiserConfiguration.Name);
                        await errorNotificationService.NotifyOfErrorByEmailAsync(wtsSettings.ServiceFailedNotificationEmails, $"Configuration '{wiserConfiguration.Name}' of '{wtsSettings.Name}' could not be parsed.", $"Wiser Task Scheduler '{wtsSettings.Name}' could not parse configuration '{wiserConfiguration.Name}'. Please check the logs for more details.", LogSettings, LogScopes.StartAndStop, wiserConfiguration.Name);
                    }

                    if (configuration != null)
                    {
                        configuration.TemplateId = wiserConfiguration.TemplateId;
                        configuration.Version = wiserConfiguration.Version;
                        configurations.Add(configuration);
                    }
                }
            }
            else
            {
                var fileContent = await File.ReadAllTextAsync(wtsSettings.MainService.LocalConfiguration);
                fileContent = ReplaceCredentials(fileContent);
                ConfigurationModel configuration = null;

                try
                {
                    configuration = await DeserializeConfigurationAsync(fileContent, $"Local file {wtsSettings.MainService.LocalConfiguration}");
                }
                catch (InvalidOperationException e)
                {
                    await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Could not parse configuration {wtsSettings.MainService.LocalConfiguration} due to exception {e}", wtsSettings.MainService.LocalConfiguration);
                    await errorNotificationService.NotifyOfErrorByEmailAsync(wtsSettings.ServiceFailedNotificationEmails, $"Configuration '{wtsSettings.MainService.LocalConfiguration}' of '{wtsSettings.Name}' could not be parsed.", $"Wiser Task Scheduler '{wtsSettings.Name}' could not parse configuration '{wtsSettings.MainService.LocalConfiguration}'. Please check the logs for more details.", LogSettings, LogScopes.StartAndStop, wtsSettings.MainService.LocalConfiguration);
                }

                if (configuration != null)
                {
                    configuration.TemplateId = 0;
                    configuration.Version = File.GetLastWriteTimeUtc(wtsSettings.MainService.LocalConfiguration).Ticks;
                    configurations.Add(configuration);
                }
            }

            if (!String.IsNullOrWhiteSpace(wtsSettings.MainService.LocalOAuthConfiguration))
            {
                var fileVersion = File.GetLastWriteTimeUtc(wtsSettings.MainService.LocalOAuthConfiguration).Ticks;
                if (fileVersion != oAuthConfigurationVersion)
                {
                    await SetOAuthConfigurationAsync(await File.ReadAllTextAsync(wtsSettings.MainService.LocalOAuthConfiguration));
                    oAuthConfigurationVersion = fileVersion;
                }
            }

            return configurations;
        }

        /// <summary>
        /// Replaces credentials in a configuration if they are present.
        /// </summary>
        /// <param name="configuration">The configuration to perform the replacements on.</param>
        /// <returns>The configuration with the credentials replaced or the original if no credentials were present.</returns>
        private string ReplaceCredentials(string configuration)
        {
            // Replace credentials in configuration.
            if (wtsSettings.Credentials != null && configuration.Contains("[{Credential:"))
            {
                foreach (var credential in wtsSettings.Credentials)
                {
                    configuration = configuration.Replace($"[{{Credential:{credential.Key}}}]", credential.Value);
                }
            }
            
            return configuration;
        }

        /// <summary>
        /// Deserialize a configuration.
        /// </summary>
        /// <param name="serializedConfiguration">The serialized configuration.</param>
        /// <param name="configurationFileName">The name of the configuration, either the template name of the file path.</param>
        /// <returns></returns>
        private async Task<ConfigurationModel> DeserializeConfigurationAsync(string serializedConfiguration, string configurationFileName)
        {
            ConfigurationModel configuration;

            if (serializedConfiguration.StartsWith("{")) // Json configuration.
            {
                configuration = JsonConvert.DeserializeObject<ConfigurationModel>(serializedConfiguration);
            }
            else if (serializedConfiguration.StartsWith("<Configuration>")) // XML configuration.
            {
                var serializer = new XmlSerializer(typeof(ConfigurationModel));
                using var reader = new StringReader(serializedConfiguration);
                configuration = (ConfigurationModel)serializer.Deserialize(reader);
            }
            else
            {
                await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Configuration '{configurationFileName}' is not in supported format.", configurationFileName);
                return null;
            }

            using var scope = serviceProvider.CreateScope();
            var configurationsService = scope.ServiceProvider.GetRequiredService<IConfigurationsService>();
            configurationsService.LogSettings = LogSettings;

            // Only add configurations to run when they are valid.
            if (await configurationsService.IsValidConfigurationAsync(configuration))
            {
                return configuration;
            }

            await logService.LogError(logger, LogScopes.StartAndStop, LogSettings, $"Did not start configuration {configuration.ServiceName} due to conflicts.", configurationFileName);
            return null;
        }

        /// <summary>
        /// Starts a new <see cref="ConfigurationsWorker"/> for the specified configuration and run scheme.
        /// </summary>
        /// <param name="runScheme">The run scheme of the worker.</param>
        /// <param name="configuration">The configuration the run scheme is within.</param>
        /// <param name="singleRun">Optional: If the configuration only needs to be ran once. Will ignore paused state and run time.</param>
        private async void StartConfigurationAsync(RunSchemeModel runScheme, ConfigurationModel configuration, bool singleRun = false)
        {
            using var scope = serviceProvider.CreateScope();
            var worker = scope.ServiceProvider.GetRequiredService<ConfigurationsWorker>();

            try
            {
                await worker.InitializeAsync(configuration, $"{configuration.ServiceName}", runScheme, singleRun);
                
                // If there is no action to be performed the thread can be closed.
                if (!worker.HasAction)
                {
                    await wiserDashboardService.UpdateServiceAsync(configuration.ServiceName, runScheme.TimeId, state: "stopped");
                    return;
                }
            }
            catch (Exception e)
            {
                await logService.LogCritical(logger, LogScopes.StartAndStop, configuration.LogSettings, $"{configuration.ServiceName} with time ID '{runScheme.TimeId}' could not be started due to exception {e}", configuration.ServiceName, runScheme.TimeId);
                await wiserDashboardService.UpdateServiceAsync(configuration.ServiceName, runScheme.TimeId, state: "crashed");

                await errorNotificationService.NotifyOfErrorByEmailAsync(String.IsNullOrWhiteSpace(configuration.ServiceFailedNotificationEmails) ? configuration.ServiceFailedNotificationEmails : wtsSettings.ServiceFailedNotificationEmails, $"Service '{configuration.ServiceName}' with time ID '{runScheme.TimeId}' of '{wtsSettings.Name}' could not be started.", $"Wiser Task Scheduler '{wtsSettings.Name}' could not start service '{configuration.ServiceName}' with time ID '{runScheme.TimeId}'. Please check the logs for more details.", runScheme.LogSettings, LogScopes.StartAndStop, configuration.ServiceName);

                return;
            }

            if (!singleRun)
            {
                activeConfigurations[configuration.ServiceName].WorkerPerTimeId.TryAdd(runScheme.TimeId, worker);
            }
            await worker.StartAsync(new CancellationToken());
            await worker.ExecuteTask; // Keep scope alive until worker stops.
        }

        private async Task StartExtraRunsAsync()
        {
            var services = await wiserDashboardService.GetServices(true);

            foreach (var service in services)
            {
                // If the service is currently running no extra run will be started.
                if (service.State.Equals("running", StringComparison.CurrentCultureIgnoreCase))
                {
                    await wiserDashboardService.UpdateServiceAsync(service.Configuration, service.TimeId, extraRun: false);
                    continue;
                }

                // If the service is not normally running by this WTS we don't have the configuration for a single run so we skip it.
                if (!activeConfigurations.ContainsKey(service.Configuration) || !activeConfigurations[service.Configuration].WorkerPerTimeId.ContainsKey(service.TimeId))
                {
                    continue;
                }
                
                var configuration = activeConfigurations[service.Configuration].WorkerPerTimeId[service.TimeId].Configuration;
                var runScheme = activeConfigurations[service.Configuration].WorkerPerTimeId[service.TimeId].RunScheme;
                
                var thread = new Thread(() => StartConfigurationAsync(runScheme, configuration, true));
                thread.Start();
            }
        }
    }
}
