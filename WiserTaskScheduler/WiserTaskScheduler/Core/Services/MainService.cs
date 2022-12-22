using System;
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

        private readonly string localConfiguration;
        private readonly string localOAuthConfiguration;
        private readonly GclSettings gclSettings;
        private readonly IServiceProvider serviceProvider;
        private readonly IWiserService wiserService;
        private readonly IOAuthService oAuthService;
        private readonly ILogService logService;
        private readonly ILogger<MainService> logger;

        private readonly ConcurrentDictionary<string, ActiveConfigurationModel> activeConfigurations;

        private long oAuthConfigurationVersion;

        /// <inheritdoc />
        public LogSettings LogSettings { get; set; }

        /// <summary>
        /// Creates a new instance of <see cref="MainService"/>.
        /// </summary>
        public MainService(IOptions<WtsSettings> wtsSettings, IOptions<GclSettings> gclSettings, IServiceProvider serviceProvider, IWiserService wiserService, IOAuthService oAuthService, ILogService logService, ILogger<MainService> logger)
        {
            localConfiguration = wtsSettings.Value.MainService.LocalConfiguration;
            localOAuthConfiguration = wtsSettings.Value.MainService.LocalOAuthConfiguration;
            this.gclSettings = gclSettings.Value;
            this.serviceProvider = serviceProvider;
            this.wiserService = wiserService;
            this.oAuthService = oAuthService;
            this.logService = logService;
            this.logger = logger;

            activeConfigurations = new ConcurrentDictionary<string, ActiveConfigurationModel>();
        }

        /// <inheritdoc />
        public async Task ManageConfigurations()
        {
            var configurations = await GetConfigurations();

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
                    var configurationStopTasks = StopConfiguration(configuration.ServiceName);
                    await WaitTillConfigurationsStopped(configurationStopTasks);
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

                    var thread = new Thread(() => StartConfiguration(runScheme, configuration));
                    thread.Start();
                }
            }
            
            await StopRemovedConfigurations(configurations);
        }

        private async Task SetOAuthConfiguration(string oAuthConfiguration)
        {
            var serializer = new XmlSerializer(typeof(OAuthConfigurationModel));
            using var reader = new StringReader(oAuthConfiguration);
            var configuration = (OAuthConfigurationModel)serializer.Deserialize(reader);

            configuration.LogSettings ??= LogSettings;

            await oAuthService.SetConfigurationAsync(configuration);
        }

        private async Task StopRemovedConfigurations(List<ConfigurationModel> configurations)
        {
            foreach (var activeConfiguration in activeConfigurations)
            {
                if (configurations.Any(configuration => configuration.ServiceName.Equals(activeConfiguration.Key)))
                {
                    continue;
                }

                var configurationStopTasks = StopConfiguration(activeConfiguration.Key);
                await WaitTillConfigurationsStopped(configurationStopTasks);
                activeConfigurations.TryRemove(new KeyValuePair<string, ActiveConfigurationModel>(activeConfiguration.Key, activeConfigurations[activeConfiguration.Key]));
            }
        }

        /// <inheritdoc />
        public async Task StopAllConfigurations()
        {
            var configurationStopTasks = new List<Task>();

            foreach (var configuration in activeConfigurations)
            {
                configurationStopTasks.AddRange(StopConfiguration(configuration.Key));
            }

            await WaitTillConfigurationsStopped(configurationStopTasks);
        }

        private List<Task> StopConfiguration(string configurationName)
        {
            var configurationStopTasks = new List<Task>();
            var cancellationToken = new CancellationToken();

            // If the configuration is already running but on a different version stop the current active one.
            foreach (var worker in activeConfigurations[configurationName].WorkerPerTimeId)
            {
                configurationStopTasks.Add(worker.Value.StopAsync(cancellationToken));
            }

            return configurationStopTasks;
        }

        private async Task WaitTillConfigurationsStopped(List<Task> configurationStopTasks)
        {
            for (var i = 0; i < configurationStopTasks.Count; i++)
            {
                await configurationStopTasks[i];

                await logService.LogInformation(logger, LogScopes.StartAndStop, LogSettings, $"Stopped {i + 1}/{configurationStopTasks.Count} configurations workers.", LogName);
            }
        }

        /// <summary>
        /// Retrieve all configurations.
        /// </summary>
        /// <returns>Returns the configurations</returns>
        private async Task<List<ConfigurationModel>> GetConfigurations()
        {
            var configurations = new List<ConfigurationModel>();
            
            if (String.IsNullOrWhiteSpace(localConfiguration))
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

                    if (wiserConfiguration.EditorValue.StartsWith("<OAuthConfiguration>"))
                    {
                        if (String.IsNullOrWhiteSpace(localOAuthConfiguration))
                        {
                            if (wiserConfiguration.Version != oAuthConfigurationVersion)
                            {
                                await SetOAuthConfiguration(wiserConfiguration.EditorValue);
                                oAuthConfigurationVersion = wiserConfiguration.Version;
                            }
                        }

                        continue;
                    }

                    var configuration = await DeserializeConfigurationAsync(wiserConfiguration.EditorValue, wiserConfiguration.Name);

                    if (configuration != null)
                    {
                        configuration.Version = wiserConfiguration.Version;
                        configurations.Add(configuration);
                    }
                }
            }
            else
            {
                var configuration = await DeserializeConfigurationAsync(await File.ReadAllTextAsync(localConfiguration), $"Local file {localConfiguration}");

                if (configuration != null)
                {
                    configuration.Version = File.GetLastWriteTimeUtc(localConfiguration).Ticks;
                    configurations.Add(configuration);
                }
            }

            if (!String.IsNullOrWhiteSpace(localOAuthConfiguration))
            {
                var fileVersion = File.GetLastWriteTimeUtc(localOAuthConfiguration).Ticks;
                if (fileVersion != oAuthConfigurationVersion)
                {
                    await SetOAuthConfiguration(await File.ReadAllTextAsync(localOAuthConfiguration));
                    oAuthConfigurationVersion = fileVersion;
                }
            }

            return configurations;
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
        private async void StartConfiguration(RunSchemeModel runScheme, ConfigurationModel configuration)
        {
            using var scope = serviceProvider.CreateScope();
            var worker = scope.ServiceProvider.GetRequiredService<ConfigurationsWorker>();
            await worker.InitializeAsync(configuration, $"{configuration.ServiceName} (Time id: {runScheme.TimeId})", runScheme);
            activeConfigurations[configuration.ServiceName].WorkerPerTimeId.TryAdd(runScheme.TimeId, worker);
            await worker.StartAsync(new CancellationToken());
            await worker.ExecuteTask; // Keep scope alive until worker stops.
        }
    }
}
