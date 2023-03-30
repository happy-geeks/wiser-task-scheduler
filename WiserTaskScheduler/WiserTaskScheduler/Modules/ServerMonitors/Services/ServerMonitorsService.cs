using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Modules.ServerMonitors.Interfaces;
using WiserTaskScheduler.Modules.ServerMonitors.Models;
using WiserTaskScheduler.Modules.ServerMonitors.Enums;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Logging;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using Microsoft.Extensions.Options;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Communication.Services;
using GeeksCoreLibrary.Modules.DataSelector.Services;
using GeeksCoreLibrary.Modules.Templates.Services;
using GeeksCoreLibrary.Modules.Communication.Interfaces;

namespace WiserTaskScheduler.Modules.ServerMonitors.Services
{
    internal class ServerMonitorsService : IServerMonitorsService, IActionsService, IScopedService
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ILogService logService;
        private readonly ILogger<ServerMonitorsService> logger;

        private string connectionString;

        private Dictionary<string, bool> emailDrivesSent = new Dictionary<string, bool>();
        private bool emailRamSent;
        private bool emailCpuSent;
        private bool emailNetworkSent;
        
        private string receiver;
        private string subject;
        private string body;

        //create the right performanceCounter variable types.
        private PerformanceCounter cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
        private PerformanceCounter ramCounter = new PerformanceCounter("Memory", "Available MBytes");
        private DriveInfo[] allDrives = DriveInfo.GetDrives();

        private bool firstValueUsed;
        private int cpuIndex;
        private int aboveThresholdTimer;
        float[] cpuValues = new float[10];

        public ServerMonitorsService(IServiceProvider serviceProvider, ILogService logService, ILogger<ServerMonitorsService> logger)
        {
            this.serviceProvider = serviceProvider;
            this.logService = logService;
            this.logger = logger;
        }

        /// <inheritdoc />
        public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
        {
            connectionString = configuration.ConnectionString;
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
        {
            var monitorItem = (ServerMonitorModel)action;
            using var scope = serviceProvider.CreateScope();
            using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

            await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);

            // Wiser Items Service requires dependency injection that results in the need of MVC services that are unavailable.
            // Get all other services and create the Wiser Items Service with one of the services missing.
            var objectService = scope.ServiceProvider.GetRequiredService<IObjectsService>();
            var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();
            var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
            var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
            var wiserItemsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<WiserItemsService>>();
            var gclCommunicationsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<CommunicationsService>>();
            var dataSelectorsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<DataSelectorsService>>();

            var templatesService = new TemplatesService(null, gclSettings, databaseConnection, stringReplacementsService, null, null, null, null, null, null, null, null, null, databaseHelpersService);
            var dataSelectorsService = new DataSelectorsService(gclSettings, databaseConnection, stringReplacementsService, templatesService, null, null, dataSelectorsServiceLogger, null);
            var wiserItemsService = new WiserItemsService(databaseConnection, objectService, stringReplacementsService, dataSelectorsService, databaseHelpersService, gclSettings, wiserItemsServiceLogger);
            var gclCommunicationsService = new CommunicationsService(gclSettings, gclCommunicationsServiceLogger, wiserItemsService, databaseConnection, databaseHelpersService);

            var threshold = monitorItem.Threshold;

            //Check which type of server monitor is used.
            //Cpu is default Value.
            switch (monitorItem.ServerMonitorType)
            {
                case ServerMonitorTypes.Drive:
                    if (monitorItem.DriveName == "All")
                    {
                        await GetAllHardDrivesSpaceAsync(monitorItem, threshold, configurationServiceName, gclCommunicationsService);
                    }
                    else
                    {
                        await GetHardDriveSpaceAsync(monitorItem, threshold, configurationServiceName, gclCommunicationsService, monitorItem.DriveName);
                    }
                    break;
                case ServerMonitorTypes.Ram:
                    await GetRamSpaceAsync(monitorItem, threshold, configurationServiceName, gclCommunicationsService);
                    break;
                case ServerMonitorTypes.Cpu:
                    await GetCpuUsageAsync(monitorItem, threshold, configurationServiceName, gclCommunicationsService);
                    break;
                case ServerMonitorTypes.Network:
                    await GetNetworkUtilization(monitorItem, threshold, configurationServiceName, gclCommunicationsService);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(monitorItem.ServerMonitorType));
            }

            return new JObject
            {
                {"Results", 0}
            };
        }

        /// <summary>
        /// Gets all the available hard drives and checks the free space available for all of them 
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action. </param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <returns></returns>
        private async Task GetAllHardDrivesSpaceAsync(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService)
        {
            foreach (var drive in allDrives)
            {
                //Checks for each drive if it already exists in the dictonary.
                if (!emailDrivesSent.ContainsKey(drive.Name))
                {
                    emailDrivesSent[drive.Name] = false;
                }

                //Calculate the percentage of free space availible and see if it matches with the given threshold.
                decimal freeSpace = drive.TotalFreeSpace;
                decimal fullSpace = drive.TotalSize;
                var percentage = freeSpace / fullSpace * 100;

                //Set the email settings correctly
                receiver = monitorItem.EmailAddressForWarning;
                subject = $"Low space on Drive {drive.Name}";
                body = $"Drive {drive.Name} only has {Decimal.Round(percentage)}% space left, this is below the threshold of {threshold}%.";

                await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Drive {drive} has {drive.TotalFreeSpace}Bytes of free space", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

                //Check if the threshold is higher then the free space available.
                if (percentage > threshold)
                {
                    emailDrivesSent[drive.Name] = false;
                    continue;
                }
                
                //Check if an email already has been sent
                if (emailDrivesSent[drive.Name])
                {
                    continue;
                }

                emailDrivesSent[drive.Name] = true;
                await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
            }
        }

        /// <summary>
        /// This gets the hard drive free space available for one specific drive.
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action.</param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <param name="driveName">The name of the drive that is used by the monitor.</param>
        /// <returns></returns>
        private async Task GetHardDriveSpaceAsync(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService, string driveName)
        {
            var drive = new DriveInfo(driveName);

            //Check to see if the drive is already within the dictonary.
            if (!emailDrivesSent.ContainsKey(drive.Name))
            {
                emailDrivesSent[drive.Name] = false;
            }

            //Calculate the percentage of free space availible and see if it matches with the given threshold.
            decimal freeSpace = drive.TotalFreeSpace;
            decimal fullSpace = drive.TotalSize;
            var percentage = Decimal.Round(freeSpace) / Decimal.Round(fullSpace) * 100;

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = $"Low space on Drive {drive.Name}";
            body = $"Drive {drive.Name} only has {Decimal.Round(percentage)}% space left, this is below the threshold of {threshold}%.";

            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Drive {drive} has {drive.TotalFreeSpace}Bytes of free space", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Check if the threshold is higher then the free space available.
            if (percentage > threshold)
            {
                emailDrivesSent[drive.Name] = false;
                return;
            }

            //Check if an email already has been sent.
            if (emailDrivesSent[drive.Name])
            {
                return;
            }

            emailDrivesSent[drive.Name] = true;
            await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
        }

        /// <summary>
        /// Gets the available RAM space and caclulates if it needs to send an email.
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action.</param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <returns></returns>
        private async Task GetRamSpaceAsync(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService)
        {
            var ramValue = ramCounter.NextValue();
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"RAM is: {ramValue}MB available", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "Low on RAM space.";
            body = $"Your ram has {ramValue}MB available which is below your set threshold of {threshold}MB.";

            //Check if the ram is above the threshold.
            if (ramValue > threshold)
            {
                emailRamSent = false;
                return;
            }

            //Check if an email already has been sent.
            if (emailRamSent)
            {
                return;
            }

            emailRamSent = true;
            await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
        }

        /// <summary>
        /// Checks the Cpu usage detection type to use and calls the right function.
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action.</param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <returns></returns>
        private async Task GetCpuUsageAsync(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService)
        {
            //gets the detection type of cpu usage to use.
            switch(monitorItem.CpuUsageDetectionType)
            {
                case CpuUsageDetectionTypes.ArrayCount:
                    await GetCpuUsageArrayCountAsync(monitorItem, threshold, configurationServiceName, gclCommunicationsService);
                    break;
                case CpuUsageDetectionTypes.Counter:
                    await GetCpuUsageCounterAsync(monitorItem, threshold, configurationServiceName, gclCommunicationsService);
                    break;
            }
        }

        /// <summary>
        /// Gets the CPU usage with the use of a counter which checks if for a certain amount of times the value above the threshold is.
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action.</param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <returns></returns>
        private async Task GetCpuUsageArrayCountAsync(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService)
        {
            //the first value of performance counter will always be 0.
            if (!firstValueUsed)
            {
                firstValueUsed = true;
                var firstValue = cpuCounter.NextValue();
            }
            var count = 0;
            var realvalue = cpuCounter.NextValue();
            //gets 60 percent of the size of the array.
            var arrayCountThreshold = (int)(10 * 0.6);
            //Puts the value into the array.
            cpuValues[cpuIndex] = realvalue;
            //if the index for the array is at the end make it start at the beginning again.
            //so then it loops through the array the whole time.
            cpuIndex = (cpuIndex + 1) % 10;

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "CPU usage too high.";
            body = $"The CPU usage has exceeded the threshold of {threshold}% for 6 or more consecutive times";

            //Counts how many values inside the array are above the threshold and adds them to the count.
            count = cpuValues.Count(val => val > threshold);
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Array count is: {count}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Checks if the count is above the threshold.
            if (count <= arrayCountThreshold)
            {
                emailCpuSent = false;
                return;
            }

            //Check if an email already has been sent
            if (emailCpuSent)
            {
                return;
            }

            emailCpuSent = true;
            await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
        }

        /// <summary>
        /// Gets the CPU usage with the use of a counter which checks if for a certain amount of times the value above the threshold is.
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action.</param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <returns></returns>
        private async Task GetCpuUsageCounterAsync(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService)
        {
            //the first value of performance counter will always be 0.
            if (!firstValueUsed)
            {
                firstValueUsed = true;
                var firstValue = cpuCounter.NextValue();
            }
            var realvalue = cpuCounter.NextValue();

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "CPU usage too high.";
            body = $"The CPU usage has been above the threshold of {threshold}% for {aboveThresholdTimer} runs.";
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"CPU is: {realvalue}%", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Checks if the value is above the threshold and if so then adds 1 to the counter.
            //If the value is below the threshold then reset the counter to 0.
            if(realvalue < threshold)
            {
                emailCpuSent = false;
                aboveThresholdTimer = 0;
                return;
            }

            //Check if an email already has been sent
            if (emailCpuSent)
            {
                return;
            }

            aboveThresholdTimer++;
            if (aboveThresholdTimer >= 6)
            {
                emailCpuSent = true;
                await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
            }

            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"CPU timer count  is: {aboveThresholdTimer}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);
        }

        /// <summary>
        /// Gets the network utilization of the specified network card and checks if an e-mail needs to be send.
        /// </summary>
        /// <param name="monitorItem">The information for the monitor action.</param>
        /// <param name="threshold">The threshold to check if an email needs to be send.</param>
        /// <param name="gclCommunicationsService">The name of the service in the configuration, used for logging.</param>
        /// <param name="configurationServiceName">The communications service from the GCL to actually send out the email.</param>
        /// <returns></returns>
        private async Task GetNetworkUtilization(ServerMonitorModel monitorItem, int threshold, string configurationServiceName, CommunicationsService gclCommunicationsService)
        {
            var networkInterfaceName = monitorItem.NetworkInterfaceName;

            const int numberOfIterations = 10;

            //get the correct types of the performancecounter class
            var bandwidthCounter = new PerformanceCounter("Network Interface", "Current Bandwidth", networkInterfaceName);
            var bandwidth = bandwidthCounter.NextValue();
            var dataSentCounter = new PerformanceCounter("Network Interface", "Bytes Sent/sec", networkInterfaceName);
            var dataReceivedCounter = new PerformanceCounter("Network Interface", "Bytes Received/sec", networkInterfaceName);

            var sendSum = 0f;
            var receiveSum = 0f;

            for (var index = 0; index < numberOfIterations; index++)
            {
                sendSum += dataSentCounter.NextValue();
                receiveSum += dataReceivedCounter.NextValue();
            }
            var dataSent = sendSum;
            var dataReceived = receiveSum;

            var utilization = (8 * (dataSent + dataReceived)) / (bandwidth * numberOfIterations) * 100;
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Network utilization: {utilization}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "High network utilization.";
            body = $"Your network utilization is {utilization} which is above the threshold of {threshold}.";

            //Checks if the value is above the threshold.
            if (utilization < threshold)
            {
                emailNetworkSent = false;
                return;
            }

            //Check if an email already has been sent
            if (emailNetworkSent)
            {
                return;
            }

            emailNetworkSent = true;
            await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
        }
    }
}