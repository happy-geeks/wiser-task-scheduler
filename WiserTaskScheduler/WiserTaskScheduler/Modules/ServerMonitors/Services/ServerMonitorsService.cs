﻿using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
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

namespace WiserTaskScheduler.Modules.ServerMonitors.Services
{
    internal class ServerMonitorsService : IServerMonitorsService, IActionsService, IScopedService
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ILogService logService;
        private readonly ILogger<ServerMonitorsService> logger;

        private string connectionString;

        private Dictionary<string, bool> emailDrivesSent = new Dictionary<string, bool>();
        private bool emailRAMSent;
        private bool emailCPUSent;
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

        public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
        {
            connectionString = configuration.ConnectionString;
            return Task.CompletedTask;
        }

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

            int threshold = monitorItem.Threshold;

            //Check which type of server monitor is used.
            //Cpu is default Value.
            switch (monitorItem.ServerMonitorType)
            {
                case ServerMonitorTypes.Drive:
                    if (monitorItem.DriveName == "All")
                    {
                        await GetAllHardDrivesSpaceAsync(monitorItem, threshold, gclCommunicationsService, configurationServiceName);
                    }
                    else
                    {
                        await GetHardDriveSpaceAsync(monitorItem, threshold, gclCommunicationsService, configurationServiceName, monitorItem.DriveName);
                    }
                    break;
                case ServerMonitorTypes.Ram:
                    await GetRAMSpaceAsync(monitorItem, threshold, gclCommunicationsService, configurationServiceName);
                    break;
                case ServerMonitorTypes.Cpu:
                    await GetCpuUsageAsync(monitorItem, threshold, gclCommunicationsService, configurationServiceName);
                    break;
                case ServerMonitorTypes.Network:
                    await GetNetworkUtilization(monitorItem, threshold, gclCommunicationsService, configurationServiceName);
                    break;
                default:
                    break;
            }

            return new JObject
            {
                {"Results", 0}
            };

        }

        //Gets info over all existing hard drives.
        public async Task GetAllHardDrivesSpaceAsync(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName)
        {
            foreach (var drive in allDrives)
            {
                if (!emailDrivesSent.ContainsKey(drive.Name))
                {
                    emailDrivesSent[drive.Name] = false;
                }
            }

            foreach (var drive in allDrives)
            {
                //Calculate the percentage of free space availible and see if it matches with the given threshold.
                double freeSpace = drive.TotalFreeSpace;
                double fullSpace = drive.TotalSize;
                double percentage = freeSpace / fullSpace * 100;
                //Set the right values for the email.

                //Set the email settings correctly
                receiver = monitorItem.EmailAddressForWarning;
                subject = $"Low space on Drive {drive.Name}";
                body = $"Drive {drive.Name} only has {percentage}% space left, this is below the threshold of {threshold}";

                await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Drive {drive} has {drive.TotalFreeSpace}Bytes of free space", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

                //Check if the threshold is higher then the free space available.
                if (percentage < threshold)
                {
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Drive {drive.Name} only has {percentage}% space left, this is below the threshold of {threshold}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

                    //Only send an email if the Drive threshold hasn't already been reached.
                    if (!emailDrivesSent[drive.Name])
                    {
                        emailDrivesSent[drive.Name] = true;
                        await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
                    }
                }
                else
                {
                    emailDrivesSent[drive.Name] = false;
                }
            }
        }


        //Gets only the data from 1 Specified Hard Drive.
        public async Task GetHardDriveSpaceAsync(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName, string driveName)
        {
            DriveInfo drive = new DriveInfo(driveName);

            if (!emailDrivesSent.ContainsKey(drive.Name))
            {
                emailDrivesSent[drive.Name] = false;
            }

            //Calculate the percentage of free space availible and see if it matches with the given threshold.
            double freeSpace = drive.TotalFreeSpace;
            double fullSpace = drive.TotalSize;
            double percentage = freeSpace / fullSpace * 100;

            //Set the email settings correctly
            receiver = monitorItem.EmailAddressForWarning;
            subject = $"Low space on Drive {drive.Name}";
            body = $"Drive {drive.Name} only has {percentage}% space left, this is below the threshold of {threshold}";

            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Drive {drive} has {drive.TotalFreeSpace}Bytes of free space", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Check if the threshold is higher then the free space available.
            if (percentage < threshold)
            {
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Drive {drive.Name} only has {percentage}% space left, this is below the threshold of {threshold}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

                //Only send an email if the Drive threshold hasn't already been reached.
                if (!emailDrivesSent[drive.Name])
                {
                    emailDrivesSent[drive.Name] = true;
                    await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
                }
            }
            else
            {
                emailDrivesSent[drive.Name] = false;
            }
        }

        public async Task GetRAMSpaceAsync(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName)
        {
            double ramValue = ramCounter.NextValue();
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"RAM is: {ramValue}MB available", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "Low on RAM space.";
            body = $"Your ram has {ramValue}MB available which is below your set threshold of {threshold}MB";

            //Check if the ram is above the threshold.
            if (ramValue < threshold)
            {
                if (!emailRAMSent)
                {
                    emailRAMSent = true;
                    await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
                }
            }
            else
            {
                emailRAMSent = false;
            }
        }

        public async Task GetCpuUsageAsync(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName)
        {
            //gets the detection type of cpu usage to use.
            switch(monitorItem.CpuUsageDetectionType)
            {
                case CpuUsageDetectionTypes.ArrayCount:
                    await GetCpuUsageArrayCountAsync(monitorItem, threshold, gclCommunicationsService, configurationServiceName);
                    break;
                case CpuUsageDetectionTypes.Counter:
                    await GetCpuUsageCounterAsync(monitorItem, threshold, gclCommunicationsService, configurationServiceName);
                    break;
            }
        }

        public async Task GetCpuUsageArrayCountAsync(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName)
        {
            //the first value of performance counter will always be 0.
            if (!firstValueUsed)
            {
                firstValueUsed = true;
                float firstValue = cpuCounter.NextValue();
            }
            float count = 0;
            float realvalue = cpuCounter.NextValue();
            //gets 60 percent of the size of the array
            int arrayCountThreshold = (int)(10 * 0.6);
            //Puts the value into the array.
            cpuValues[cpuIndex] = realvalue;
            //if the index for the array is at the end make it start at the beginning again.
            //so then it loops through the array the whole time.
            cpuIndex = (cpuIndex + 1) % 10;

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "CPU usage too high.";
            body = $"The CPU usage is above {threshold}.";

            //Counts how many values inside the array are above the threshold and adds them to the count
            count = cpuValues.Count(val => val > threshold);
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Array count is: {count}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            
            if (count >= arrayCountThreshold)
            {
                if (!emailCPUSent)
                {
                    await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
                    emailCPUSent = true;
                }
            }
            else
            {
                emailCPUSent = false;
            }
        }
        public async Task GetCpuUsageCounterAsync(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName)
        {

            //the first value of performance counter will always be 0.
            if (!firstValueUsed)
            {
                firstValueUsed = true;
                float firstValue = cpuCounter.NextValue();
            }
            float realvalue = cpuCounter.NextValue();

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "CPU usage too high.";
            body = $"The CPU usage has been above the threshold for {aboveThresholdTimer} runs.";
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"CPU is: {realvalue}%", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Checks if the value is above the threshold and if so then adds 1 to the counter.
            //If the value is below the threshold then reset the counter to 0.
            if (realvalue > threshold)
            {
                aboveThresholdTimer++;
                if (aboveThresholdTimer >= 6)
                {
                    if (!emailCPUSent)
                    {
                        await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
                        emailCPUSent = true;
                    }
                }
                else
                {
                    emailCPUSent = false;
                }
            }
            else
            {
                aboveThresholdTimer = 0;
            }
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"CPU timer count  is: {aboveThresholdTimer}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);
        }

        public async Task GetNetworkUtilization(ServerMonitorModel monitorItem, int threshold, CommunicationsService gclCommunicationsService, string configurationServiceName)
        {
            string networkInterfaceName = monitorItem.NetworkInterfaceName;

            const int numberOfIterations = 10;

            //get the correct types of the performancecounter class
            PerformanceCounter bandwidthCounter = new PerformanceCounter("Network Interface", "Current Bandwidth", networkInterfaceName);
            float bandwidth = bandwidthCounter.NextValue();
            PerformanceCounter dataSentCounter = new PerformanceCounter("Network Interface", "Bytes Sent/sec", networkInterfaceName);
            PerformanceCounter dataReceivedCounter = new PerformanceCounter("Network Interface", "Bytes Received/sec", networkInterfaceName);

            float sendSum = 0;
            float receiveSum = 0;

            for (int index = 0; index < numberOfIterations; index++)
            {
                sendSum += dataSentCounter.NextValue();
                receiveSum += dataReceivedCounter.NextValue();
            }
            float dataSent = sendSum;
            float dataReceived = receiveSum;

            double utilization = (8 * (dataSent + dataReceived)) / (bandwidth * numberOfIterations) * 100;
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, monitorItem.LogSettings, $"Network utilization: {utilization}", configurationServiceName, monitorItem.TimeId, monitorItem.Order);

            //Set the email settings correctly.
            receiver = monitorItem.EmailAddressForWarning;
            subject = "High network utilization.";
            body = $"Your network utilization is {utilization} which is above the threshold of {threshold}.";

            if (utilization > threshold)
            {
                if(!emailNetworkSent)
                {
                    await gclCommunicationsService.SendEmailAsync(receiver, subject, body);
                }
            }
            else
            {
                emailNetworkSent = true;
            }

        }
    }
}