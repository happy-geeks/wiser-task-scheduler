using System.Diagnostics;
using System.IO.Compression;
using System.Net.Http.Json;
using System.ServiceProcess;
using AutoUpdater.Enums;
using AutoUpdater.Interfaces;
using AutoUpdater.Models;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Communication.Interfaces;
using GeeksCoreLibrary.Modules.Communication.Models;
using Microsoft.Extensions.Options;

namespace AutoUpdater.Services;

public class UpdateService : IUpdateService
{
    private const string WtsTempPath = "C:/temp/wts";
    private const string WtsExeFile = "WiserTaskScheduler.exe";

    private const int UpdateDelayAfterServiceShutdown = 60000;
    private const int DeleteFileAttempts = 5;
    private const int DeleteFileDelay = 1000;

    private readonly UpdateSettings updateSettings;
    private readonly ILogger<UpdateService> logger;
    private readonly IServiceProvider serviceProvider;

    private Version lastDownloadedVersion;
    
    public UpdateService(IOptions<UpdateSettings> updateSettings, ILogger<UpdateService> logger, IServiceProvider serviceProvider)
    {
        this.updateSettings = updateSettings.Value;
        this.logger = logger;
        this.serviceProvider = serviceProvider;
        
        Directory.CreateDirectory(Path.Combine(WtsTempPath, "update"));
        Directory.CreateDirectory(Path.Combine(WtsTempPath, "backups"));
    }

    /// <inheritdoc />
    public async Task UpdateServicesAsync()
    {
        logger.LogInformation("Starting with updating the WTS services.");

        try
        {
            var versionList = await GetVersionList();
            if (lastDownloadedVersion == null || lastDownloadedVersion != versionList[0].Version)
            {
                await DownloadUpdate(versionList[0].Version);
            }

            foreach (var wts in updateSettings.WtsInstancesToUpdate)
            {
                new Thread(() => UpdateWts(wts, versionList)).Start();
            }
        }
        catch (Exception e)
        {
            logger.LogError($"Failed to update the WTS services due to exception:{Environment.NewLine}{Environment.NewLine}{e}");
        }
    }

    /// <summary>
    /// Get the list of all the versions of the WTS.
    /// </summary>
    /// <returns>Returns the list of all the versions of the WTS.</returns>
    private async Task<List<VersionModel>> GetVersionList()
    {
        logger.LogInformation("Retrieving version list from server.");
        
        using var request = new HttpRequestMessage(HttpMethod.Get, updateSettings.VersionListUrl);
        using var client = new HttpClient(new HttpClientHandler() {AllowAutoRedirect = true});
        using var response = await client.SendAsync(request);
        return await response.Content.ReadFromJsonAsync<List<VersionModel>>();
    }

    /// <summary>
    /// Download the update files to the disk.
    /// </summary>
    /// <param name="version">The version being downloaded.</param>
    private async Task DownloadUpdate(Version version)
    {
        logger.LogInformation("Download the latest update from the server.");
        
        var filePath = Path.Combine(WtsTempPath, "update", "update.zip");
        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }
        
        using var request = new HttpRequestMessage(HttpMethod.Get, updateSettings.VersionDownloadUrl);
        using var client = new HttpClient(new HttpClientHandler() {AllowAutoRedirect = true});
        using var response = await client.SendAsync(request);
        await File.WriteAllBytesAsync(filePath, await response.Content.ReadAsByteArrayAsync());

        lastDownloadedVersion = version;
    }
    
    /// <summary>
    /// Update a single WTS, method is started on its own thread.
    /// </summary>
    /// <param name="wts">The WTS information to update.</param>
    /// <param name="versionList">All the versions of the WTS to be checked against.</param>
    private void UpdateWts(WtsModel wts, List<VersionModel> versionList)
    {
        logger.LogInformation($"Updating WTS '{wts.ServiceName}'.");
        
        var versionInfo = FileVersionInfo.GetVersionInfo(Path.Combine(wts.PathToFolder, WtsExeFile));
        var version = new Version(versionInfo.FileVersion);
        var updateState = CheckForUpdates(version, versionList);

        switch (updateState)
        {
            case UpdateStates.UpToDate:
                logger.LogInformation($"WTS '{wts.ServiceName}' is up-to-date.");
                return;
            case UpdateStates.BreakingChanges:
                logger.LogWarning($"Could not update WTS '{wts.ServiceName}' to version {versionList[0].Version} due to breaking changes since the current version of the WTS ({version}).{Environment.NewLine}Please check the release logs and resolve the breaking changes before manually updating the WTS.");
                EmailAdministrator(wts.ContactEmail, "WTS Auto Updater - Manual action required", $"Could not update WTS '{wts.ServiceName}' to version {versionList[0].Version} due to breaking changes since the current version of the WTS ({version}).<br/>Please check the release logs and resolve the breaking changes before manually updating the WTS.", wts.ServiceName);
                return;
            case UpdateStates.Update:
                PerformUpdate(wts, version, versionList[0].Version);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(updateState), updateState.ToString());
        }
    }

    /// <summary>
    /// Check if the WTS can and needs to be updated.
    /// </summary>
    /// <param name="version">The current version of the WTS.</param>
    /// <param name="versionList">All the versions of the WTS to be checked against.</param>
    /// <returns></returns>
    private UpdateStates CheckForUpdates(Version version, List<VersionModel> versionList)
    {
        if (version == versionList[0].Version)
        {
            return UpdateStates.UpToDate;
        }
        
        for (var i = versionList.Count - 1; i >= 0; i--)
        {
            if (version >= versionList[i].Version)
            {
                continue;
            }

            if (versionList[i].ContainsBreakingChanges)
            {
                return UpdateStates.BreakingChanges;
            }
        }

        return UpdateStates.Update;
    }

    /// <summary>
    /// Update the current WTS to the newest version.
    /// Tries to restore to current version if the update failed.
    /// </summary>
    /// <param name="wts">The WTS information to update.</param>
    /// <param name="currentVersion">The current version of the WTS.</param>
    /// <param name="versionToUpdateTo">The version to update the WTS to.</param>
    private void PerformUpdate(WtsModel wts, Version currentVersion, Version versionToUpdateTo)
    {
        var serviceController = new ServiceController();
        serviceController.ServiceName = wts.ServiceName;
        bool serviceAlreadyStopped;

        // Check if the service has been found. If the status throws an invalid operation exception the service does not exist.
        try
        {
            serviceAlreadyStopped = serviceController.Status == ServiceControllerStatus.Stopped;
        }
        catch (InvalidOperationException)
        {
            EmailAdministrator(wts.ContactEmail, "WTS Auto Updater - WTS not found", $"The service for WTS '{wts.ServiceName}' could not be found on the server and can therefore not be updated.", wts.ServiceName);
            logger.LogWarning($"No service found for '{wts.ServiceName}'.");
            return;
        }

        if (!serviceAlreadyStopped)
        {
            serviceController.Stop();
            serviceController.WaitForStatus(ServiceControllerStatus.Stopped);
            
            // Wait for 1 minute after the service has been stopped to increase the chance all resources have been released.
            Thread.Sleep(UpdateDelayAfterServiceShutdown);
        }

        try
        {
            BackupWts(wts);
            PlaceWts(wts, Path.Combine(WtsTempPath, "update", "update.zip"));

            // If the service was not running when the update started it does not need to restart.
            if (!serviceAlreadyStopped)
            {
                try
                {
                    serviceController.Start();
                    serviceController.WaitForStatus(ServiceControllerStatus.Running);
                }
                catch (InvalidOperationException updateException)
                {
                    RevertUpdate(wts, serviceController, currentVersion, versionToUpdateTo, updateException);

                    return;
                }
            }

            if (wts.SendEmailOnUpdateComplete)
            {
                logger.LogInformation($"WTS '{wts.ServiceName}' has been successfully updated to version {versionToUpdateTo}.");
                EmailAdministrator(wts.ContactEmail, "WTS Auto Updater - Update installed", $"The service for WTS '{wts.ServiceName}' has been successfully updated to version {versionToUpdateTo}.", wts.ServiceName);
            }
        }
        catch (Exception e)
        {
            logger.LogError($"Exception occured while updating WTS '{wts.ServiceName}'.{Environment.NewLine}{Environment.NewLine}{e}");
            EmailAdministrator(wts.ContactEmail, "WTS Auto Updater - Updating failed!", $"Failed to update WTS '{wts.ServiceName}' to version {versionToUpdateTo}.<br/><br/>Error when updating:<br/>{e.ToString().ReplaceLineEndings("<br/>")}", wts.ServiceName);
        }
    }

    /// <summary>
    /// Make a backup of the current WTS.
    /// </summary>
    /// <param name="wts">The WTS information to update.</param>
    private void BackupWts(WtsModel wts)
    {
        var backupPath = Path.Combine(WtsTempPath, "backups", wts.ServiceName);
        Directory.CreateDirectory(backupPath);
        
        // Delete old backups.
        foreach (var file in new DirectoryInfo(backupPath).GetFiles())
        {
            file.Delete();
        }
        
        ZipFile.CreateFromDirectory(wts.PathToFolder,  Path.Combine(backupPath, $"{DateTime.Now:yyyyMMdd}.zip"));
    }

    /// <summary>
    /// Delete all files except the app settings and extract the files from the <see cref="source"/> to the folder.
    /// </summary>
    /// <param name="wts">The WTS information to update.</param>
    /// <param name="source">The source to extract the files from.</param>
    private void PlaceWts(WtsModel wts, string source)
    {
        foreach (var file in new DirectoryInfo(wts.PathToFolder).GetFiles())
        {
            if (file.Name.StartsWith("appsettings") && file.Name.EndsWith(".json"))
            {
                continue;
            }
            
            // Try to delete the file 5 times with a 1 second delay between each attempt.
            var attempts = 0;
            do
            {
                attempts++;
                try
                {
                    file.Delete();
                    break;
                }
                catch (UnauthorizedAccessException)
                {
                    if (attempts == DeleteFileAttempts)
                        throw;
                    
                    Thread.Sleep(DeleteFileDelay);
                }
            } while (attempts < DeleteFileAttempts);
        }
        
        ZipFile.ExtractToDirectory(source, wts.PathToFolder, true);
    }

    /// <summary>
    /// Revert the WTS back to the previous version from the backup made prior to the update.
    /// </summary>
    /// <param name="wts">The WTS information to update.</param>
    /// <param name="serviceController"></param>
    /// <param name="currentVersion"></param>
    /// <param name="versionToUpdateTo"></param>
    /// <param name="updateException"></param>
    private void RevertUpdate(WtsModel wts, ServiceController serviceController, Version currentVersion, Version versionToUpdateTo, InvalidOperationException updateException)
    {
        PlaceWts(wts, Path.Combine(WtsTempPath, "backups", wts.ServiceName, $"{DateTime.Now:yyyyMMdd}.zip"));

        try
        {
            // Try to start the previous installed version again.
            serviceController.Start();
            serviceController.WaitForStatus(ServiceControllerStatus.Running);
            EmailAdministrator(wts.ContactEmail, "WTS Auto Updater - Updating failed!", $"Failed to update WTS '{wts.ServiceName}' to version {versionToUpdateTo}, successfully restored to version {currentVersion}.<br/><br/>Error when updating:<br/>{updateException.ToString().ReplaceLineEndings("<br/>")}", wts.ServiceName);
        }
        catch (InvalidOperationException revertException)
        {
            logger.LogError($"Failed to update WTS '{wts.ServiceName}' to version {versionToUpdateTo}, failed to restore version {currentVersion}.{Environment.NewLine}{Environment.NewLine}Error when reverting:{Environment.NewLine}{revertException}{Environment.NewLine}{Environment.NewLine}Error when updating:<br/>{updateException}");
            EmailAdministrator(wts.ContactEmail, "WTS Auto Updater - Updating and reverting failed!", $"Failed to update WTS '{wts.ServiceName}' to version {versionToUpdateTo}, failed to restore version {currentVersion}.<br/><br/>Error when reverting:<br/>{revertException.ToString().ReplaceLineEndings("<br/>")}<br/><br/>Error when updating:<br/>{updateException.ToString().ReplaceLineEndings("<br/>")}", wts.ServiceName);
        }
    }
    
    /// <summary>
    /// Send a email to the administrator of the WTS.
    /// </summary>
    /// <param name="receiver">The email address of the administrator.</param>
    /// <param name="subject">The subject of the email.</param>
    /// <param name="body">The body of the email.</param>
    /// <param name="serviceName">The name of the WTS service for which an email is being send.</param>
    private void EmailAdministrator(string receiver, string subject, string body, string serviceName)
    {
        if (String.IsNullOrWhiteSpace(receiver))
        {
            logger.LogWarning($"No email address provided for '{serviceName}'.");
            return;
        }
        
        var scope = serviceProvider.CreateScope();
        var communicationsService = scope.ServiceProvider.GetRequiredService<ICommunicationsService>();
        var receivers = new List<CommunicationReceiverModel>();

        foreach (var emailAddress in receiver.Split(';'))
        {
            if (!String.IsNullOrWhiteSpace(emailAddress))
            {
                receivers.Add(new CommunicationReceiverModel() {Address = emailAddress});
            }
        }

        var communication = new SingleCommunicationModel()
        {
            Receivers =  receivers,
            Subject = subject,
            Content = body
        };

        communicationsService.SendEmailDirectlyAsync(communication);
    }
}