using System.Diagnostics;
using System.IO.Compression;
using System.Net.Http.Json;
using System.ServiceProcess;
using AutoUpdater.Enums;
using AutoUpdater.Interfaces;
using AutoUpdater.Models;
using GeeksCoreLibrary.Modules.Communication.Models;
using HelperLibrary;
using Microsoft.Extensions.Options;
using Serilog;

namespace AutoUpdater.Services;

public class UpdateService : IUpdateService
{
    private const string AisTempPath = "C:/temp/ais";
    private const string AisExeFile = "AutoImportServiceCore.exe";
    private const string VersionListUrl = "https://localhost:44306/versions.json";
    private const string VersionDownloadUrl = "https://localhost:44306/Update.zip";

    private readonly UpdateSettings updateSettings;
    private readonly ILogger<UpdateService> logger;
    private readonly IServiceProvider serviceProvider;

    private Version lastDownloadedVersion;
    
    public UpdateService(IOptions<UpdateSettings> updateSettings, ILogger<UpdateService> logger, IServiceProvider serviceProvider)
    {
        this.updateSettings = updateSettings.Value;
        this.logger = logger;
        this.serviceProvider = serviceProvider;
        
        Directory.CreateDirectory(Path.Combine(AisTempPath, "update"));
        Directory.CreateDirectory(Path.Combine(AisTempPath, "backups"));
    }

    /// <inheritdoc />
    public async Task UpdateServicesAsync()
    {
        logger.LogInformation("Starting with updating the AIS services.");

        try
        {
            var versionList = await GetVersionList();
            if (lastDownloadedVersion == null || lastDownloadedVersion != versionList[0].Version)
            {
                await DownloadUpdate(versionList[0].Version);
            }

            foreach (var ais in updateSettings.AisInstancesToUpdate)
            {
                new Thread(() => UpdateAis(ais, versionList)).Start();
            }
        }
        catch (Exception e)
        {
            logger.LogError($"Failed to update the AIS services due to exception:{Environment.NewLine}{Environment.NewLine}{e}");
        }
    }

    /// <summary>
    /// Get the list of all the versions of the AIS.
    /// </summary>
    /// <returns>Returns the list of all the versions of the AIS.</returns>
    private async Task<List<VersionModel>> GetVersionList()
    {
        logger.LogInformation("Retrieving version list from server.");
        
        using var request = new HttpRequestMessage(HttpMethod.Get, VersionListUrl);
        using var client = new HttpClient();
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
        
        var filePath = Path.Combine(AisTempPath, "update", "Update.zip");
        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }
        
        using var request = new HttpRequestMessage(HttpMethod.Get, VersionDownloadUrl);
        using var client = new HttpClient();
        using var response = await client.SendAsync(request);
        await File.WriteAllBytesAsync(filePath, await response.Content.ReadAsByteArrayAsync());

        lastDownloadedVersion = version;
    }
    
    /// <summary>
    /// Update a single AIS, method is started on its own thread.
    /// </summary>
    /// <param name="ais">The AIS information to update.</param>
    /// <param name="versionList">All the versions of the AIS to be checked against.</param>
    private void UpdateAis(AisModel ais, List<VersionModel> versionList)
    {
        logger.LogInformation($"Updating AIS '{ais.ServiceName}'.");
        
        var versionInfo = FileVersionInfo.GetVersionInfo(Path.Combine(ais.PathToFolder, AisExeFile));
        var version = new Version(versionInfo.FileVersion);
        var updateState = CheckForUpdates(version, versionList);

        switch (updateState)
        {
            case UpdateStates.UpToDate:
                return;
            case UpdateStates.BreakingChanges:
                logger.LogWarning($"Could not update AIS '{ais.ServiceName}' to version {versionList[0].Version} due to breaking changes since the current version of the AIS ({version}).{Environment.NewLine}Please check the release logs and resolve the breaking changes before manually updating the AIS.");
                EmailAdministrator(ais.ContactEmail, "AIS Auto Updater - Manual action required", $"Could not update AIS '{ais.ServiceName}' to version {versionList[0].Version} due to breaking changes since the current version of the AIS ({version}).<br/>Please check the release logs and resolve the breaking changes before manually updating the AIS.");
                return;
            case UpdateStates.Update:
                PerformUpdate(ais, version, versionList[0].Version);
                return;
            default:
                throw new NotImplementedException($"Update state '{updateState}' is not yet implemented.");
        }
    }

    /// <summary>
    /// Check if the AIS can and needs to be updated.
    /// </summary>
    /// <param name="version">The current version of the AIS.</param>
    /// <param name="versionList">All the versions of the AIS to be checked against.</param>
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
    /// 
    /// </summary>
    /// <param name="ais">The AIS information to update.</param>
    /// <param name="currentVersion"></param>
    /// <param name="versionToUpdateTo"></param>
    private void PerformUpdate(AisModel ais, Version currentVersion, Version versionToUpdateTo)
    {
        var serviceController = new ServiceController();
        serviceController.ServiceName = ais.ServiceName;
        
        // Check if the service has been found. If the status throws an invalid operation exception the service does not exist.
        try
        {
            _ = serviceController.Status;
        }
        catch (InvalidOperationException)
        {
            EmailAdministrator(ais.ContactEmail, "AIS Auto Updater - AIS not found", $"The service for AIS '{ais.ServiceName}' could not be found on the server and can therefore not be updated.");
            logger.LogWarning($"No service found for '{ais.ServiceName}'.");
            return;
        }

        var serviceAlreadyStopped = serviceController.Status == ServiceControllerStatus.Stopped;
        
        if (!serviceAlreadyStopped)
        {
            serviceController.Stop();
            serviceController.WaitForStatus(ServiceControllerStatus.Stopped);
        }

        try
        {
            BackupAis(ais);
            PlaceAis(ais, Path.Combine(AisTempPath, "update", "Update.zip"));

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
                    RevertUpdate(ais, serviceController, currentVersion, versionToUpdateTo, updateException);

                    return;
                }
            }

            if (ais.SendEmailOnUpdateComplete)
            {
                logger.LogInformation($"AIS '{ais.ServiceName}' has been successfully updated to version {versionToUpdateTo}.");
                EmailAdministrator(ais.ContactEmail, "AIS Auto Updater - Update installed", $"The AIS has been successfully updated to version {versionToUpdateTo}.");
            }
        }
        catch (Exception e)
        {
            logger.LogError($"Exception occured while updating AIS '{ais.ServiceName}'.{Environment.NewLine}{Environment.NewLine}{e}");
        }
    }

    /// <summary>
    /// Make a backup of the current AIS.
    /// </summary>
    /// <param name="ais">The AIS information to update.</param>
    private void BackupAis(AisModel ais)
    {
        var backupPath = Path.Combine(AisTempPath, "backups", ais.ServiceName);
        Directory.CreateDirectory(backupPath);
        
        // Delete old backups.
        foreach (var file in new DirectoryInfo(backupPath).GetFiles())
        {
            file.Delete();
        }
        
        ZipFile.CreateFromDirectory(ais.PathToFolder,  Path.Combine(backupPath, $"{DateTime.Now:yyyyMMdd}.zip"));
    }

    /// <summary>
    /// Delete all files except the app settings and extract the files from the <see cref="source"/> to the folder.
    /// </summary>
    /// <param name="ais">The AIS information to update.</param>
    /// <param name="source">The source to extract the files from.</param>
    private void PlaceAis(AisModel ais, string source)
    {
        foreach (var file in new DirectoryInfo(ais.PathToFolder).GetFiles())
        {
            if (file.Name.StartsWith("appsettings") && file.Name.EndsWith(".json"))
            {
                continue;
            }
            
            file.Delete();
        }
        ZipFile.ExtractToDirectory(source, ais.PathToFolder, true);
    }

    /// <summary>
    /// Revert the AIS back to the previous version from the backup made prior to the update.
    /// </summary>
    /// <param name="ais">The AIS information to update.</param>
    /// <param name="serviceController"></param>
    /// <param name="currentVersion"></param>
    /// <param name="versionToUpdateTo"></param>
    /// <param name="updateException"></param>
    private void RevertUpdate(AisModel ais, ServiceController serviceController, Version currentVersion, Version versionToUpdateTo, InvalidOperationException updateException)
    {
        PlaceAis(ais, Path.Combine(AisTempPath, "backups", ais.ServiceName, $"{DateTime.Now:yyyyMMdd}.zip"));

        try
        {
            // Try to start the previous installed version again.
            serviceController.Start();
            serviceController.WaitForStatus(ServiceControllerStatus.Running);
            EmailAdministrator(ais.ContactEmail, "AIS Auto Updater - Updating failed!", $"Failed to update AIS '{ais.ServiceName}' to version {versionToUpdateTo}, successfully restored to version {currentVersion}.<br/><br/>Error when updating:<br/>{updateException.ToString().ReplaceLineEndings("<br/>")}");
        }
        catch (InvalidOperationException revertException)
        {
            logger.LogError($"Failed to update AIS '{ais.ServiceName}' to version {versionToUpdateTo}, failed to restore version {currentVersion}.{Environment.NewLine}{Environment.NewLine}Error when reverting:{Environment.NewLine}{revertException}{Environment.NewLine}{Environment.NewLine}Error when updating:<br/>{updateException}");
            EmailAdministrator(ais.ContactEmail, "AIS Auto Updater - Updating and reverting failed!", $"Failed to update AIS '{ais.ServiceName}' to version {versionToUpdateTo}, failed to restore version {currentVersion}.<br/><br/>Error when reverting:<br/>{revertException.ToString().ReplaceLineEndings("<br/>")}<br/><br/>Error when updating:<br/>{updateException.ToString().ReplaceLineEndings("<br/>")}");
        }
    }
    
    /// <summary>
    /// Send a email to the administrator of the AIS.
    /// </summary>
    /// <param name="receiver">The email address of the administrator.</param>
    /// <param name="subject">The subject of the email.</param>
    /// <param name="body">The body of the email.</param>
    private void EmailAdministrator(string receiver, string subject, string body)
    {
        var scope = serviceProvider.CreateScope();
        var communicationsService = GclServicesHelper.GetCommunicationsService(scope, null);
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

        communicationsService.SendEmailDirectlyAsync(communication, updateSettings.MailSettings);
    }
}