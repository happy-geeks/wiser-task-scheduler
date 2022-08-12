using System.IO.Compression;
using System.ServiceProcess;
using AutoUpdater.Models;
using GeeksCoreLibrary.Modules.Communication.Models;
using HelperLibrary;
using Microsoft.Extensions.Options;

namespace AutoUpdater.Workers;

public class UpdateWorker : BackgroundService
{
    private const string AisTempPath = "C:/temp/ais";
    
    private readonly UpdateSettings updateSettings;
    private readonly ILogger<UpdateWorker> logger;
    private readonly IServiceProvider serviceProvider;

    public UpdateWorker(IOptions<UpdateSettings> updateSettings, ILogger<UpdateWorker> logger, IServiceProvider serviceProvider)
    {
        this.updateSettings = updateSettings.Value;
        this.logger = logger;
        this.serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Directory.CreateDirectory(Path.Combine(AisTempPath, "update"));
        Directory.CreateDirectory(Path.Combine(AisTempPath, "backups"));
        
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var ais in updateSettings.AisInstancesToUpdate)
            {
                new Thread(() => UpdateAis(ais)).Start();
            }
            await Task.Delay(DateTime.Now.Date.AddDays(1) - DateTime.Now, stoppingToken);
        }
    }

    private void UpdateAis(AisModel ais)
    {
        var serviceController = new ServiceController();
        serviceController.ServiceName = ais.ServiceName;
        
        // Check if the service has been found. If the status throws an invalid operation exception the service does not exist.
        try
        {
            _ = serviceController.Status;
        }
        catch (InvalidOperationException e)
        {
            logger.LogInformation("No service found");
            return;
        }

        var serviceAlreadyStopped = serviceController.Status == ServiceControllerStatus.Stopped;
        
        if (!serviceAlreadyStopped)
        {
            serviceController.Stop();
            serviceController.WaitForStatus(ServiceControllerStatus.Stopped);
        }

        // Cleanup old backup and create a new one.
        var backupPath = Path.Combine(AisTempPath, "backups", ais.ServiceName);
        Directory.CreateDirectory(backupPath);
        foreach (var file in new DirectoryInfo(backupPath).GetFiles())
        {
            file.Delete();
        }
        ZipFile.CreateFromDirectory(ais.PathToFolder,  Path.Combine(backupPath, $"{DateTime.Now:yyyyMMdd}.zip"));
        
        // TODO Replace files
        foreach (var file in new DirectoryInfo(ais.PathToFolder).GetFiles())
        {
            if (file.Name.StartsWith("appsettings") && file.Name.EndsWith(".json"))
            {
                continue;
            }
            
            file.Delete();
        }
        ZipFile.ExtractToDirectory(Path.Combine(AisTempPath, "update", "Update.zip"), ais.PathToFolder);

        // If the service was not running when the update started it does not need to restart.
        if (!serviceAlreadyStopped)
        {
            serviceController.Start();
            serviceController.WaitForStatus(ServiceControllerStatus.Running);
        }
        
        if (ais.SendMailOnUpdateComplete)
        {
            MailUser(ais.ContactEmail, "AIS Auto Updater", "The AIS has been successfully updated to version ...");
        }
    }

    private void MailUser(string receiver, string subject, string body)
    {
        var scope = serviceProvider.CreateScope();
        var communicationsService = GclServicesHelper.GetCommunicationsService(scope, null);

        var communication = new SingleCommunicationModel()
        {
            Receivers = new List<CommunicationReceiverModel> {new() {Address = receiver}},
            Subject = subject,
            Content = body
        };

        communicationsService.SendEmailDirectlyAsync(communication, updateSettings.MailSettings);
    }
}