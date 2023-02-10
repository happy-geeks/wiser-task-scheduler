using AutoUpdater.Interfaces;

namespace AutoUpdater.Workers;

public class UpdateWorker : BackgroundService
{
    private readonly IUpdateService updateService;
    private readonly ILogger<UpdateWorker> logger;

    public UpdateWorker(IUpdateService updateService, ILogger<UpdateWorker> logger)
    {
        this.updateService = updateService;
        this.logger = logger;
        
        logger.LogInformation("Auto Updater started.");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Execution has started.");
        
        while (!stoppingToken.IsCancellationRequested)
        {
            await updateService.UpdateServicesAsync();
            await Task.Delay(DateTime.Now.Date.AddDays(1) - DateTime.Now, stoppingToken);
        }
    }
}