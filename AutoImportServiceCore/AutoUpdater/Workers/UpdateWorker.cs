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
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(DateTime.Now.Date.AddDays(1) - DateTime.Now, stoppingToken);
            await updateService.UpdateServicesAsync();
        }
    }
}