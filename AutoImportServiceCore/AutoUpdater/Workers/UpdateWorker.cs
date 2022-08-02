namespace AutoUpdater.Workers;

public class UpdateWorker : BackgroundService
{
    private readonly ILogger<UpdateWorker> logger;

    public UpdateWorker(ILogger<UpdateWorker> logger)
    {
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await Task.Delay(1000, stoppingToken);
        }
    }
}