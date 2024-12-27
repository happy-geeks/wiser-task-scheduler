using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Workers;

public class AutoProjectDeployWorker : BaseWorker
{
    private const string LogName = "AutoProjectDeploy";

    private readonly IAutoProjectDeployService autoProjectDeployService;

    public AutoProjectDeployWorker(IOptions<WtsSettings> wtsSettings, IAutoProjectDeployService autoProjectDeployService, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
    {
        Initialize(LogName, wtsSettings.Value.AutoProjectDeploy.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
        RunScheme.LogSettings ??= new LogSettings();

        this.autoProjectDeployService = autoProjectDeployService;
        this.autoProjectDeployService.LogSettings = RunScheme.LogSettings;
    }

    /// <inheritdoc />
    protected override async Task ExecuteActionAsync(CancellationToken stoppingToken)
    {
        await autoProjectDeployService.ManageAutoProjectDeployAsync(stoppingToken);
    }
}