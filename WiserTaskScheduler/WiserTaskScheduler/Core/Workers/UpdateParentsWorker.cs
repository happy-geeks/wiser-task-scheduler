using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Workers;

public class UpdateParentsWorker : BaseWorker
{
    private const string LogName = "ParentUpdateService";

    private readonly IParentUpdateService parentUpdateService;

    /// <summary>
    /// Creates a new instance of <see cref="UpdateParentsWorker"/>.
    /// </summary>
    /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
    /// <param name="parentUpdateService">The service to handle updating the changed information of the item.</param>
    /// <param name="baseWorkerDependencyAggregate">The aggregate containing the dependencies needed by the <see cref="BaseWorker"/>.</param>
    public UpdateParentsWorker(IOptions<WtsSettings> wtsSettings, IParentUpdateService parentUpdateService, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
    {
        Initialize(LogName, wtsSettings.Value.ParentsUpdateService.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
        RunScheme.LogSettings = wtsSettings.Value.ParentsUpdateService.LogSettings;

        this.parentUpdateService = parentUpdateService;

        this.parentUpdateService.LogSettings = RunScheme.LogSettings;
    }

    /// <inheritdoc />
    protected override async Task ExecuteActionAsync(CancellationToken stoppingToken)
    {
        await parentUpdateService.ParentsUpdateAsync();
    }
}