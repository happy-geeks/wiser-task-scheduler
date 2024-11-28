using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.RunSchemes.Models;

namespace WiserTaskScheduler.Core.Workers;

public class AutoProjectDeployWorker : BaseWorker
{
    private const string LogName = "AutoProjectDeploy";

    private readonly IAutoProjectDeployService autoProjectDeployService;
    private readonly ILogService logService;
    private readonly ILogger<AutoProjectDeployWorker> logger;

    public AutoProjectDeployWorker(IOptions<WtsSettings> wtsSettings, IAutoProjectDeployService autoProjectDeployService, ILogService logService, ILogger<AutoProjectDeployWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
    {
        Initialize(LogName, wtsSettings.Value.AutoProjectDeploy.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
        RunScheme.LogSettings ??= new LogSettings();

        this.autoProjectDeployService = autoProjectDeployService;
        this.logService = logService;
        this.logger = logger;

        this.autoProjectDeployService.LogSettings = RunScheme.LogSettings;
    }

    /// <inheritdoc />
    protected override async Task ExecuteActionAsync()
    {
        await autoProjectDeployService.ManageAutoProjectDeployAsync();
    }
}