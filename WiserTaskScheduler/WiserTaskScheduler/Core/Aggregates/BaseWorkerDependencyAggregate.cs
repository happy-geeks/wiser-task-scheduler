using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Interfaces;
using WiserTaskScheduler.Modules.Wiser.Interfaces;

namespace WiserTaskScheduler.Core.Aggregates;

/// <summary>
/// An aggregate for the dependencies of the <see cref="BaseWorker"/>.
/// </summary>
public class BaseWorkerDependencyAggregate(
    ILogService logService,
    ISlackChatService slackChatService,
    ILogger<BaseWorker> logger,
    IRunSchemesService runSchemesService,
    IWiserDashboardService wiserDashboardService,
    IErrorNotificationService errorNotificationService,
    IOptions<WtsSettings> wtsSettings)
    : IBaseWorkerDependencyAggregate, IScopedService, ISingletonService
{
    /// <inheritdoc />
    public ILogService LogService { get; } = logService;

    public ISlackChatService SlackChatService { get; } = slackChatService;

    /// <inheritdoc />
    public ILogger<BaseWorker> Logger { get; } = logger;

    /// <inheritdoc />
    public IRunSchemesService RunSchemesService { get; } = runSchemesService;

    /// <inheritdoc />
    public IWiserDashboardService WiserDashboardService { get; } = wiserDashboardService;

    /// <inheritdoc />
    public IErrorNotificationService ErrorNotificationService { get; } = errorNotificationService;

    /// <inheritdoc />
    public WtsSettings WtsSettings { get; } = wtsSettings.Value;
}