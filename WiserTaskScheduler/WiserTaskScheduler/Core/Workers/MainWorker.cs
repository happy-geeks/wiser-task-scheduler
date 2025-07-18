﻿using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using Environment = System.Environment;

namespace WiserTaskScheduler.Core.Workers;

/// <summary>
/// The <see cref="MainWorker"/> manages all WTS configurations that are provided by Wiser.
/// </summary>
public class MainWorker : BaseWorker
{
    private const string LogName = "MainService";

    private readonly IMainService mainService;
    private readonly ILogService logService;
    private readonly ILogger<MainWorker> logger;
    private readonly ISlackChatService slackChatService;

    private readonly string wtsName;

    /// <summary>
    /// Creates a new instance of <see cref="MainWorker"/>.
    /// </summary>
    /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
    /// <param name="mainService">The main service to handle the main functionality of the WTS.</param>
    /// <param name="slackChatService">The service for sending updates to a Slack channel.</param>
    /// <param name="logService">The service to use for logging.</param>
    /// <param name="logger">The logger to use for logging.</param>
    /// <param name="baseWorkerDependencyAggregate">The aggregate containing the dependencies needed by the <see cref="BaseWorker"/>.</param>
    public MainWorker(IOptions<WtsSettings> wtsSettings, IMainService mainService, ISlackChatService slackChatService, ILogService logService, ILogger<MainWorker> logger, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
    {
        Initialize(LogName, wtsSettings.Value.MainService.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
        RunScheme.LogSettings ??= new LogSettings();

        this.mainService = mainService;
        this.logService = logService;
        this.logger = logger;
        this.slackChatService = slackChatService;

        wtsName = $"{Environment.MachineName} - {wtsSettings.Value.Name}";

        this.mainService.LogSettings = RunScheme.LogSettings;

        slackChatService.SendChannelMessageAsync($"*Wiser Task Scheduler has started ({wtsName})*");
    }

    /// <inheritdoc />
    protected override async Task ExecuteActionAsync(CancellationToken stoppingToken)
    {
        await mainService.ManageConfigurations();
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, "Main worker needs to stop, stopping all configuration workers.", Name, RunScheme.TimeId);
        await slackChatService.SendChannelMessageAsync($"*Wiser Task Scheduler is shutting down ({wtsName})*");
        await mainService.StopAllConfigurationsAsync();
        await logService.LogInformation(logger, LogScopes.StartAndStop, RunScheme.LogSettings, "All configuration workers have stopped, stopping main worker.", Name, RunScheme.TimeId);
        await base.StopAsync(cancellationToken);
        await slackChatService.SendChannelMessageAsync($"*Wiser Task Scheduler was shut down ({wtsName})*");
    }
}