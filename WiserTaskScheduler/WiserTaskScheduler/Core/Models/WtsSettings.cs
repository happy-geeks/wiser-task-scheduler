using System;
using System.Collections.Generic;
using WiserTaskScheduler.Core.Models.AutoProjectDeploy;
using WiserTaskScheduler.Core.Models.Cleanup;
using WiserTaskScheduler.Core.Models.ParentsUpdate;
using WiserTaskScheduler.Core.Models.ProductsApiUpdater;
using WiserTaskScheduler.Core.Services;
using WiserTaskScheduler.Modules.Branches.Models;
using WiserTaskScheduler.Modules.Branches.Services;
using WiserTaskScheduler.Modules.Wiser.Models;

namespace WiserTaskScheduler.Core.Models;

/// <summary>
/// The settings for the WTS.
/// </summary>
public class WtsSettings
{
    private readonly string name;

    /// <summary>
    /// Gets or sets the name of the WTS to use for communication.
    /// </summary>
    public string Name
    {
        get => String.IsNullOrWhiteSpace(name) ? $"({Environment.MachineName})" : name;
        init => name = value;
    }

    /// <summary>
    /// Gets or sets the base directory for the secrets file.
    /// </summary>
    public string SecretsBaseDirectory { get; init; }

    /// <summary>
    /// Gets or sets the settings of the <see cref="MainService"/>.
    /// </summary>
    public MainServiceSettings MainService { get; init; } = new();

    /// <summary>
    /// Gets or sets the settings of the <see cref="CleanupService"/>.
    /// </summary>
    public CleanupServiceSettings CleanupService { get; init; } = new();

    /// <summary>
    /// Gets or sets the settings of the <see cref="ProductsApiUpdateService"/>.
    /// </summary>
    public ProductsApiUpdateServiceSettings ProductsApiUpdateService { get; init; } = new();

    /// <summary>
    /// Gets or sets the settings of the <see cref="ParentsUpdateService"/>.
    /// </summary>
    public ParentsUpdateServiceSettings ParentsUpdateService { get; init; } = new();

    /// <summary>
    /// Gets or sets the settings for the connection to Wiser 3.
    /// </summary>
    public WiserSettings Wiser { get; init; } = new();

    /// <summary>
    /// Gets or sets the <see cref="SlackSettings"/> used by the <see cref="SlackChatService"/>.
    /// </summary>
    public SlackSettings SlackSettings { get; init; } = new();

    /// <summary>
    /// Gets or sets the <see cref="AutoProjectDeploy"/> used by the <see cref="AutoProjectDeployService"/>.
    /// </summary>
    public AutoProjectDeploySettings AutoProjectDeploy { get; init; } = new();

    /// <summary>
    /// Gets or sets the <see cref="BatchLoggerOptions"/> used by the <see cref="BranchBatchLoggerService"/>.
    /// </summary>
    public BatchLoggerOptions BatchLoggerOptions { get; init; } = new();

    /// <summary>
    /// A semicolon (;) seperated list of email addresses to notify when a core service failed during execution.
    /// </summary>
    public string ServiceFailedNotificationEmails { get; init; }

    /// <summary>
    /// The number of minutes to wait before sending the same error notification again.
    /// </summary>
    public int ErrorNotificationsIntervalInMinutes { get; init; } = 60;

    /// <summary>
    /// Gets or sets the credentials from the secrets app settings.
    /// </summary>
    public IReadOnlyDictionary<string, string> Credentials { get; init; } = new Dictionary<string, string>();
}