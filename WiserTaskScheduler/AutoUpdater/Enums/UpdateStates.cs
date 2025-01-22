namespace AutoUpdater.Enums;

public enum UpdateStates
{
    /// <summary>
    /// The WTS is already up-to-date with the latest version.
    /// </summary>
    UpToDate,

    /// <summary>
    /// The WTS needs an update and can be updated.
    /// </summary>
    Update,

    /// <summary>
    /// The WTS cannot be updated because breaking changes will be introduced.
    /// </summary>
    BreakingChanges
}