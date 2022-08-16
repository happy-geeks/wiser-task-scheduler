namespace AutoUpdater.Enums;

public enum UpdateStates
{
    /// <summary>
    /// The AIS is already up-to-date with the latest version.
    /// </summary>
    UpToDate,
    
    /// <summary>
    /// The AIS need an update and can be updated.
    /// </summary>
    Update,
    
    /// <summary>
    /// The AIS cannot be updated because breaking changes will be introduced.
    /// </summary>
    BreakingChanges
}