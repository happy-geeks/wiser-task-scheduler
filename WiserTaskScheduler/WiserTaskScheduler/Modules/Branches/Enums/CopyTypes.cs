namespace WiserTaskScheduler.Modules.Branches.Enums;

public enum CopyTypes
{
    /// <summary>
    /// Fully skip the given table.
    /// </summary>
    Nothing,

    /// <summary>
    /// Copy the structure of the table but not its data.
    /// </summary>
    Structure
}