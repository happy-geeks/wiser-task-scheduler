namespace AutoUpdater.Interfaces;

public interface IUpdateService
{
    /// <summary>
    /// Update all the services that have been configured in the app settings file.
    /// </summary>
    /// <returns></returns>
    Task UpdateServicesAsync();
}