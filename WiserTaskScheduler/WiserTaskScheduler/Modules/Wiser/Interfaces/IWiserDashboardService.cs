using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using GeeksCoreLibrary.Modules.WiserDashboard.Models;

namespace WiserTaskScheduler.Modules.Wiser.Interfaces;

public interface IWiserDashboardService
{
    /// <summary>
    /// Get the service based on configuration and time ID from the database.
    /// </summary>
    /// <param name="configuration">The name of the configuration.</param>
    /// <param name="timeId">The time ID of the service.</param>
    /// <returns></returns>
    Task<Service> GetServiceAsync(string configuration, int timeId);

    /// <summary>
    /// Get all services, can be limited to only services that require an extra run.
    /// </summary>
    /// <param name="onlyWithExtraRun">If only services marked for an extra run need to be retrieved.</param>
    /// <returns></returns>
    Task<List<Service>> GetServices(bool onlyWithExtraRun);

    /// <summary>
    /// Create a service to be stored in the database.
    /// </summary>
    /// <param name="configuration">The name of the configuration.</param>
    /// <param name="timeId">The time ID of the service.</param>
    /// <returns></returns>
    Task CreateServiceAsync(string configuration, int timeId);

    /// <summary>
    /// Update a service in the database.
    /// Any optional values will be ignored if no value is set.
    /// </summary>
    /// <param name="configuration">The name of the configuration.</param>
    /// <param name="timeId">The time ID of the service.</param>
    /// <param name="action">Optional: The action.</param>
    /// <param name="scheme">Optional: The scheme type.</param>
    /// <param name="lastRun">Optional: The date time of the last time the service ran.</param>
    /// <param name="nextRun">Optional: The date time of the service it will run.</param>
    /// <param name="runTime">Optional: The time it took to complete the last run.</param>
    /// <param name="state">Optional: The current state of the service.</param>
    /// <param name="paused">Optional: The paused state of the service. Leave null to keep the current value.</param>
    /// <param name="extraRun">Optional: If the service need to be run an extra time. Leave null to keep the current value.</param>
    /// <param name="templateId">Optional: The ID of the template that of the service</param>
    /// <returns></returns>
    Task UpdateServiceAsync(string configuration, int timeId, string action = null, string scheme = null, DateTime? lastRun = null, DateTime? nextRun = null, TimeSpan? runTime = null, string state = null, bool? paused = null, bool? extraRun = null, int templateId = -1);

    /// <summary>
    /// Get the unique states that logs have been written to since a certain time for a run scheme.
    /// </summary>
    /// <param name="configuration">The name of the configuration.</param>
    /// <param name="timeId">The time ID of the run scheme.</param>
    /// <param name="runStartTime">The datetime the run scheme started.</param>
    /// <returns></returns>
    Task<List<string>> GetLogStatesFromLastRun(string configuration, int timeId, DateTime runStartTime);

    /// <summary>
    /// Check if the service is paused.
    /// </summary>
    /// <param name="configuration">The name of the configuration.</param>
    /// <param name="timeId">The time ID of the run scheme.</param>
    /// <returns></returns>
    Task<bool> IsServicePaused(string configuration, int timeId);

    /// <summary>
    /// Check if the service is already running.
    /// </summary>
    /// <param name="configuration">The name of the configuration.</param>
    /// <param name="timeId">The time ID of the run scheme.</param>
    /// <returns></returns>
    Task<bool> IsServiceRunning(string configuration, int timeId);
}