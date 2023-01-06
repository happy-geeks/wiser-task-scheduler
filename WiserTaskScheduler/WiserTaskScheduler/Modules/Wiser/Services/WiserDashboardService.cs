using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using WiserTaskScheduler.Modules.Wiser.Interfaces;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.WiserDashboard.Models;
using Microsoft.Extensions.DependencyInjection;

namespace WiserTaskScheduler.Modules.Wiser.Services;

public class WiserDashboardService : IWiserDashboardService, ISingletonService
{
    private readonly IServiceProvider serviceProvider;

    public WiserDashboardService(IServiceProvider serviceProvider)
    {
        this.serviceProvider = serviceProvider;
    }
    
    /// <inheritdoc />
    public async Task<Service> GetServiceAsync(string configuration, int timeId)
    {
        var query = $@"SELECT *
FROM {WiserTableNames.WtsServices}
WHERE configuration = ?configuration
AND time_id = ?timeId";

        var parameters = new Dictionary<string, object>()
        {
            {"configuration", configuration},
            {"timeId", timeId}
        };

        var data = await ExecuteQueryAsync(query, parameters);

        if (data.Rows.Count == 0)
        {
            return null;
        }
        
        return GetServiceFromDataRow(data.Rows[0]);
    }
    
    /// <inheritdoc />
    public async Task<List<Service>> GetServices(bool onlyWithExtraRun)
    {
        var query = $@"SELECT  *
FROM {WiserTableNames.WtsServices}
{(onlyWithExtraRun ? "WHERE extra_run = 1" : "")}";

        var data = await ExecuteQueryAsync(query);
        return GetServicesFromData(data);
    }

    /// <inheritdoc />
    public async Task CreateServiceAsync(string configuration, int timeId)
    {
        var query = $"INSERT INTO {WiserTableNames.WtsServices} (configuration, time_id) VALUES (?configuration, ?timeId)";
        var parameters = new Dictionary<string, object>()
        {
            {"configuration", configuration},
            {"timeId", timeId}
        };

        await ExecuteQueryAsync(query, parameters);
    }

    /// <inheritdoc />
    public async Task UpdateServiceAsync(string configuration, int timeId, string action = null, string scheme = null, DateTime? lastRun = null, DateTime? nextRun = null, TimeSpan? runTime = null, string state = null, bool? paused = null, bool? extraRun = null, int templateId = -1)
    {
        var querySetParts = new List<string>();
        var parameters = new Dictionary<string, object>()
        {
            {"configuration", configuration},
            {"timeId", timeId}
        };

        if (action != null)
        {
            querySetParts.Add("action = ?action");
            parameters.Add("action", action);
        }

        if (scheme != null)
        {
            querySetParts.Add("scheme = ?scheme");
            parameters.Add("scheme", scheme);
        }
        
        if (lastRun != null)
        {
            querySetParts.Add("last_run = ?lastRun");
            parameters.Add("lastRun", lastRun);
        }
        
        if (nextRun != null)
        {
            querySetParts.Add("next_run = ?nextRun");
            parameters.Add("nextRun", nextRun);
        }
        
        if (runTime != null)
        {
            querySetParts.Add("run_time = ?runTime");
            parameters.Add("runTime", runTime.Value.TotalMinutes);
        }
        
        if (state != null)
        {
            querySetParts.Add("state = ?state");
            parameters.Add("state", state);
        }

        if (paused.HasValue)
        {
            querySetParts.Add("paused = ?paused");
            parameters.Add("paused", paused.Value);
        }

        if (extraRun.HasValue)
        {
            querySetParts.Add("extra_run = ?extraRun");
            parameters.Add("extraRun", extraRun);
        }

        if (!querySetParts.Any())
        {
            return;
        }

        if (templateId >= 0)
        {
            querySetParts.Add("template_id = ?templateId");
            parameters.Add("templateId", templateId);
        }

        var query = $"UPDATE {WiserTableNames.WtsServices} SET {String.Join(',', querySetParts)} WHERE configuration = ?configuration AND time_id = ?timeId";
        await ExecuteQueryAsync(query, parameters);
    }

    /// <summary>
    /// Execute a given query with the given parameters.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">The parameters to set before executing the query.</param>
    /// <returns>Returns the <see cref="DataTable"/> from the query result.</returns>
    private async Task<DataTable> ExecuteQueryAsync(string query, Dictionary<string, object> parameters = null)
    {
        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        if (parameters != null && parameters.Any())
        {
            foreach (var parameter in parameters)
            {
                databaseConnection.AddParameter(parameter.Key, parameter.Value);
            }
        }

        return await databaseConnection.GetAsync(query);
    }

    /// <summary>
    /// Get a <see cref="Service"/> from a <see cref="DataRow"/>.
    /// </summary>
    /// <param name="row">The <see cref="DataRow"/> to get the <see cref="Service"/> from.</param>
    /// <returns></returns>
    private Service GetServiceFromDataRow(DataRow row)
    {
        return new Service
        {
            Id = row.Field<int>("id"),
            Configuration = row.Field<string>("configuration"),
            TimeId = row.Field<int>("time_id"),
            Action = row.Field<string>("action"),
            Scheme = row.Field<string>("scheme"),
            LastRun = row.Field<DateTime?>("last_run"),
            NextRun = row.Field<DateTime?>("next_run"),
            RunTime = row.IsNull("run_time") ? 0 : row.Field<double>("run_time"),
            State = row.Field<string>("state"),
            TemplateId = row.Field<int>("template_id")
        };
    }

    /// <summary>
    /// Get a collection of <see cref="Service"/>s from a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="data">The <see cref="DataTable"/> to get the <see cref="Service"/>s from.</param>
    /// <returns></returns>
    private List<Service> GetServicesFromData(DataTable data)
    {
        return (from DataRow row in data.Rows select GetServiceFromDataRow(row)).ToList();
    }

    /// <inheritdoc />
    public async Task<List<string>> GetLogStatesFromLastRun(string configuration, int timeId, DateTime runStartTime)
    {
        var states = new List<string>();
        
        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        databaseConnection.AddParameter("runStartTime", runStartTime);
        databaseConnection.AddParameter("configuration", configuration);
        databaseConnection.AddParameter("timeId", timeId);
        
        var data = await databaseConnection.GetAsync($@"SELECT DISTINCT level
FROM {WiserTableNames.WtsLogs}
WHERE added_on >= ?runStartTime
AND configuration = ?configuration
AND time_id = ?timeId");

        foreach (DataRow row in data.Rows)
        {
            states.Add(row.Field<string>("level"));
        }

        return states;
    }

    /// <inheritdoc />
    public async Task<bool> IsServicePaused(string configuration, int timeId)
    {
        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        databaseConnection.AddParameter("configuration", configuration);
        databaseConnection.AddParameter("timeId", timeId);

        var dataTable = await databaseConnection.GetAsync($@"SELECT paused
FROM {WiserTableNames.WtsServices}
WHERE configuration = ?configuration
AND time_id = ?timeId");

        // If no service is found for this combination treat it as paused to prevent unwanted executions.
        if (dataTable.Rows.Count == 0)
        {
            return true;
        }

        return dataTable.Rows[0].Field<bool>("paused");
    }
    
    /// <inheritdoc />
    public async Task<bool> IsServiceRunning(string configuration, int timeId)
    {
        using var scope = serviceProvider.CreateScope();
        using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        databaseConnection.AddParameter("configuration", configuration);
        databaseConnection.AddParameter("timeId", timeId);

        var dataTable = await databaseConnection.GetAsync($@"SELECT state
FROM {WiserTableNames.WtsServices}
WHERE configuration = ?configuration
AND time_id = ?timeId");
        
        return dataTable.Rows[0].Field<string>("state").Equals("running", StringComparison.InvariantCultureIgnoreCase);
    }
}