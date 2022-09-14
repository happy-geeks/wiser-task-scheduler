using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using AutoImportServiceCore.Modules.Wiser.Interfaces;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.WiserDashboard.Models;
using Microsoft.Extensions.DependencyInjection;

namespace AutoImportServiceCore.Modules.Wiser.Services;

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
FROM {WiserTableNames.AisServices}
WHERE configuration = ?configuration
AND time_id = ?timeId";

        var parameters = new Dictionary<string, object>()
        {
            {"configuration", configuration},
            {"timeId", timeId}
        };

        var data = await ExecuteQueryAsync(query, parameters);
        return GetServiceFromData(data);
    }

    /// <inheritdoc />
    public async Task CreateServiceAsync(string configuration, int timeId)
    {
        var query = $"INSERT INTO {WiserTableNames.AisServices} (configuration, time_id) VALUES (?configuration, ?timeId)";
        var parameters = new Dictionary<string, object>()
        {
            {"configuration", configuration},
            {"timeId", timeId}
        };

        await ExecuteQueryAsync(query, parameters);
    }

    /// <inheritdoc />
    public async Task UpdateServiceAsync(string configuration, int timeId, string action = null, string scheme = null, DateTime? lastRun = null, DateTime? nextRun = null, TimeSpan? runTime = null, string state = null)
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

        if (!querySetParts.Any())
        {
            return;
        }

        var query = $"UPDATE {WiserTableNames.AisServices} SET {String.Join(',', querySetParts)} WHERE configuration = ?configuration AND time_id = ?timeId";
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
    /// Get a <see cref="Service"/> from a <see cref="DataTable"/>.
    /// </summary>
    /// <param name="data">The <see cref="DataTable"/> to get the <see cref="Service"/> from.</param>
    /// <returns>Returns a <see cref="Service"/> or <see langword="null"/>.</returns>
    private Service GetServiceFromData(DataTable data)
    {
        if (data.Rows.Count == 0)
            return default;
        
        return new Service()
        {
            Id = data.Rows[0].Field<int>("id"),
            Configuration = data.Rows[0].Field<string>("configuration"),
            TimeId = data.Rows[0].Field<int>("time_id"),
            Action = data.Rows[0].Field<string>("action"),
            Scheme = data.Rows[0].Field<string>("scheme"),
            LastRun = data.Rows[0].Field<DateTime?>("last_run"),
            NextRun = data.Rows[0].Field<DateTime?>("next_run"),
            RunTime = data.Rows[0].Field<double>("run_time"),
            State = data.Rows[0].Field<string>("state")
        };
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
FROM {WiserTableNames.AisLogs}
WHERE added_on >= ?runStartTime
AND configuration = ?configuration
AND time_id = ?timeId
AND is_test = 0");

        foreach (DataRow row in data.Rows)
        {
            states.Add(row.Field<string>("level"));
        }

        return states;
    }
}