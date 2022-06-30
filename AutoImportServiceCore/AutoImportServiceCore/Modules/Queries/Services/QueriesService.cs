using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Helpers;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Queries.Interfaces;
using AutoImportServiceCore.Modules.Queries.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Helpers;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.Queries.Services
{
    /// <summary>
    /// A service for a query action.
    /// </summary>
    public class QueriesService : IQueriesService, IActionsService, IScopedService
    {
        private readonly ILogService logService;
        private readonly ILogger<QueriesService> logger;
        private readonly IServiceProvider serviceProvider;

        private string connectionString;

        /// <summary>
        /// Creates a new instance of <see cref="QueriesService"/>.
        /// </summary>
        public QueriesService(ILogService logService, ILogger<QueriesService> logger, IServiceProvider serviceProvider)
        {
            this.logService = logService;
            this.logger = logger;
            this.serviceProvider = serviceProvider;
        }

        /// <inheritdoc />
        public async Task Initialize(ConfigurationModel configuration)
        {
            connectionString = configuration.ConnectionString;
        }

        /// <inheritdoc />
        public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
        {
            using var scope = serviceProvider.CreateScope();
            var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

            var query = (QueryModel)action;
            await databaseConnection.ChangeConnectionStringsAsync(connectionString, connectionString);
            databaseConnection.ClearParameters();

            // Enforce the set character set and collation that is used during the execution of this action.
            databaseConnection.AddParameter("characterSet", query.CharacterEncoding.CharacterSet);
            databaseConnection.AddParameter("collation", query.CharacterEncoding.Collation);
            await databaseConnection.GetAsync("SET NAMES ?characterSet COLLATE ?collation", cleanUp: false);
            databaseConnection.ClearParameters();

            await logService.LogInformation(logger, LogScopes.RunStartAndStop, query.LogSettings, $"Executing query in time id: {query.TimeId}, order: {query.Order}", configurationServiceName, query.TimeId, query.Order);

            // If not using a result set execute the query as given.
            if (String.IsNullOrWhiteSpace(query.UseResultSet))
            {
                await logService.LogInformation(logger, LogScopes.RunBody, query.LogSettings, $"Query: {query.Query}", configurationServiceName, query.TimeId, query.Order);
                var dataTable = await databaseConnection.GetAsync(query.Query);
                return GetResultSetFromDataTable(dataTable);
            }

            var keyParts = query.UseResultSet.Split('.');
            var remainingKey = keyParts.Length > 1 ? query.UseResultSet.Substring(keyParts[0].Length + 1) : "";
            var tuple = ReplacementHelper.PrepareText(query.Query, (JObject)resultSets[keyParts[0]], remainingKey, insertValues: false);
            var queryString = tuple.Item1;
            var parameterKeys = tuple.Item2;
            var insertedParameters = tuple.Item3;

            await logService.LogInformation(logger, LogScopes.RunBody, query.LogSettings, $"Query: {queryString}", configurationServiceName, query.TimeId, query.Order);

            // Perform the query when there are no parameters. Either no values are used from the using result set or all values have been combined and a single query is sufficient.
            if (parameterKeys.Count == 0)
            {
                foreach (var parameter in insertedParameters)
                {
                    databaseConnection.AddParameter(parameter.Key, parameter.Value);
                }

                var dataTable = await databaseConnection.GetAsync(queryString);
                return GetResultSetFromDataTable(dataTable);
            }

            var jArray = new JArray();

            // Perform the query for each row in the result set that is being used.
            var usingResultSet = ResultSetHelper.GetCorrectObject<JArray>(query.UseResultSet, ReplacementHelper.EmptyRows, resultSets);
            var rows = new List<int> {0, 0};
            var keyWithSecondLayer = parameterKeys.FirstOrDefault(key => key.Contains("[j]"));
            for (var i = 0; i < usingResultSet.Count; i++)
            {
                rows[0] = i;
                var lastIQuery = i == usingResultSet.Count - 1;

                if (keyWithSecondLayer != null)
                {
                    var secondLayerArray = ResultSetHelper.GetCorrectObject<JArray>($"{query.UseResultSet}[i].{keyWithSecondLayer.Substring(0, keyWithSecondLayer.IndexOf("[j]"))}", rows, resultSets);

                    for (var j = 0; j < secondLayerArray.Count; j++)
                    {
                        rows[1] = j;
                        var lastJQuery = j == secondLayerArray.Count - 1;
                        jArray.Add(await ExecuteQueryWithParameters(databaseConnection, queryString, rows, usingResultSet, parameterKeys, insertedParameters, lastIQuery && lastJQuery));
                    }
                }
                else
                {
                    jArray.Add(await ExecuteQueryWithParameters(databaseConnection, queryString, rows, usingResultSet, parameterKeys, insertedParameters, lastIQuery));
                }
            }

            return new JObject
            {
                {"Results", jArray}
            };
        }

        private async Task<JObject> ExecuteQueryWithParameters(IDatabaseConnection databaseConnection, string queryString, List<int> rows, JArray usingResultSet, List<string> parameterKeys, List<KeyValuePair<string, string>> insertedParameters, bool lastQuery)
        {
            var parameters = new List<KeyValuePair<string, string>>(insertedParameters);

            foreach (var key in parameterKeys)
            {
                var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                var value = ReplacementHelper.GetValue(key, rows, (JObject)usingResultSet[rows[0]], false);
                parameters.Add(new KeyValuePair<string, string>(parameterName, value));
            }

            databaseConnection.ClearParameters();
            foreach (var parameter in parameters)
            {
                databaseConnection.AddParameter(parameter.Key, parameter.Value);
            }

            var dataTable = await databaseConnection.GetAsync(queryString, cleanUp: lastQuery);
            return GetResultSetFromDataTable(dataTable);
        }

        private JObject GetResultSetFromDataTable(DataTable dataTable)
        {
            var resultSet = new JObject();

            if (dataTable == null || dataTable.Rows.Count == 0)
            {
                return resultSet;
            }

            var jArray = new JArray();

            foreach (DataRow row in dataTable.Rows)
            {
                var jObject = new JObject();

                for (var i = 0; i < dataTable.Columns.Count; i++)
                {
                    var columnName = dataTable.Columns[i].ColumnName;
                    jObject.Add(columnName, row[i].ToString());
                }

                jArray.Add(jObject);
            }

            resultSet.Add("Results", jArray);

            return resultSet;
        }
    }
}
