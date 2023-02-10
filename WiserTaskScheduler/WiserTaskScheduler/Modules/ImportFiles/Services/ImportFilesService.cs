using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.ImportFiles.Interfaces;
using WiserTaskScheduler.Modules.ImportFiles.Models;

namespace WiserTaskScheduler.Modules.ImportFiles.Services
{
    public class ImportFilesService : IImportFilesService, IActionsService, IScopedService
    {
        private readonly ILogService logService;
        private readonly ILogger<ImportFilesService> logger;

        public ImportFilesService(ILogService logService, ILogger<ImportFilesService> logger)
        {
            this.logService = logService;
            this.logger = logger;
        }

        /// <inheritdoc />
        public Task InitializeAsync(ConfigurationModel configuration)
        {
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
        {
            var importFile = (ImportFileModel) action;

            if (importFile.Separator.Equals("\\t"))
            {
                importFile.Separator = '\t'.ToString();
            }

            if (importFile.SingleFile)
            {
                return await ImportFile(importFile, ReplacementHelper.EmptyRows, resultSets, configurationServiceName, importFile.UseResultSet);
            }
            
            var jArray = new JArray();

            if (String.IsNullOrWhiteSpace(importFile.UseResultSet))
            {
                await logService.LogError(logger, LogScopes.StartAndStop, importFile.LogSettings, $"The import in configuration '{configurationServiceName}', time ID '{importFile.TimeId}', order '{importFile.Order}' is set to not be a single file but no result set has been provided. If the information is not dynamic set action to single file, otherwise provide a result set to use.", configurationServiceName, importFile.TimeId, importFile.Order);
                
                return new JObject
                {
                    {"Results", jArray}
                };
            }

            var rows = ResultSetHelper.GetCorrectObject<JArray>(importFile.UseResultSet, ReplacementHelper.EmptyRows, resultSets);

            for (var i = 0; i < rows.Count; i++)
            {
                var indexRows = new List<int> { i };
                jArray.Add(await ImportFile(importFile, indexRows, resultSets, configurationServiceName, $"{importFile.UseResultSet}[{i}]"));
            }

            return new JObject
            {
                {"Results", jArray}
            };
        }

        /// <summary>
        /// Import a file.
        /// </summary>
        /// <param name="importFile">The information for the file to be imported.</param>
        /// <param name="rows">The indexes/rows of the array, passed to be used if '[i]' is used in the key.</param>
        /// <param name="resultSets">The result sets from previous actions in the same run.</param>
        /// <param name="configurationServiceName">The name of the configuration that contains this action.</param>
        /// <param name="useResultSet">The name of the result set to use, either the value as defined or added with an index.</param>
        /// <returns></returns>
        private async Task<JObject> ImportFile(ImportFileModel importFile, List<int> rows, JObject resultSets, string configurationServiceName, string useResultSet)
        {
            var filePath = importFile.FilePath;
            
            // Replace the file path if a result set is used.
            if (!String.IsNullOrWhiteSpace(useResultSet))
            {
                var keyParts = useResultSet.Split('.');
                var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, ReplacementHelper.EmptyRows, resultSets);
                var remainingKey = keyParts.Length > 1 ? useResultSet.Substring(keyParts[0].Length + 1) : "";

                var tuple = ReplacementHelper.PrepareText(filePath, usingResultSet, remainingKey, importFile.HashSettings);

                filePath = ReplacementHelper.ReplaceText(tuple.Item1, rows, tuple.Item2, usingResultSet, importFile.HashSettings);
            }

            if (!File.Exists(filePath))
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, importFile.LogSettings, $"Failed to import file '{importFile.FilePath}'. Path does not exists.", configurationServiceName, importFile.TimeId, importFile.Order);

                return new JObject()
                {
                    {"Success", false}
                };
            }

            try
            {
                await logService.LogInformation(logger, LogScopes.RunBody, importFile.LogSettings, $"Importing file {filePath}.", configurationServiceName, importFile.TimeId, importFile.Order);
                var lines = await File.ReadAllLinesAsync(filePath);
                var jArray = new JArray();

                if (!importFile.HasFieldNames)
                {
                    var firstColumnLenght = -1;

                    for (var i = 0; i < lines.Length; i++)
                    {
                        if (String.IsNullOrWhiteSpace(lines[i]))
                        {
                            await logService.LogWarning(logger, LogScopes.RunBody, importFile.LogSettings, $"Did not import line {i} due to empty row in file {filePath}", configurationServiceName, importFile.TimeId, importFile.Order);
                            continue;
                        }

                        var columns = new JArray(lines[i].Split(importFile.Separator));

                        // Use the first row to determine the number of columns to be expected in each row.
                        if (firstColumnLenght == -1)
                        {
                            firstColumnLenght = columns.Count;
                        }
                        else if(columns.Count != firstColumnLenght)
                        {
                            await logService.LogWarning(logger, LogScopes.RunBody, importFile.LogSettings, $"Did not import line {i} due to missing columns in file {filePath}", configurationServiceName, importFile.TimeId, importFile.Order);
                            continue;
                        }

                        var row = new JObject()
                        {
                            {"Columns", columns }
                        };
                        jArray.Add(row);
                    }

                    return new JObject()
                    {
                        {"Success", true},
                        {"Results", jArray}
                    };
                }
                
                var fieldNames = lines[0].Split(importFile.Separator);

                for (var i = 1; i < lines.Length; i++)
                {
                    var row = new JObject();

                    if (String.IsNullOrWhiteSpace(lines[i]))
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, importFile.LogSettings, $"Did not import line {i} due to empty row in file {filePath}", configurationServiceName, importFile.TimeId, importFile.Order);
                        continue;
                    }

                    var columns = lines[i].Split(importFile.Separator);

                    if (columns.Length != fieldNames.Length)
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, importFile.LogSettings, $"Did not import line {i} due to missing columns in file {filePath}", configurationServiceName, importFile.TimeId, importFile.Order);
                        continue;
                    }

                    for (var j = 0; j < fieldNames.Length; j++)
                    {
                        row.Add(fieldNames[j], columns[j]);
                    }

                    jArray.Add(row);
                }

                var fieldArray = new JArray();
                foreach (var fieldName in fieldNames)
                {
                    fieldArray.Add(fieldName);
                }
                
                return new JObject()
                {
                    {"Success", true},
                    {"Fields",  fieldArray},
                    {"Results", jArray}
                };
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, importFile.LogSettings, $"Failed to import file '{importFile.FilePath}'.\n{e}", configurationServiceName, importFile.TimeId, importFile.Order);

                return new JObject()
                {
                    {"Success", false}
                };
            }
        }
    }
}
