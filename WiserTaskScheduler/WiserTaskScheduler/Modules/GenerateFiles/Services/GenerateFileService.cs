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
using WiserTaskScheduler.Modules.Body.Interfaces;
using WiserTaskScheduler.Modules.GenerateFiles.Interfaces;
using WiserTaskScheduler.Modules.GenerateFiles.Models;

namespace WiserTaskScheduler.Modules.GenerateFiles.Services
{
    /// <summary>
    /// A service for a generate file action.
    /// </summary>
    public class GenerateFileService : IGenerateFileService, IActionsService, IScopedService
    {
        private readonly IBodyService bodyService;
        private readonly ILogService logService;
        private readonly ILogger<GenerateFileService> logger;

        /// <summary>
        /// Create a new instance of <see cref="GenerateFileService"/>.
        /// </summary>
        /// <param name="bodyService"></param>
        /// <param name="logService"></param>
        /// <param name="logger"></param>
        public GenerateFileService(IBodyService bodyService, ILogService logService, ILogger<GenerateFileService> logger)
        {
            this.bodyService = bodyService;
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
            var generateFile = (GenerateFileModel) action;

            if (generateFile.SingleFile)
            {
                return await GenerateFile(generateFile, ReplacementHelper.EmptyRows, resultSets, configurationServiceName, generateFile.UseResultSet);
            }
            
            var jArray = new JArray();
            
            if (String.IsNullOrWhiteSpace(generateFile.UseResultSet))
            {
                await logService.LogError(logger, LogScopes.StartAndStop, generateFile.LogSettings, $"The generate file in configuration '{configurationServiceName}', time ID '{generateFile.TimeId}', order '{generateFile.Order}' is set to not be a single file but no result set has been provided. If the information is not dynamic set action to single file, otherwise provide a result set to use.", configurationServiceName, generateFile.TimeId, generateFile.Order);
                
                return new JObject
                {
                    {"Results", jArray}
                };
            }

            var rows = ResultSetHelper.GetCorrectObject<JArray>(generateFile.UseResultSet, ReplacementHelper.EmptyRows, resultSets);

            for (var i = 0; i < rows.Count; i++)
            {
                var indexRows = new List<int> { i };
                jArray.Add(await GenerateFile(generateFile, indexRows, resultSets, configurationServiceName, $"{generateFile.UseResultSet}[{i}]", i));
            }

            return new JObject
            {
                {"Results", jArray}
            };
        }

        /// <summary>
        /// Generate a file.
        /// </summary>
        /// <param name="generateFile">The information for the file to be generated.</param>
        /// <param name="rows">The indexes/rows of the array, passed to be used if '[i]' is used in the key.</param>
        /// <param name="resultSets">The result sets from previous actions in the same run.</param>
        /// <returns></returns>
        private async Task<JObject> GenerateFile(GenerateFileModel generateFile, List<int> rows, JObject resultSets, string configurationServiceName, string useResultSet, int forcedIndex = -1)
        {
            var fileLocation = generateFile.FileLocation;
            var fileName = generateFile.FileName;

            // Replace the file location and name if a result set is used.
            if (!String.IsNullOrWhiteSpace(useResultSet))
            {
                var keyParts = useResultSet.Split('.');
                var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, ReplacementHelper.EmptyRows, resultSets);
                var remainingKey = keyParts.Length > 1 ? useResultSet.Substring(keyParts[0].Length + 1) : "";

                var fileLocationTuple = ReplacementHelper.PrepareText(fileLocation, usingResultSet, remainingKey, generateFile.HashSettings);
                var fileNameTuple = ReplacementHelper.PrepareText(fileName, usingResultSet, remainingKey, generateFile.HashSettings);

                fileLocation = ReplacementHelper.ReplaceText(fileLocationTuple.Item1, rows, fileLocationTuple.Item2, usingResultSet, generateFile.HashSettings);
                fileName = ReplacementHelper.ReplaceText(fileNameTuple.Item1, rows, fileNameTuple.Item2, usingResultSet, generateFile.HashSettings);
            }

            await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateFile.LogSettings, $"Generating file '{fileName}' at '{fileLocation}'.", configurationServiceName, generateFile.TimeId, generateFile.Order);

            var body = bodyService.GenerateBody(generateFile.Body, rows, resultSets, generateFile.HashSettings, forcedIndex);

            var fileGenerated = false;
            try
            {
                Directory.CreateDirectory(fileLocation);
                await File.WriteAllTextAsync(Path.Combine(fileLocation, fileName), body);
                fileGenerated = true;
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateFile.LogSettings, $"File '{fileName}' generated at '{fileLocation}'.", configurationServiceName, generateFile.TimeId, generateFile.Order);
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, generateFile.LogSettings, $"Failed to generate file '{fileName}' at '{fileLocation}'.\n{e}", configurationServiceName, generateFile.TimeId, generateFile.Order);
            }
            
            return new JObject()
            {
                {"FileName", fileName},
                {"FileLocation", fileLocation},
                {"Success", fileGenerated}
            };
        }
    }
}
