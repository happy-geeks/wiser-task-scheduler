using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.GclConverters.Interfaces;
using GeeksCoreLibrary.Modules.GclConverters.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using PdfSharp.Pdf;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Interfaces;
using WiserTaskScheduler.Modules.GenerateFiles.Interfaces;
using WiserTaskScheduler.Modules.GenerateFiles.Models;

namespace WiserTaskScheduler.Modules.GenerateFiles.Services;

/// <summary>
/// A service for a generate file action.
/// </summary>
public class GenerateFileService(IBodyService bodyService, ILogService logService, ILogger<GenerateFileService> logger, IServiceProvider serviceProvider) : IGenerateFileService, IActionsService, IScopedService
{
    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
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

        if (rows == null)
        {
            throw new ResultSetException($"Failed to find an array at key '{generateFile.UseResultSet}' in result sets to loop over for generating multiple files.");
        }

        var indexRows = new List<int> { 0 };
        for (var i = 0; i < rows.Count; i++)
        {
            indexRows[0] = i;
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
    /// <param name="configurationServiceName">The name of the configuration that is being executed.</param>
    /// <param name="useResultSet">The path/key to the result set that needs to be used.</param>
    /// <param name="forcedIndex">The index a body part uses if it is set to use the forced index.</param>
    /// <returns></returns>
    private async Task<JObject> GenerateFile(GenerateFileModel generateFile, List<int> rows, JObject resultSets, string configurationServiceName, string useResultSet, int forcedIndex = -1)
    {
        var fileLocation = generateFile.FileLocation;
        var fileName = generateFile.FileName;
        var itemId = generateFile.ItemId;
        var itemLinkId = generateFile.ItemLinkId;
        var propertyName = generateFile.PropertyName;
        var pdfsToMerge = new Dictionary<string, List<string>>();

        // Replace the file location and name if a result set is used.
        if (!String.IsNullOrWhiteSpace(useResultSet))
        {
            var keyParts = useResultSet.Split('.');
            var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, ReplacementHelper.EmptyRows, resultSets);
            var remainingKey = keyParts.Length > 1 ? useResultSet[(keyParts[0].Length + 1)..] : "";

            var fileLocationTuple = ReplacementHelper.PrepareText(fileLocation, usingResultSet, remainingKey, generateFile.HashSettings);
            var fileNameTuple = ReplacementHelper.PrepareText(fileName, usingResultSet, remainingKey, generateFile.HashSettings);
            var itemIdTuple = ReplacementHelper.PrepareText(itemId, usingResultSet, remainingKey, generateFile.HashSettings);
            var itemLinkIdTuple = ReplacementHelper.PrepareText(itemLinkId, usingResultSet, remainingKey, generateFile.HashSettings);
            var propertyNameTuple = ReplacementHelper.PrepareText(propertyName, usingResultSet, remainingKey, generateFile.HashSettings);

            fileLocation = ReplacementHelper.ReplaceText(fileLocationTuple.Item1, rows, fileLocationTuple.Item2, usingResultSet, generateFile.HashSettings);
            fileName = ReplacementHelper.ReplaceText(fileNameTuple.Item1, rows, fileNameTuple.Item2, usingResultSet, generateFile.HashSettings);
            itemId =  ReplacementHelper.ReplaceText(itemIdTuple.Item1, rows, itemIdTuple.Item2, usingResultSet, generateFile.HashSettings);
            itemLinkId =  ReplacementHelper.ReplaceText(itemLinkIdTuple.Item1, rows, itemLinkIdTuple.Item2, usingResultSet, generateFile.HashSettings);
            propertyName =  ReplacementHelper.ReplaceText(propertyNameTuple.Item1, rows, propertyNameTuple.Item2, usingResultSet, generateFile.HashSettings);

            // Build dictionary of PDFs to merge, replace variables in itemId and propertyName
            if (generateFile.Body.MergePdfs != null)
            {
                foreach (var pdf in generateFile.Body.MergePdfs)
                {
                    var pdfWiserItemIdTuple = ReplacementHelper.PrepareText(pdf.WiserItemId, usingResultSet, remainingKey, generateFile.HashSettings);
                    var pdfItemId = ReplacementHelper.ReplaceText(pdfWiserItemIdTuple.Item1, rows, pdfWiserItemIdTuple.Item2, usingResultSet, generateFile.HashSettings);

                    if (String.IsNullOrEmpty(pdfItemId))
                    {
                        continue;
                    }

                    var pdfWiserPropertyNameTuple = ReplacementHelper.PrepareText(pdf.PropertyName, usingResultSet, remainingKey, generateFile.HashSettings);
                    var pdfPropertyName = ReplacementHelper.ReplaceText(pdfWiserPropertyNameTuple.Item1, rows, pdfWiserPropertyNameTuple.Item2, usingResultSet, generateFile.HashSettings);

                    if (String.IsNullOrEmpty(pdfPropertyName))
                    {
                        continue;
                    }

                    if (pdfsToMerge.ContainsKey(pdfItemId))
                    {
                        pdfsToMerge[pdfItemId].Add(pdfPropertyName);
                    }
                    else
                    {
                        pdfsToMerge.Add(pdfItemId, [pdfPropertyName]);
                    }
                }
            }
        }

        var logLocation = !String.IsNullOrEmpty(fileLocation) ? $"'{fileLocation}'" : $"wiser_itemfile property '{propertyName}', item_id '{itemId}', itemlink_id '{itemLinkId}'";
        await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateFile.LogSettings, $"Generating file '{fileName}' at {logLocation}.", configurationServiceName, generateFile.TimeId, generateFile.Order);
        var body = bodyService.GenerateBody(generateFile.Body, rows, resultSets, generateFile.HashSettings, forcedIndex);
        FileContentResult pdfFile = null;

        // Generate PDF if needed
        if (generateFile.Body.GeneratePdf)
        {
            using var scope = serviceProvider.CreateScope();
            var htmlToPdfConverterService = scope.ServiceProvider.GetRequiredService<IHtmlToPdfConverterService>();
            var pdfSettings = new HtmlToPdfRequestModel
            {
                Html = body
            };
            pdfFile = await htmlToPdfConverterService.ConvertHtmlStringToPdfAsync(pdfSettings);

            // Merge PDFs to generated PDF
            if (pdfsToMerge.Count > 0)
            {
                using var stream = new MemoryStream(pdfFile.FileContents);
                using var mergeResultPdfDocument = new PdfDocument(stream);

                // Get WiserItemsService. It's needed to get the template.
                var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();

                var documentsAdded = false;
                foreach (var pdf in pdfsToMerge)
                {
                    foreach (var propertyNameToReplace in pdf.Value)
                    {
                        var wiserItemFile = await wiserItemsService.GetItemFileAsync(Convert.ToUInt64(pdf.Key), "item_id", propertyNameToReplace);
                        if (wiserItemFile == null)
                        {
                            continue;
                        }

                        using var extraStream = new MemoryStream(wiserItemFile.Content);
                        using var extraDocument = new PdfDocument(extraStream);
                        foreach (var page in extraDocument.Pages)
                        {
                            mergeResultPdfDocument.AddPage(page);
                        }

                        documentsAdded = true;
                    }
                }

                if (documentsAdded)
                {
                    using var saveStream = new MemoryStream();
                    mergeResultPdfDocument.Save(saveStream);
                    pdfFile.FileContents = saveStream.ToArray();
                }
            }
        }

        var fileGenerated = false;
        try
        {
            if (!String.IsNullOrEmpty(fileLocation)) // Save to file location on disk
            {
                Directory.CreateDirectory(fileLocation);
                if (pdfFile == null)
                {
                    await File.WriteAllTextAsync(Path.Combine(fileLocation, fileName), body);
                }
                else
                {
                    await File.WriteAllBytesAsync(Path.Combine(fileLocation, fileName), pdfFile.FileContents);
                }
            }
            else // Save to wiser_itemfile table in database
            {
                var itemFile = new WiserItemFileModel
                {
                    ItemId = Convert.ToUInt64(String.IsNullOrEmpty(itemId) ? "0" : itemId),
                    ItemLinkId = Convert.ToUInt64(String.IsNullOrEmpty(itemLinkId) ? "0" : itemLinkId),
                    PropertyName = propertyName,
                    FileName = fileName,
                    Content = (pdfFile == null) ? Encoding.UTF8.GetBytes(body) : pdfFile.FileContents,
                    ContentType = generateFile.Body.ContentType,
                    AddedOn = DateTime.UtcNow,
                    AddedBy = "WiserTaskScheduler"
                };

                using var scope = serviceProvider.CreateScope();
                var wiserItemsService = scope.ServiceProvider.GetRequiredService<IWiserItemsService>();
                await wiserItemsService.AddItemFileAsync(itemFile, "WiserTaskScheduler", skipPermissionsCheck: true);
            }

            fileGenerated = true;
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, generateFile.LogSettings, $"File '{fileName}' generated at {logLocation}.", configurationServiceName, generateFile.TimeId, generateFile.Order);
        }
        catch (Exception e)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, generateFile.LogSettings, $"Failed to generate file '{fileName}' at {logLocation}.\n{e}", configurationServiceName, generateFile.TimeId, generateFile.Order);
        }

        return new JObject
        {
            {"FileName", fileName},
            {"FileLocation", fileLocation},
            {"Success", fileGenerated},
            {"ItemId", itemId},
            {"ItemLinkId", itemLinkId},
            {"PropertyName", propertyName}
        };
    }
}