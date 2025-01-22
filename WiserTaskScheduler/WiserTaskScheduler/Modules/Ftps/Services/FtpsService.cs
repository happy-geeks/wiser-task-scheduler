using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Modules.Ftps.Interfaces;
using GeeksCoreLibrary.Modules.Ftps.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Enums;
using WiserTaskScheduler.Modules.Ftps.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Models;

namespace WiserTaskScheduler.Modules.Ftps.Services;

public class FtpsService(IBodyService bodyService, IFtpHandlerFactory ftpHandlerFactory, ILogService logService, ILogger<FtpsService> logger) : IFtpsService, IActionsService, IScopedService
{
    /// <inheritdoc />
    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var ftpAction = (FtpModel) action;
        var ftpHandler = ftpHandlerFactory.GetFtpHandler(ftpAction.Type);

        var ftpSettings = new FtpSettings
        {
            User = ftpAction.User,
            Password = ftpAction.Password,
            Host = ftpAction.Host,
            Port = ftpAction.Port,
            EncryptionMode = ftpAction.EncryptionMode,
            UsePassive = ftpAction.UsePassive,
            SshPrivateKeyPassphrase = ftpAction.SshPrivateKeyPassphrase,
            SshPrivateKeyPath = ftpAction.SshPrivateKeyPath
        };

        await ftpHandler.OpenConnectionAsync(ftpSettings);

        try
        {
            if (ftpAction.SingleAction)
            {
                return await ExecuteFtpAction(ftpAction, ftpHandler, resultSets, ReplacementHelper.EmptyRows, ftpAction.UseResultSet, configurationServiceName);
            }
            else
            {
                var rows = ResultSetHelper.GetCorrectObject<JArray>(ftpAction.UseResultSet, ReplacementHelper.EmptyRows, resultSets);
                var jArray = new JArray();

                // Execute the action for each result in the stated result set.
                for (var i = 0; i < rows.Count; i++)
                {
                    var indexRows = new List<int> {i};
                    jArray.Add(await ExecuteFtpAction(ftpAction, ftpHandler, resultSets, indexRows, $"{ftpAction.UseResultSet}[{i}]", configurationServiceName));
                }

                return new JObject
                {
                    {"Results", jArray}
                };
            }
        }
        finally
        {
            await ftpHandler.CloseConnectionAsync();
        }
    }

    /// <summary>
    /// Execute an FTP action based on the information.
    /// </summary>
    /// <param name="ftpAction">The information for the FTP action.</param>
    /// <param name="ftpHandler">The handler to be used for the correct protocol of FTP.</param>
    /// <param name="resultSets">The result sets from previous actions in the same run.</param>
    /// <param name="rows">The indexes/rows of the array, passed to be used if '[i]' is used in the key.</param>
    /// <param name="useResultSet">The key for the result set. Will be modified for index when not a single action.</param>
    /// <param name="configurationServiceName">The name of the service in the configuration, used for logging.</param>
    /// <returns></returns>
    private async Task<JObject> ExecuteFtpAction(FtpModel ftpAction, IFtpHandler ftpHandler, JObject resultSets, List<int> rows, string useResultSet, string configurationServiceName)
    {
        var fromPath = ftpAction.From;
        var toPath = ftpAction.To;

        // Replace the from and to paths if a result set is used.
        if (!String.IsNullOrWhiteSpace(useResultSet))
        {
            var keyParts = useResultSet.Split('.');
            var usingResultSet = ResultSetHelper.GetCorrectObject<JObject>(useResultSet, ReplacementHelper.EmptyRows, resultSets);
            var remainingKey = keyParts.Length > 1 ? useResultSet[(keyParts[0].Length + 1)..] : "";

            var fromPathTuple = ReplacementHelper.PrepareText(fromPath, usingResultSet, remainingKey, ftpAction.HashSettings);
            var toPathTuple = ReplacementHelper.PrepareText(toPath, usingResultSet, remainingKey, ftpAction.HashSettings);

            fromPath = ReplacementHelper.ReplaceText(fromPathTuple.Item1, rows, fromPathTuple.Item2, usingResultSet, ftpAction.HashSettings);
            toPath = ReplacementHelper.ReplaceText(toPathTuple.Item1, rows, toPathTuple.Item2, usingResultSet, ftpAction.HashSettings);
        }

        var result = new JObject
        {
            {"FromPath", fromPath},
            {"ToPath", toPath},
            {"Action", ftpAction.Action.ToString()}
        };

        switch (ftpAction.Action)
        {
            case FtpActionTypes.Upload:
                try
                {
                    bool success;

                    // If there is no from path a file will be generated from the body and uploaded to the server.
                    if (String.IsNullOrWhiteSpace(ftpAction.From))
                    {
                        var body = bodyService.GenerateBody(ftpAction.Body, ReplacementHelper.EmptyRows, resultSets, ftpAction.HashSettings);
                        success = await ftpHandler.UploadAsync(toPath, Encoding.UTF8.GetBytes(body));
                        result.Add("Success", success);

                        if (success)
                        {
                            await logService.LogInformation(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Upload of generated files to '{toPath}' was successful.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                        }
                        else
                        {
                            await logService.LogWarning(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Upload of generated files to '{toPath}' was not successful.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                        }
                    }
                    else
                    {
                        success = await ftpHandler.UploadAsync(ftpAction.AllFilesInFolder, toPath, fromPath);
                        result.Add("Success", success);

                        if (success)
                        {
                            await logService.LogInformation(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Upload of file(s) from '{fromPath}' to '{toPath}' was successful.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                        }
                        else
                        {
                            await logService.LogWarning(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Upload of file(s) from '{fromPath}' to '{toPath}' was not successful.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                        }
                    }

                    // Only delete the file(s) if a success state was given.
                    if (success && ftpAction.DeleteFileAfterAction)
                    {
                        try
                        {
                            if (!ftpAction.AllFilesInFolder)
                            {
                                await logService.LogInformation(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Deleting file '{fromPath}' after successful upload", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                                File.Delete(fromPath);
                            }
                            else
                            {
                                var files = Directory.GetFiles(fromPath);
                                foreach (var file in files)
                                {
                                    await logService.LogInformation(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Deleting file '{fromPath}' after successful folder upload.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                                    File.Delete(Path.Combine(fromPath, file));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to delete file(s) after action was successful upload due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                        }
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to upload file(s) from '{fromPath}' to '{toPath} due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    result.Add("Success", false);
                }

                break;
            case FtpActionTypes.Download:
                try
                {
                    var success = await ftpHandler.DownloadAsync(ftpAction.AllFilesInFolder, fromPath, toPath);
                    result.Add("Success", success);

                    if (success)
                    {
                        await logService.LogInformation(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Download of file(s) from '{fromPath}' to '{toPath}' was successful.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    }
                    else
                    {
                        await logService.LogWarning(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Download of file(s) from '{fromPath}' to '{toPath}' was not successful.", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    }

                    // Only delete the file(s) if a success state was given.
                    if (success && ftpAction.DeleteFileAfterAction)
                    {
                        try
                        {
                            await logService.LogInformation(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Deleting file(s) '{fromPath}' after successful download", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                            await ftpHandler.DeleteFileAsync(ftpAction.AllFilesInFolder, fromPath);
                        }
                        catch (Exception e)
                        {
                            await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to delete file(s) after action was successful download due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                        }
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to download file(s) from '{fromPath}' to '{toPath} due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    result.Add("Success", false);
                }

                break;
            case FtpActionTypes.FilesInDirectory:
                try
                {
                    var filesOnServer = await ftpHandler.GetFilesInFolderAsync(fromPath);
                    result.Add("FilesInDirectory", new JArray(filesOnServer));
                    result.Add("FilesInDirectoryCount", filesOnServer.Count);
                    result.Add("Success", true);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to get file listing from '{fromPath}' due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    result.Add("Success", false);
                }

                break;
            case FtpActionTypes.Delete:
                try
                {
                    await ftpHandler.DeleteFileAsync(ftpAction.AllFilesInFolder, fromPath);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to delete file{(ftpAction.AllFilesInFolder ? "s" : "")} from '{fromPath}' due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    result.Add("Success", false);
                }

                break;
            case FtpActionTypes.Move:
            case FtpActionTypes.Rename:
                try
                {
                    var success = await ftpHandler.MoveFileAsync(fromPath, toPath);
                    result.Add("Success", success);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to move file from '{fromPath}' to '{toPath}' due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    result.Add("Success", false);
                }
                break;
            default:
                throw new NotImplementedException($"FTP action '{ftpAction.Action}' is not yet implemented.");
        }

        return result;
    }
}