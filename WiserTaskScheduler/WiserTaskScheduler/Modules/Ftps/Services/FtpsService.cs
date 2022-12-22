using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Enums;
using WiserTaskScheduler.Modules.Ftps.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace WiserTaskScheduler.Modules.Ftps.Services;

public class FtpsService : IFtpsService, IActionsService, IScopedService
{
    private readonly IBodyService bodyService;
    private readonly IFtpHandlerFactory ftpHandlerFactory;
    private readonly ILogService logService;
    private readonly ILogger<FtpsService> logger;

    public FtpsService(IBodyService bodyService, IFtpHandlerFactory ftpHandlerFactory, ILogService logService, ILogger<FtpsService> logger)
    {
        this.bodyService = bodyService;
        this.ftpHandlerFactory = ftpHandlerFactory;
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
        var ftpAction = (FtpModel) action;
        var ftpHandler = ftpHandlerFactory.GetFtpHandler(ftpAction.Type);

        await ftpHandler.OpenConnectionAsync(ftpAction);

        var fromPath = ftpAction.From;
        var toPath = ftpAction.To;

        var result = new JObject()
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
                        var body = bodyService.GenerateBody(ftpAction.Body, ReplacementHelper.EmptyRows, resultSets);
                        success = await ftpHandler.UploadAsync(ftpAction, toPath, Encoding.UTF8.GetBytes(body));
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
                        success = await ftpHandler.UploadAsync(ftpAction, toPath, fromPath);
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
                    var success = await ftpHandler.DownloadAsync(ftpAction, fromPath, toPath);
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
                            await ftpHandler.DeleteFileAsync(ftpAction, fromPath);
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
                    var filesOnServer = await ftpHandler.GetFilesInFolderAsync(ftpAction, fromPath);
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
                    await ftpHandler.DeleteFileAsync(ftpAction, fromPath);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to delete file{(ftpAction.AllFilesInFolder ? "s" : "")} from '{fromPath}' due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                    result.Add("Success", false);
                }

                break;
            default:
                throw new NotImplementedException($"FTP action '{ftpAction.Action}' is not yet implemented.");
        }

        await ftpHandler.CloseConnectionAsync();

        return result;
    }
}