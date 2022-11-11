using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Helpers;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Body.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Enums;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.Ftps.Services;

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
    public async Task Initialize(ConfigurationModel configuration)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var ftpAction = (FtpModel) action;
        var ftpHandler = ftpHandlerFactory.GetFtpHandler(ftpAction.Type);

        await ftpHandler.OpenConnectionAsync(ftpAction);

        var fromPath = ftpAction.From;
        var toPath = ftpAction.To;

        switch (ftpAction.Action)
        {
            case FtpActionTypes.Upload:
                try
                {
                    if (String.IsNullOrWhiteSpace(ftpAction.From))
                    {
                        var body = bodyService.GenerateBody(ftpAction.Body, ReplacementHelper.EmptyRows, resultSets);
                        await ftpHandler.UploadAsync(ftpAction, toPath, Encoding.UTF8.GetBytes(body));
                    }
                    else
                    {
                        await ftpHandler.UploadAsync(ftpAction, toPath, fromPath);
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to upload file from '{fromPath}' to '{toPath} due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                }

                break;
            case FtpActionTypes.Download:
                try
                {
                    var fileBytes = await ftpHandler.DownloadAsync(ftpAction, fromPath);
                    if (fileBytes != null)
                    {
                        await File.WriteAllBytesAsync(toPath, fileBytes);
                    }
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to download file from '{fromPath}' to '{toPath} due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
                }
                
                break;
            case FtpActionTypes.FilesInDirectory:
                try
                {
                    var filesInDirectory = await ftpHandler.GetFilesInFolderAsync(ftpAction, fromPath);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunBody, ftpAction.LogSettings, $"Failed to get file listing from '{fromPath}' due to exception: {e}", configurationServiceName, ftpAction.TimeId, ftpAction.Order);
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
                }

                break;
            default:
                throw new NotImplementedException($"FTP action '{ftpAction.Action}' is not yet implemented.");
        }

        await ftpHandler.CloseConnectionAsync();

        return null;
    }
}