using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Helpers;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Body.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Enums;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.Ftps.Services;

public class FtpsService : IFtpsService, IActionsService, IScopedService
{
    private readonly IBodyService bodyService;

    public FtpsService(IBodyService bodyService)
    {
        this.bodyService = bodyService;
    }
    
    /// <inheritdoc />
    public async Task Initialize(ConfigurationModel configuration)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var ftpAction = (FtpModel) action;
        IFtpHandler ftpHandler;

        switch (ftpAction.Type)
        {
            case FtpTypes.Ftps:
                ftpHandler = new FtpsHandler();
                break;
            case FtpTypes.Sftp:
                //break;
            default:
                throw new NotImplementedException($"FTP type '{ftpAction.Type}' is not yet implemented.");
        }

        await ftpHandler.OpenConnectionAsync(ftpAction);

        var fromPath = ftpAction.From;
        var toPath = ftpAction.To;
        byte[] fileBytes;
        
        switch (ftpAction.Action)
        {
            case FtpActionTypes.Upload:
                bool fileUploaded;
                
                if (String.IsNullOrWhiteSpace(ftpAction.From))
                {
                    // TODO use Body.

                    var body = bodyService.GenerateBody(ftpAction.Body, ReplacementHelper.EmptyRows, resultSets);
                    fileUploaded = await ftpHandler.UploadAsync(ftpAction, toPath, Encoding.UTF8.GetBytes(body));
                }
                else
                {
                    fileUploaded = await ftpHandler.UploadAsync(ftpAction, toPath, fromPath);
                }
                
                break;
            case FtpActionTypes.Download:
                fileBytes = await ftpHandler.DownloadAsync(ftpAction, fromPath);
                await File.WriteAllBytesAsync(toPath, fileBytes);
                break;
            default:
                throw new NotImplementedException($"FTP action '{ftpAction.Action}' is not yet implemented.");
        }

        await ftpHandler.CloseConnectionAsync();

        return null;
    }
}