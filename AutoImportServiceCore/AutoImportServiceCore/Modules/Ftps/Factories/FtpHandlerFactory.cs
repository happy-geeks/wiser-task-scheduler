using System;
using AutoImportServiceCore.Modules.Ftps.Enums;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Services;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace AutoImportServiceCore.Modules.Ftps.Factories;

public class FtpHandlerFactory : IFtpHandlerFactory, IScopedService
{
    private readonly IServiceProvider serviceProvider;

    public FtpHandlerFactory(IServiceProvider serviceProvider)
    {
        this.serviceProvider = serviceProvider;
    }
    
    public IFtpHandler GetFtpHandler(FtpTypes ftpType)
    {
        switch (ftpType)
        {
            case FtpTypes.Ftps:
                return serviceProvider.GetRequiredService<FtpsHandler>();
            case FtpTypes.Sftp:
            //break;
            default:
                throw new NotImplementedException($"FTP type '{ftpType}' is not yet implemented.");
        }
    }
}