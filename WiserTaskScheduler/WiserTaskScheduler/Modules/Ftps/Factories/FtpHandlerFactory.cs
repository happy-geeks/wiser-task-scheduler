using System;
using WiserTaskScheduler.Modules.Ftps.Enums;
using WiserTaskScheduler.Modules.Ftps.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Services;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace WiserTaskScheduler.Modules.Ftps.Factories;

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
                return serviceProvider.GetRequiredService<SftpHandler>();
            default:
                throw new NotImplementedException($"FTP type '{ftpType}' is not yet implemented.");
        }
    }
}