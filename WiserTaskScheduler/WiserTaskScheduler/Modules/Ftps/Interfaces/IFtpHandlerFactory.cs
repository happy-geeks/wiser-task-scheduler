using WiserTaskScheduler.Modules.Ftps.Enums;

namespace WiserTaskScheduler.Modules.Ftps.Interfaces;

/// <summary>
/// A factory to create the correct handler for the FTP.
/// </summary>
public interface IFtpHandlerFactory
{
    /// <summary>
    /// Gets the correct handler for the FTP.
    /// </summary>
    /// <param name="ftpType">The FTP type to create.</param>
    /// <returns></returns>
    IFtpHandler GetFtpHandler(FtpTypes ftpType);
}