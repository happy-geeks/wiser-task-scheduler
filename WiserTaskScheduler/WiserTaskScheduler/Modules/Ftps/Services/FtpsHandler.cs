using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using WiserTaskScheduler.Modules.Ftps.Extensions;
using WiserTaskScheduler.Modules.Ftps.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Models;
using FluentFTP;
using FluentFTP.Helpers;

namespace WiserTaskScheduler.Modules.Ftps.Services;

public class FtpsHandler : IFtpHandler
{
    private FtpClient client;

    /// <inheritdoc />
    public async Task OpenConnectionAsync(FtpModel ftpAction)
    {
        client = new FtpClient(ftpAction.Host, ftpAction.Port, ftpAction.User, ftpAction.Password);
        client.EncryptionMode = ftpAction.EncryptionMode.ConvertToFtpsEncryptionMode();
        client.ValidateAnyCertificate = ftpAction.Host.StartsWith("localhost", StringComparison.OrdinalIgnoreCase);
        client.DataConnectionType = ftpAction.UsePassive ? FtpDataConnectionType.AutoPassive : FtpDataConnectionType.AutoActive;

        await client.ConnectAsync();
    }

    /// <inheritdoc />
    public Task CloseConnectionAsync()
    {
        client.Dispose();
        return Task.CompletedTask;
    }

    public async Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, string fromPath)
    {
        if (!ftpAction.AllFilesInFolder)
        {
            return (await client.UploadAsync(await File.ReadAllBytesAsync(fromPath), uploadPath, createRemoteDir: true)).IsSuccess();
        }
        
        var ftpResults = await client.UploadDirectoryAsync(fromPath, uploadPath, existsMode: FtpRemoteExists.Overwrite);
        return ftpResults.Any(ftpResult => ftpResult.IsSuccess);
    }

    /// <inheritdoc />
    public async Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, byte[] fileBytes)
    {
        return (await client.UploadAsync(fileBytes, uploadPath, createRemoteDir: true)).IsSuccess();
    }

    /// <inheritdoc />
    public async Task<bool> DownloadAsync(FtpModel ftpAction, string downloadPath, string writePath)
    {
        if (!ftpAction.AllFilesInFolder)
        {
            return (await client.DownloadFileAsync(writePath, downloadPath)).IsSuccess();
        }
        
        // Get the names of the files that need to be downloaded.
        var filesToDownload = await GetFilesInFolderAsync(ftpAction, downloadPath);
        if (!filesToDownload.Any())
        {
            return true;
        }
            
        // Combine the name with the path to the folder.
        for (var i = 0; i < filesToDownload.Count; i++)
        {
            filesToDownload[i] = Path.Combine(downloadPath, filesToDownload[i]);
        }
        
        var downloadCount = await client.DownloadFilesAsync(writePath, filesToDownload);
        return downloadCount == filesToDownload.Count;
    }

    /// <inheritdoc />
    public async Task<List<string>> GetFilesInFolderAsync(FtpModel ftpAction, string folderPath)
    {
        var listing = await client.GetListingAsync(folderPath);

        return listing.Select(file => file.Name).ToList();
    }

    /// <inheritdoc />
    public async Task<bool> DeleteFileAsync(FtpModel ftpAction, string filePath)
    {
        if (ftpAction.AllFilesInFolder)
        {
            var files = await GetFilesInFolderAsync(ftpAction, filePath);
            foreach (var file in files)
            {
                await client.DeleteFileAsync(Path.Combine(filePath, file));
            }

            return true;
        }

        await client.DeleteFileAsync(filePath);
        return true;
    }
}