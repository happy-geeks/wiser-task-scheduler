using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Extensions;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Models;
using FluentFTP;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.Logging;

namespace AutoImportServiceCore.Modules.Ftps.Services;

public class FtpsHandler : IFtpHandler
{
    private FtpClient client;

    /// <inheritdoc />
    public async Task OpenConnectionAsync(FtpModel ftpAction)
    {
        client = new FtpClient(ftpAction.Host, ftpAction.User, ftpAction.Password);
        client.EncryptionMode = ftpAction.EncryptionMode.ConvertToFtpsEncryptionMode();
        client.ValidateAnyCertificate = ftpAction.Host.StartsWith("localhost", StringComparison.OrdinalIgnoreCase);

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
        if (ftpAction.AllFilesInFolder)
        {
            await client.UploadDirectoryAsync(fromPath, uploadPath, existsMode: FtpRemoteExists.Overwrite);
            return true;
        }
        
        await client.UploadAsync(await File.ReadAllBytesAsync(fromPath), uploadPath, createRemoteDir: true);
        return true;
    }

    /// <inheritdoc />
    public async Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, byte[] fileBytes)
    {
        await client.UploadAsync(fileBytes, uploadPath, createRemoteDir: true);
        return true;
    }

    /// <inheritdoc />
    public async Task<byte[]> DownloadAsync(FtpModel ftpAction, string downloadPath)
    {
        return await client.DownloadAsync(downloadPath, default);
    }

    /// <inheritdoc />
    public async Task<List<string>> GetFilesInFolderAsync(FtpModel ftpAction, string folderPath)
    {
        var filesInFolder = new List<string>();
        
        var listing = await client.GetListingAsync(folderPath);

        foreach (var file in listing)
        {
            filesInFolder.Add(file.Name);
        }

        return filesInFolder;
    }

    /// <inheritdoc />
    public async Task<bool> DeleteFileAsync(FtpModel ftpAction, string filePath)
    {
        if (ftpAction.AllFilesInFolder)
        {
            var files = await GetFilesInFolderAsync(ftpAction, filePath);
            foreach (var file in files)
            {
                await client.DeleteFileAsync($"{filePath}{file}");
            }

            return true;
        }

        await client.DeleteFileAsync(filePath);
        return true;
    }
}