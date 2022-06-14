using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AutoImportServiceCore.Modules.Ftps.Extensions;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Models;
using FluentFTP;

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
    public async Task CloseConnectionAsync()
    {
        client.Dispose();
    }

    public async Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, string fromPath)
    {
        // TODO Use log service.

        try
        {
            if (ftpAction.AllFilesInFolder)
            {
                await client.UploadDirectoryAsync(fromPath, uploadPath, existsMode: FtpRemoteExists.Overwrite);
                return true;
            }
            
            await client.UploadAsync(await File.ReadAllBytesAsync(fromPath), uploadPath, createRemoteDir: true);
            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, byte[] fileBytes)
    {
        // TODO Use log service.

        try
        {
            await client.UploadAsync(fileBytes, uploadPath, createRemoteDir: true);
            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<byte[]> DownloadAsync(FtpModel ftpAction, string downloadPath)
    {
        try
        {
            return await client.DownloadAsync(downloadPath, default);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<List<string>> GetFilesInFolderAsync(FtpModel ftpAction, string folderPath)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc />
    public async Task<bool> DeleteFileAsync(FtpModel ftpAction, string filePath)
    {
        throw new System.NotImplementedException();
    }
}