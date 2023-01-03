using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Renci.SshNet;
using WiserTaskScheduler.Modules.Ftps.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Models;

namespace WiserTaskScheduler.Modules.Ftps.Services;

public class SftpHandler : IFtpHandler
{
    private SftpClient client;

    /// <inheritdoc />
    public Task OpenConnectionAsync(FtpModel ftpAction)
    {
        AuthenticationMethod[] authenticationMethods;

        // Use SSH or username/password if none is set.
        if (!String.IsNullOrWhiteSpace(ftpAction.SshPrivateKeyPath))
        {
            authenticationMethods = new AuthenticationMethod[]
            {
                new PrivateKeyAuthenticationMethod(ftpAction.User, new PrivateKeyFile[]
                {
                    new(ftpAction.SshPrivateKeyPath, ftpAction.SshPrivateKeyPassphrase)
                })
            };
        }
        else
        {
            authenticationMethods = new AuthenticationMethod[]
            {
                new PasswordAuthenticationMethod(ftpAction.User, ftpAction.Password)
            };
        }
        
        var connectionInfo = new ConnectionInfo(ftpAction.Host, ftpAction.Port, ftpAction.User, authenticationMethods);
        client = new SftpClient(connectionInfo);
        client.Connect();
        
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task CloseConnectionAsync()
    {
        client.Disconnect();
        client.Dispose();
        return Task.CompletedTask;
    }

    public Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, string fromPath)
    {
        if (!ftpAction.AllFilesInFolder)
        {
            using var stream = File.OpenRead(fromPath);
            client.UploadFile(stream, uploadPath);
            return Task.FromResult(true);
        }
        
        foreach(var file in Directory.GetFiles(fromPath))
        {
            // Fix upload path, make dynamic with file name.
            using var stream = File.OpenRead(file);
            client.UploadFile(stream, Path.Combine(uploadPath, file.Split(Path.DirectorySeparatorChar)[^1]));
        }

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, byte[] fileBytes)
    {
        using var stream = new MemoryStream(fileBytes);
        client.UploadFile(stream, uploadPath);
        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public async Task<bool> DownloadAsync(FtpModel ftpAction, string downloadPath, string writePath)
    {
        if (!ftpAction.AllFilesInFolder)
        {
            await using var stream = File.OpenWrite(writePath);
            client.DownloadFile(downloadPath, stream);
            await stream.FlushAsync();
            return true;
        }

        // Get the names of the files that need to be downloaded.
        var filesToDownload = await GetFilesInFolderAsync(ftpAction, downloadPath);
        if (!filesToDownload.Any())
        {
            return true;
        }

        // Combine the name with the path to the folder.
        foreach (var file in filesToDownload)
        {
            await using var stream = File.OpenWrite(Path.Combine(writePath, file));
            client.DownloadFile(Path.Combine(downloadPath, file), stream);
            await stream.FlushAsync();
        }

        return true;
    }

    /// <inheritdoc />
    public Task<List<string>> GetFilesInFolderAsync(FtpModel ftpAction, string folderPath)
    {
    var listing = new List<string>();

        foreach (var file in client.ListDirectory(folderPath))
        {
            if (!file.IsDirectory)
            {
                listing.Add(file.Name);
            }
        }

        return Task.FromResult(listing);
    }

    /// <inheritdoc />
    public async Task<bool> DeleteFileAsync(FtpModel ftpAction, string filePath)
    {
        if (ftpAction.AllFilesInFolder)
        {
            var files = await GetFilesInFolderAsync(ftpAction, filePath);
            foreach (var file in files)
            {
                client.Delete(Path.Combine(filePath, file));
            }

            return true;
        }

        client.Delete(filePath);
        return true;
    }
}