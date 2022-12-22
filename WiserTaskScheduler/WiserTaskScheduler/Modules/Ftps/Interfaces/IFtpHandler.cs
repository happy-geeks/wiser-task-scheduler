﻿using System.Collections.Generic;
using System.Threading.Tasks;
using WiserTaskScheduler.Modules.Ftps.Models;

namespace WiserTaskScheduler.Modules.Ftps.Interfaces;

public interface IFtpHandler
{
    /// <summary>
    /// Open the connection to an FTP server.
    /// </summary>
    /// <param name="ftpAction">The <see cref="FtpModel"/> containing the information.</param>
    /// <returns></returns>
    Task OpenConnectionAsync(FtpModel ftpAction);

    /// <summary>
    /// Close the connection to an FTP server.
    /// </summary>
    /// <returns></returns>
    Task CloseConnectionAsync();

    Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, string fromPath);
    
    /// <summary>
    /// Upload a file to an FTP server.
    /// </summary>
    /// <param name="ftpAction">The <see cref="FtpModel"/> containing the information.</param>
    /// <param name="uploadPath">The full path to file to.</param>
    /// <param name="fileBytes">The bytes of the file to upload.</param>
    /// <returns>Returns if upload was successful.</returns>
    Task<bool> UploadAsync(FtpModel ftpAction, string uploadPath, byte[] fileBytes);

    /// <summary>
    /// Download a file from an FTP server.
    /// </summary>
    /// <param name="ftpAction">The <see cref="FtpModel"/> containing the information.</param>
    /// <param name="downloadPath">The full path to download the file from or the folder path if all files are downloaded.</param>
    /// <param name="writePath">The full path to write the file to or the folder path if all files are downloaded.</param>
    /// <returns>If the file is downloaded.</returns>
    Task<bool> DownloadAsync(FtpModel ftpAction, string downloadPath, string writePath);

    /// <summary>
    /// Get all names of files in a folder in an FTP server.
    /// </summary>
    /// <param name="ftpAction">The <see cref="FtpModel"/> containing the information.</param>
    /// <param name="folderPath">The full path to the folder </param>
    /// <returns>Returns a list with all file names.</returns>
    Task<List<string>> GetFilesInFolderAsync(FtpModel ftpAction, string folderPath);

    /// <summary>
    /// Delete a file from an FTP server.
    /// </summary>
    /// <param name="ftpAction">The <see cref="FtpModel"/> containing the information.</param>
    /// <param name="filePath">The full path to the file to delete.</param>
    /// <returns>Returns if the file has been deleted.</returns>
    Task<bool> DeleteFileAsync(FtpModel ftpAction, string filePath);
}