using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Models.Cleanup;

namespace WiserTaskScheduler.Core.Services
{
    /// <summary>
    /// A service to manage all WTS configurations that are provided by Wiser.
    /// </summary>
    public class CleanupService : ICleanupService, ISingletonService
    {
        private const string LogName = "CleanupService";

        private readonly CleanupServiceSettings cleanupServiceSettings;
        private readonly IServiceProvider serviceProvider;
        private readonly ILogService logService;
        private readonly ILogger<CleanupService> logger;

        /// <inheritdoc />
        public LogSettings LogSettings { get; set; }

        public CleanupService(IOptions<WtsSettings> wtsSettings, IServiceProvider serviceProvider, ILogService logService, ILogger<CleanupService> logger)
        {
            cleanupServiceSettings = wtsSettings.Value.CleanupService;
            this.serviceProvider = serviceProvider;
            this.logService = logService;
            this.logger = logger;
        }

        /// <inheritdoc />
        public async Task CleanupAsync()
        {
            using var scope = serviceProvider.CreateScope();
            await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
            var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
            
            await CleanupFilesAsync();
            await CleanupDatabaseLogsAsync(databaseConnection, databaseHelpersService);
        }

        /// <summary>
        /// Cleanup files older than the set number of days in the given folders.
        /// </summary>
        private async Task CleanupFilesAsync()
        {
            if (cleanupServiceSettings.FileFolderPaths == null || !cleanupServiceSettings.FileFolderPaths.Any())
            {
                return;
            }

            foreach (var folderPath in cleanupServiceSettings.FileFolderPaths)
            {
                try
                {
                    var files = Directory.GetFiles(folderPath);
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Found {files.Length} files in '{folderPath}' to perform cleanup on.", LogName);
                    var filesDeleted = 0;

                    foreach (var file in files)
                    {
                        try
                        {
                            if (File.GetLastWriteTime(file) >= DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStore))
                            {
                                continue;
                            }

                            File.Delete(file);
                            filesDeleted++;
                            await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Deleted file: {file}", LogName);
                        }
                        catch (Exception e)
                        {
                            await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Could not delete file: {file} due to exception {e}", LogName);
                        }
                    }

                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {filesDeleted} files in '{folderPath}'.", LogName);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Could not delete files in folder: {folderPath} due to exception {e}", LogName);
                }
            }
        }

        /// <summary>
        /// Cleanup logs in the database older than the set number of days in the WTS logs.
        /// </summary>
        private async Task CleanupDatabaseLogsAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
        {
            databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStore));
            var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WtsLogs} WHERE added_on < ?cleanupDate", cleanUp: true);
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WtsLogs}'.", LogName);

            if (cleanupServiceSettings.OptimizeLogsTableAfterCleanup)
            {
                await databaseHelpersService.OptimizeTablesAsync(WiserTableNames.WtsLogs);
            }
        }
    }
}
