using System;
using System.Collections.Generic;
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
        private readonly string logName;

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
            
            logName = $"CleanupService ({Environment.MachineName})";
        }

        /// <inheritdoc />
        public async Task CleanupAsync()
        {
            using var scope = serviceProvider.CreateScope();
            await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
            var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
            
            await CleanupFilesAsync();
            await CleanupDatabaseLogsAsync(databaseConnection, databaseHelpersService);
            await CleanupDatabaseRenderTimesAsync(databaseConnection, databaseHelpersService);
            await CleanupWtsServicesAsync(databaseConnection, databaseHelpersService);
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
                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Found {files.Length} files in '{folderPath}' to perform cleanup on.", logName);
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
                            await logService.LogInformation(logger, LogScopes.RunBody, LogSettings, $"Deleted file: {file}", logName);
                        }
                        catch (Exception e)
                        {
                            await logService.LogError(logger, LogScopes.RunBody, LogSettings, $"Could not delete file: {file} due to exception {e}", logName);
                        }
                    }

                    await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {filesDeleted} files in '{folderPath}'.", logName);
                }
                catch (Exception e)
                {
                    await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"Could not delete files in folder: {folderPath} due to exception {e}", logName);
                }
            }
        }

        /// <summary>
        /// Cleanup logs in the database older than the set number of days in the WTS logs.
        /// </summary>
        /// <param name="databaseConnection">The database connection to use.</param>
        /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
        private async Task CleanupDatabaseLogsAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
        {
            try
            {
                databaseConnection.SetCommandTimeout(cleanupServiceSettings.Timeout);
                databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStore));
                var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WtsLogs} WHERE added_on < ?cleanupDate", cleanUp: true);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WtsLogs}'.", logName);

                if (cleanupServiceSettings.OptimizeLogsTableAfterCleanup)
                {
                    await databaseHelpersService.OptimizeTablesAsync(WiserTableNames.WtsLogs);
                }
            }
            catch (Exception e)
            {
                await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during cleanup: {e}", logName);
            }
        }

        /// <summary>
        /// Cleanup render times in the database older than the set number of days.
        /// </summary>
        /// <param name="databaseConnection">The database connection to use.</param>
        /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
        private async Task CleanupDatabaseRenderTimesAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
        {
            databaseConnection.SetCommandTimeout(cleanupServiceSettings.Timeout);
            databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStoreRenderTimes));
            var optimizeRenderLogTables = new List<string>();

            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserTemplateRenderLog))
            {
                var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WiserTemplateRenderLog} WHERE end < ?cleanupDate", cleanUp: true);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WiserTemplateRenderLog}'.", logName);
                optimizeRenderLogTables.Add(WiserTableNames.WiserTemplateRenderLog);
            }

            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserDynamicContentRenderLog))
            {
                var rowsDeleted = await databaseConnection.ExecuteAsync($"DELETE FROM {WiserTableNames.WiserDynamicContentRenderLog} WHERE end < ?cleanupDate", cleanUp: true);
                await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WiserDynamicContentRenderLog}'.", logName);
                optimizeRenderLogTables.Add(WiserTableNames.WiserDynamicContentRenderLog);
            }

            if (cleanupServiceSettings.OptimizeRenderTimesTableAfterCleanup && optimizeRenderLogTables.Any())
            {
                await databaseHelpersService.OptimizeTablesAsync(optimizeRenderLogTables.ToArray());
            }
        }
        
        /// <summary>
        /// Cleanup wts services in the database older than the set number of days in the WTS services.
        /// </summary>
        /// <param name="databaseConnection">The database connection to use.</param>
        /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
        private async Task CleanupWtsServicesAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
        {
            databaseConnection.AddParameter("cleanupDate", DateTime.Now.AddDays(-cleanupServiceSettings.NumberOfDaysToStoreWtsServices));

            // Use the next_run column to determine if a service is older than the cleanup date to prevent paused and longer run schemes from being deleted.
            var query = $"DELETE FROM {WiserTableNames.WtsServices} WHERE next_run < ?cleanupDate";
            var rowsDeleted = await databaseConnection.ExecuteAsync(query, cleanUp: true);
            
            await logService.LogInformation(logger, LogScopes.RunStartAndStop, LogSettings, $"Cleaned up {rowsDeleted} rows in '{WiserTableNames.WtsServices}'.", logName);

            if (cleanupServiceSettings.OptimizeLogsTableAfterCleanup)
            {
                await databaseHelpersService.OptimizeTablesAsync(WiserTableNames.WtsServices);
            }
        }
    }
}
