using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Branches.Interfaces;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Models.ParentsUpdate;

namespace WiserTaskScheduler.Core.Services
{
    /// <summary>
    /// A service to perform updates to parent items that are listed in the wiser_parent_updates table
    /// </summary>
    public class ParentUpdateService : IParentUpdateService, ISingletonService
    {
        private const string LogName = "ParentUpdateService";

        private readonly ParentsUpdateServiceSettings parentsUpdateServiceSettings;
        private readonly IServiceProvider serviceProvider;
        private readonly ILogService logService;
        private readonly ILogger<ParentUpdateService> logger;
        private readonly IBranchesService branchesService;
        
        private bool updatedParentUpdatesTable = false;
        private bool updatedTargetDatabaseList = false;
            
        // Database strings used to target other dbs in the same cluster.
        private readonly List<ParentUpdateDatabaseStrings> targetDatabases = [];

        /// <inheritdoc />
        public LogSettings LogSettings { get; set; }

        public ParentUpdateService(IOptions<WtsSettings> wtsSettings, IServiceProvider serviceProvider, ILogService logService, ILogger<ParentUpdateService> logger, IBranchesService branchesService)
        {
            parentsUpdateServiceSettings = wtsSettings.Value.ParentsUpdateService;
            this.serviceProvider = serviceProvider;
            this.logService = logService;
            this.logger = logger;
            this.branchesService = branchesService;
        }

        /// <inheritdoc />
        public async Task ParentsUpdateAsync()
        {
            using var scope = serviceProvider.CreateScope();
            await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
            var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
            
            // Update parent table if it has not already been done since launch. The table definitions can only change when the WTS restarts with a new update.
            if (!updatedParentUpdatesTable)
            {
                await databaseHelpersService.CheckAndUpdateTablesAsync(new List<string> 
                    {
                        WiserTableNames.WiserParentUpdates
                    }
                );
                updatedParentUpdatesTable = true;
            }

            if (!updatedTargetDatabaseList)
            {
                UpdateTargetedDatabases(databaseConnection);
                updatedTargetDatabaseList = true;
            }

            foreach (var database in targetDatabases)
            {
                await ParentsUpdateMainAsync(databaseConnection, databaseHelpersService, database);    
            }
        }

        /// <summary>
        /// The main parent update routine, checks if there are updates to be performed and performs them, then truncates WiserTableNames.WiserParentUpdates table.
        /// </summary>
        /// <param name="databaseConnection">The database connection to use.</param>
        /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
        /// <param name="targetDatabase">The database we are applying the parent updates on.</param>
        private async Task ParentsUpdateMainAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService, ParentUpdateDatabaseStrings targetDatabase)
        {
            if (await databaseHelpersService.DatabaseExistsAsync(targetDatabase.DatabaseName))
            {
                if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserParentUpdates))
                {
                    var dataTable = await databaseConnection.GetAsync(targetDatabase.ListTableQuery);

                    foreach (DataRow dataRow in dataTable.Rows)
                    {
                        var tableName = dataRow.Field<string>("target_table");

                        var query = $"UPDATE {targetDatabase.DatabaseName}.{tableName} item INNER JOIN {targetDatabase.DatabaseName}.{WiserTableNames.WiserParentUpdates} `updates` ON `item`.id = `updates`.target_id AND `updates`.target_table = {targetDatabase.DatabaseName}.'{tableName}' SET `item`.changed_on = `updates`.changed_on, `item`.changed_by = `updates`.changed_by;";

                        try 
                        {
                            await databaseConnection.ExecuteAsync(query);
                            
                            try
                            {
                                await databaseConnection.ExecuteAsync(targetDatabase.CleanUpQuery);
                            }
                            catch (Exception e)
                            {
                                logger.LogError($"Failed to run query ( {targetDatabase.CleanUpQuery} ) in parent update service due to exception:{Environment.NewLine}{Environment.NewLine}{e}");
                            }
                        }
                        catch (Exception e)
                        {
                            logger.LogError($"Failed to run query ( {query} ) in parent update service due to exception:{Environment.NewLine}{Environment.NewLine}{e}");
                        }
                    }
                }
            }
        }
        
        // <summary>
        /// Helper function to rebuild the targeted database list
        /// </summary>
        private void UpdateTargetedDatabases(IDatabaseConnection databaseConnection)
        {
            targetDatabases.Clear();
            
            // Add main database.
            var listTablesQuery = $"SELECT DISTINCT `target_table` FROM `{WiserTableNames.WiserParentUpdates}`;";
            var parentsCleanUpQuery = $"TRUNCATE `{WiserTableNames.WiserParentUpdates}`;";
            
            targetDatabases.Add(new ParentUpdateDatabaseStrings(databaseConnection.ConnectedDatabase, listTablesQuery, parentsCleanUpQuery));
            
            if (parentsUpdateServiceSettings.AdditionalDatabases != null)
            {
                // Add additional databases.
                foreach (var additionalDatabase in parentsUpdateServiceSettings.AdditionalDatabases)
                {
                    listTablesQuery = $"SELECT DISTINCT `target_table` FROM `{additionalDatabase}`.`{WiserTableNames.WiserParentUpdates}`;";
                    parentsCleanUpQuery = $"TRUNCATE `{additionalDatabase}``{WiserTableNames.WiserParentUpdates}`;";

                    targetDatabases.Add(new ParentUpdateDatabaseStrings(additionalDatabase, listTablesQuery, parentsCleanUpQuery));
                }
            }
        }   
    }
}
