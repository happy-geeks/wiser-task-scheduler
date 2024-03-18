using System;
using System.Data;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
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

        private readonly string listTablesQuery = "SELECT DISTINCT `target_table` FROM `wiser_parent_updates`;";
        
        private readonly string parentsCleanUpQuery = $"TRUNCATE `{WiserTableNames.WiserParentUpdates}`;";

        /// <inheritdoc />
        public LogSettings LogSettings { get; set; }

        public ParentUpdateService(IOptions<WtsSettings> wtsSettings, IServiceProvider serviceProvider, ILogService logService, ILogger<ParentUpdateService> logger)
        {
            parentsUpdateServiceSettings = wtsSettings.Value.ParentsUpdateService;
            this.serviceProvider = serviceProvider;
            this.logService = logService;
            this.logger = logger;
        }

        /// <inheritdoc />
        public async Task ParentsUpdateAsync()
        {
            using var scope = serviceProvider.CreateScope();
            await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();
            var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
            await ParentsUpdateMainAsync(databaseConnection, databaseHelpersService);
        }

        /// <summary>
        /// The main parent update routine, checks if there are updates to be performed and performs them, then truncates WiserTableNames.WiserParentUpdates table.
        /// </summary>
        /// <param name="databaseConnection">The database connection to use.</param>
        /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
        private async Task ParentsUpdateMainAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
        {
            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserParentUpdates))
            {
                var dataTable = await databaseConnection.GetAsync(listTablesQuery);

                foreach (DataRow dataRow in dataTable.Rows)
                {
                    var tableName = dataRow.Field<string>("target_table");

                    var query = $"UPDATE {tableName} item INNER JOIN wiser_parent_updates `updates` ON `item`.id = `updates`.target_id AND `updates`.target_table = '{tableName}' SET `item`.changed_on = `updates`.changed_on, `item`.changed_by = `updates`.changed_by;";

                    await databaseConnection.ExecuteAsync(query);
                }

                await databaseConnection.ExecuteAsync(parentsCleanUpQuery);
            }
        }
    }
}
