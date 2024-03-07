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
using WiserTaskScheduler.Core.Models.ParentsUpdate;

namespace WiserTaskScheduler.Core.Services
{
    /// <summary>
    /// A service to manage all WTS configurations that are provided by Wiser.
    /// </summary>
    public class ParentUpdateService : IParentUpdateService, ISingletonService
    {
        private const string LogName = "ParentUpdateService";

        private readonly ParentsUpdateServiceSettings parentsUpdateServiceSettings;
        private readonly IServiceProvider serviceProvider;
        private readonly ILogService logService;
        private readonly ILogger<ParentUpdateService> logger;

        private readonly string parentsUpdateQuery = "UPDATE wiser_item `item`"
                                                     + "INNER JOIN wiser_parent_updates `updates` ON `item`.id = `updates`.target_id AND `updates`.target_table = 'wiser_item'"
                                                     + "SET `item`.changed_on = `updates`.changed_on, `item`.changed_by = `updates`.changed_by;";

        private readonly string parentsCleanUpQuery = "TRUNCATE `wiser_parent_updates`;";

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
        /// The main parent update routine, checks is there are updates to be performed and performs them, then truncates the table.
        /// 
        /// </summary>
        /// <param name="databaseConnection">The database connection to use.</param>
        /// <param name="databaseHelpersService">The <see cref="IDatabaseHelpersService"/> to use.</param>
        private async Task ParentsUpdateMainAsync(IDatabaseConnection databaseConnection, IDatabaseHelpersService databaseHelpersService)
        {
            if (await databaseHelpersService.TableExistsAsync(WiserTableNames.WiserParentUpdates))
            {
                await databaseConnection.ExecuteAsync($"{parentsUpdateQuery} {parentsCleanUpQuery}", cleanUp: true);
            }
        }
    }
}
