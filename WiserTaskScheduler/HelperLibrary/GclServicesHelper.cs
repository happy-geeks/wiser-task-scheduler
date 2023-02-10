using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Communication.Interfaces;
using GeeksCoreLibrary.Modules.Communication.Services;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HelperLibrary;

public class GclServicesHelper
{
    /// <summary>
    /// Get a <see cref="IWiserItemsService"/>.
    /// Wiser Items Service requires dependency injection that results in the need of MVC services that are unavailable.
    /// Get all other services and create the Wiser Items Service with one of the services missing.
    /// </summary>
    /// <param name="scope">The scope to use.</param>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <param name="gclSettings">Optionally GCL settings, if not provided they will be injected.</param>
    /// <returns>Returns an instance of <see cref="IWiserItemsService"/></returns>
    public static IWiserItemsService GetWiserItemsService(IServiceScope scope, IDatabaseConnection? databaseConnection, IOptions<GclSettings>? gclSettings)
    {
        var objectService = scope.ServiceProvider.GetRequiredService<IObjectsService>();
        var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();
        var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
        gclSettings ??= scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        var wiserItemsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<WiserItemsService>>();
        
        return new WiserItemsService(databaseConnection, objectService, stringReplacementsService, null, databaseHelpersService, gclSettings, wiserItemsServiceLogger);
    }

    /// <summary>
    /// Get a <see cref="ICommunicationsService"/>.
    /// Communications Service requires the Wiser Items Service that can't be injected with dependency injection.
    /// </summary>
    /// <param name="scope">The scope to use.</param>
    /// <param name="databaseConnection">The database connection to use.</param>
    /// <returns>Returns an instance of <see cref="ICommunicationsService"/>.</returns>
    public static ICommunicationsService GetCommunicationsService(IServiceScope scope, IDatabaseConnection? databaseConnection)
    {
        var gclSettings = scope.ServiceProvider.GetRequiredService<IOptions<GclSettings>>();
        var gclCommunicationsServiceLogger = scope.ServiceProvider.GetRequiredService<ILogger<CommunicationsService>>();
        return new CommunicationsService(gclSettings, gclCommunicationsServiceLogger, GetWiserItemsService(scope, databaseConnection, gclSettings), databaseConnection);
    }
}