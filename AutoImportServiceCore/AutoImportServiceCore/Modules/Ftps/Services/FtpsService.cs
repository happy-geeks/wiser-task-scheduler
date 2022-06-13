using System.Threading.Tasks;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.Ftps.Services;

public class FtpsService : IFtpsService, IActionsService, IScopedService
{
    /// <inheritdoc />
    public async Task Initialize(ConfigurationModel configuration)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var ftpAction = (FtpModel) action;
        
        throw new System.NotImplementedException();
    }
}