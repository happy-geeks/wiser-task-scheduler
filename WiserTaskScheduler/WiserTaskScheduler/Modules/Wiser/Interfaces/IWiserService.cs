using System.Collections.Generic;
using System.Threading.Tasks;
using WiserTaskScheduler.Modules.Wiser.Models;

namespace WiserTaskScheduler.Modules.Wiser.Interfaces
{
    /// <summary>
    /// A service to handle the communication with the Wiser API.
    /// </summary>
    public interface IWiserService
    {
        string AccessToken { get; }

        /// <summary>
        /// Make a request to the API to get all XML configurations.
        /// </summary>
        /// <returns></returns>
        Task<List<TemplateSettingsModel>> RequestConfigurations();
    }
}
