using System.Collections.Generic;
using System.Threading.Tasks;
using WiserTaskScheduler.Modules.Wiser.Models;

namespace WiserTaskScheduler.Modules.Wiser.Interfaces;

/// <summary>
/// A service to handle the communication with the Wiser API.
/// </summary>
public interface IWiserService
{
    /// <summary>
    /// Make a request to the API to get all XML configurations.
    /// </summary>
    /// <returns></returns>
    Task<List<TemplateSettingsModel>> RequestConfigurations();

    /// <summary>
    /// Get the access token and gets a new token if none is available or if it has expired.
    /// </summary>
    /// <returns>The access token to authenticate with the Wiser API.</returns>
    Task<string> GetAccessTokenAsync();

    /// <summary>
    /// Get the access token for the Iteration branch and gets a new token if none is available or if it has expired.
    /// </summary>
    /// <returns>The access token to authenticate with the Wiser API for iteration branch.</returns>
    Task<string> GetIterationAccessTokenAsync();
}