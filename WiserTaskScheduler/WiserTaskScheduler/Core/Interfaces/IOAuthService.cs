using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Models.OAuth;

namespace WiserTaskScheduler.Core.Interfaces;

public interface IOAuthService
{
    /// <summary>
    /// Set the configuration to be used for OAuth calls.
    /// </summary>
    /// <param name="oAuthConfigurationModel">The configuration to set.</param>
    /// <returns></returns>
    Task SetConfigurationAsync(OAuthConfigurationModel oAuthConfigurationModel);

    /// <summary>
    /// Get the access token of the specified API.
    /// </summary>
    /// <param name="apiName">The name of the API to get the access token from.</param>
    /// <param name="retryAfterWrongRefreshToken">Retry to get an access token using login credentials if the refresh token didn't give a new access token.</param>
    /// <param name="configurationName">The configuration that is currently running, for logging purposes</param>
    /// <param name="timeId">The timeId that is currently running, for logging purposes</param>
    /// <param name="order">The order that is currently running, for logging purposes</param>
    /// <returns>Returns the access token to the API.</returns>
    Task<(OAuthState State, string AuthorizationHeaderValue, JToken ResponseBody, HttpStatusCode ResponseStatusCode)> GetAccessTokenAsync(string apiName, bool retryAfterWrongRefreshToken, string configurationName, int timeId, int order);

    /// <summary>
    /// Tells that the specified API gave an access token was invalid and caused the request to be "Unauthorized".
    /// API information will be cleared so the next request will request a new access token.
    /// </summary>
    /// <param name="apiName">The name of the API that gave an invalid access token.</param>
    /// <param name="resetRefreshToken">Optional: also resets the refresh token if true.</param>
    /// <returns></returns>
    Task RequestWasUnauthorizedAsync(string apiName, bool resetRefreshToken = false);
}