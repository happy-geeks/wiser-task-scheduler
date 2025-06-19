using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.ProductsApiUpdate.Models;
using WiserTaskScheduler.Modules.ProductsApiUpdate.Interfaces;

namespace WiserTaskScheduler.Modules.ProductsApiUpdate.Services;

/// <summary>
/// A service to perform updates on products when the products api is used.
/// </summary>
public class ProductsApiUpdateService(
    ILogService logService,
    ILogger<ProductsApiUpdateService> logger,
    IServiceProvider serviceProvider,
    IOAuthService oAuthService,
    IOptions<GclSettings> gclSettings) : IProductsApiUpdateService, IActionsService, IScopedService
{
    // The section used for the refresh-all API call, update this if the API changes in the future.
    private const string ApiUrlSection = "/api/v3/products/refresh-all";

    // the timeout value we use for refresh-calls.
    private const int RefreshTimeout = 600;

    public Task InitializeAsync(ConfigurationModel configuration, HashSet<string> tablesToOptimize)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public async Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName)
    {
        var productsApiUpdate = (ProductsApiUpdateModel) action;

        if (String.IsNullOrEmpty(productsApiUpdate.ServerUrl))
        {
            var msg = $"The Products Api Update Service '{configurationServiceName}', time ID '{productsApiUpdate.TimeId}', order '{productsApiUpdate.Order}' Does not have a server url configured.";
            await logService.LogError(logger, LogScopes.StartAndStop, productsApiUpdate.LogSettings, msg, configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
            return new JObject
            {
                { "StatusCode", ((int)HttpStatusCode.InternalServerError).ToString() },
                { "BodyPlainText", "no server url provided" }
            };
        }

        if (String.IsNullOrEmpty(productsApiUpdate.OAuth))
        {
            var msg = $"The Products Api Update Service '{configurationServiceName}', time ID '{productsApiUpdate.TimeId}', order '{productsApiUpdate.Order}' Does not have a Oauth configured.";
            await logService.LogError(logger, LogScopes.StartAndStop, productsApiUpdate.LogSettings, msg, configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
            return new JObject
            {
                { "StatusCode", ((int)HttpStatusCode.InternalServerError).ToString() },
                { "BodyPlainText", "no Oauth provided" }
            };
        }

        var fullUrl = $"{productsApiUpdate.ServerUrl}{ApiUrlSection}";
        await logService.LogInformation(logger, LogScopes.RunBody, productsApiUpdate.LogSettings, $"Url: {fullUrl}, method: {HttpMethod.Post}", configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
        var request = new HttpRequestMessage(HttpMethod.Post, fullUrl);

        if (!String.IsNullOrWhiteSpace(productsApiUpdate.OAuth))
        {
            var (oauthState, authorizationHeaderValue, _, _) = await oAuthService.GetAccessTokenAsync(productsApiUpdate.OAuth);
            switch (oauthState)
            {
                case OAuthState.SuccessfullyRequestedNewToken:
                case OAuthState.UsingAlreadyExistingToken:
                    request.Headers.Add("Authorization", authorizationHeaderValue);
                    break;
                case OAuthState.AuthenticationFailed:
                case OAuthState.RefreshTokenFailed:
                case OAuthState.NotEnoughInformation:
                    await logService.LogWarning(logger, LogScopes.RunBody, productsApiUpdate.LogSettings,
                        $"OAuth '{productsApiUpdate.OAuth}' authentication failed ({oauthState.ToString()}) for configuration '{configurationServiceName}' with time ID '{productsApiUpdate.TimeId}' and order '{productsApiUpdate.Order}'.",
                        configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
                    break;
                case OAuthState.WaitingForManualAuthentication:
                    // If we're waiting for manual authentication,
                    // return an unauthorized response and don't attempt to execute the API request.
                    return new JObject
                    {
                        { "StatusCode", ((int)HttpStatusCode.Unauthorized).ToString() },
                        { "Body", "" },
                        { "BodyPlainText", "" },
                        { "UsedResultSet", null }
                    };
                default:
                    throw new ArgumentOutOfRangeException(nameof(oauthState), oauthState.ToString(), null);
            }
        }

        await logService.LogInformation(logger, LogScopes.RunBody, productsApiUpdate.LogSettings, "", configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);

        var httpHandler = new HttpClientHandler();

        using var client = new HttpClient(httpHandler);

        client.Timeout = TimeSpan.FromSeconds(RefreshTimeout);

        using var response = await client.SendAsync(request);

        if (response.StatusCode != HttpStatusCode.OK)
        {
            await logService.LogError(logger, LogScopes.StartAndStop, productsApiUpdate.LogSettings, $"The Products Api did not return with status code 200 but {response.StatusCode} '{configurationServiceName}', time ID '{productsApiUpdate.TimeId}', order '{productsApiUpdate.Order}' Does not have a Oauth configured.", configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
            return null;
        }

        var responseBody = await response.Content.ReadAsStringAsync();
        var responseObject = JObject.Parse(responseBody);

        if ( responseObject.TryGetValue("IsDone", out var isDone))
        {
            if (isDone.Type == JTokenType.Boolean && isDone.Value<bool>())
            {
                await logService.LogInformation(logger, LogScopes.RunBody, productsApiUpdate.LogSettings, "The Products Api Update has been completed successfully.", configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
                return isDone;
            }
            else
            {
                await logService.LogWarning(logger, LogScopes.RunBody, productsApiUpdate.LogSettings, "The Products Api Update is not done yet. Please check the API response for more details.", configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
                return isDone;
            }
        }
        else
        {
            await logService.LogWarning(logger, LogScopes.RunBody, productsApiUpdate.LogSettings, "The Products Api Update response does not contain an 'IsDone' field. Please check the API response for more details.", configurationServiceName, productsApiUpdate.TimeId, productsApiUpdate.Order);
        }

        return null;
    }
}