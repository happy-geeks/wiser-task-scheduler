﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models.OAuth;

namespace WiserTaskScheduler.Core.Services
{
    public class OAuthService : IOAuthService, ISingletonService
    {
        private const string LogName = "OAuthService";

        private readonly GclSettings gclSettings;
        private readonly ILogService logService;
        private readonly ILogger<OAuthService> logger;
        private readonly IServiceProvider serviceProvider;

        private OAuthConfigurationModel configuration;

        // Semaphore is a locking system that can be used with async code.
        private static readonly SemaphoreSlim OauthApiLock = new(1, 1);

        public OAuthService(IOptions<GclSettings> gclSettings, ILogService logService, ILogger<OAuthService> logger, IServiceProvider serviceProvider)
        {
            this.gclSettings = gclSettings.Value;
            this.logService = logService;
            this.logger = logger;
            this.serviceProvider = serviceProvider;
        }

        /// <inheritdoc />
        public async Task SetConfigurationAsync(OAuthConfigurationModel configuration)
        {
            this.configuration = configuration;

            if (this.configuration.OAuths == null || !this.configuration.OAuths.Any())
            {
                await logService.LogWarning(logger, LogScopes.StartAndStop, this.configuration.LogSettings, "An OAuth configuration has been added but it does not contain OAuths to setup. Consider removing the OAuth configuration.", LogName);
                return;
            }

            using var scope = serviceProvider.CreateScope();
            var objectsService = scope.ServiceProvider.GetRequiredService<IObjectsService>();

            // Check if there is already information stored in the database to use.
            foreach (var oAuth in this.configuration.OAuths)
            {
                oAuth.AccessToken = (await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_AccessToken"))?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
                oAuth.TokenType = await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_TokenType");
                oAuth.RefreshToken = (await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_RefreshToken"))?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
                var expireTime = await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_ExpireTime");
                oAuth.ExpireTime = String.IsNullOrWhiteSpace(expireTime) ? DateTime.MinValue : Convert.ToDateTime(expireTime);

                oAuth.LogSettings ??= this.configuration.LogSettings;
            }
        }

        /// <inheritdoc />
        public async Task<string> GetAccessTokenAsync(string apiName, bool retryAfterWrongRefreshToken = true)
        {
            var oAuthApi = configuration.OAuths.SingleOrDefault(oAuth => oAuth.ApiName.Equals(apiName));

            if (oAuthApi == null)
            {
                return null;
            }

            // Check if a new access token needs to be requested and request it.
            OAuthState result;

            // Lock to prevent multiple requests at once.
            await OauthApiLock.WaitAsync();

            try
            {
                if (!String.IsNullOrWhiteSpace(oAuthApi.AccessToken) && oAuthApi.ExpireTime > DateTime.Now)
                {
                    result = OAuthState.CurrentToken;
                }
                else
                {
                    var formData = new List<KeyValuePair<string, string>>();

                    // Setup correct authentication.
                    OAuthState failState;
                    if (String.IsNullOrWhiteSpace(oAuthApi.AccessToken) || String.IsNullOrWhiteSpace(oAuthApi.RefreshToken))
                    {
                        await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Requesting new access token for '{apiName}' using username and password.", LogName);

                        failState = OAuthState.FailedLogin;
                        formData.Add(new KeyValuePair<string, string>("grant_type", "password"));
                        formData.Add(new KeyValuePair<string, string>("username", oAuthApi.Username));
                        formData.Add(new KeyValuePair<string, string>("password", oAuthApi.Password));
                    }
                    else
                    {
                        await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Requesting new access token for '{apiName}' using refresh token.", LogName);

                        failState = OAuthState.FailedRefreshToken;
                        formData.Add(new KeyValuePair<string, string>("grant_type", "refresh_token"));
                        formData.Add(new KeyValuePair<string, string>("refresh_token", oAuthApi.RefreshToken));
                    }

                    if (oAuthApi.FormKeyValues != null)
                    {
                        foreach (var keyValue in oAuthApi.FormKeyValues)
                        {
                            formData.Add(new KeyValuePair<string, string>(keyValue.Key, keyValue.Value));
                        }
                    }

                    var request = new HttpRequestMessage(HttpMethod.Post, oAuthApi.Endpoint)
                    {
                        Content = new FormUrlEncodedContent(formData)
                    };
                    request.Headers.Add("Accept", "application/json");

                    using var client = new HttpClient();
                    var response = await client.SendAsync(request);

                    using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
                    var json = await reader.ReadToEndAsync();

                    if (!response.IsSuccessStatusCode)
                    {
                        await logService.LogError(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Failed to get access token for {oAuthApi.ApiName}. Received: {response.StatusCode}\n{json}", LogName);
                        result = failState;
                    }
                    else
                    {
                        var body = JObject.Parse(json);

                        oAuthApi.AccessToken = (string) body["access_token"];
                        oAuthApi.TokenType = (string) body["token_type"];
                        oAuthApi.RefreshToken = (string) body["refresh_token"];

                        if (body["expires_in"].Type == JTokenType.Integer)
                        {
                            oAuthApi.ExpireTime = DateTime.Now.AddSeconds((int) body["expires_in"]);
                        }
                        else
                        {
                            oAuthApi.ExpireTime = DateTime.Now.AddSeconds(Convert.ToInt32((string) body["expires_in"]));
                        }

                        oAuthApi.ExpireTime -= oAuthApi.ExpireTimeOffset;

                        await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"A new access token has been retrieved for {oAuthApi.ApiName} and is valid till {oAuthApi.ExpireTime}", LogName);

                        result = OAuthState.NewToken;
                    }
                }
            }
            finally
            {
                // Release the lock. This is in a finally to be 100% sure that it will always be released. Otherwise the application might freeze.
                OauthApiLock.Release();
            }

            if (result == OAuthState.FailedLogin)
            {
                return null;
            }

            if (result == OAuthState.FailedRefreshToken)
            {
                if (!retryAfterWrongRefreshToken)
                {
                    return null;
                }

                //Retry to get the token with the login credentials if the refresh token was invalid.
                await RequestWasUnauthorizedAsync(apiName);
                return await GetAccessTokenAsync(apiName, false);
            }

            if (result == OAuthState.CurrentToken)
            {
                return $"{oAuthApi.TokenType} {oAuthApi.AccessToken}";
            }

            await SaveToDatabaseAsync(oAuthApi);

            return $"{oAuthApi.TokenType} {oAuthApi.AccessToken}";
        }

        /// <inheritdoc />
        public async Task RequestWasUnauthorizedAsync(string apiName)
        {
            var oAuthApi = configuration.OAuths.SingleOrDefault(oAuth => oAuth.ApiName.Equals(apiName));

            if (oAuthApi == null)
            {
                return;
            }

            oAuthApi.AccessToken = null;
            oAuthApi.TokenType = null;
            oAuthApi.RefreshToken = null;
            oAuthApi.ExpireTime = DateTime.MinValue;

            await SaveToDatabaseAsync(oAuthApi);
        }

        /// <summary>
        /// Save the state/information of the OAuth to the database.
        /// </summary>
        /// <param name="oAuthApi">The name of the API to save the information for.</param>
        private async Task SaveToDatabaseAsync(OAuthModel oAuthApi)
        {
            using var scope = serviceProvider.CreateScope();
            var objectsService = scope.ServiceProvider.GetRequiredService<IObjectsService>();

            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_AccessToken", oAuthApi.AccessToken.EncryptWithAes(gclSettings.DefaultEncryptionKey));
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_TokenType", oAuthApi.TokenType);
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_RefreshToken", oAuthApi.RefreshToken.EncryptWithAes(gclSettings.DefaultEncryptionKey));
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_ExpireTime", oAuthApi.ExpireTime.ToString());
        }
    }
}