using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Enums;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Wiser.Interfaces;
using AutoImportServiceCore.Modules.Wiser.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace AutoImportServiceCore.Modules.Wiser.Services
{
    /// <inheritdoc cref="IWiserService"/> />
    public class WiserService : IWiserService, ISingletonService
    {
        private readonly WiserSettings wiserSettings;
        private readonly ILogService logService;
        private readonly ILogger<WiserService> logger;
        private readonly LogSettings logSettings;

        [Browsable(false)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        private string accessToken;
        private DateTime accessTokenExpireTime;
        private string refreshToken;

        public string AccessToken => GetAccessToken();

        public WiserService(IOptions<AisSettings> aisSettings, ILogService logService, ILogger<WiserService> logger)
        {
            wiserSettings = aisSettings.Value.Wiser;
            this.logService = logService;
            this.logger = logger;
            logSettings = wiserSettings?.LogSettings ?? new LogSettings();

            accessToken = "";
            accessTokenExpireTime = DateTime.MinValue;
            refreshToken = "";
        }

        /// <summary>
        /// Get the access token and gets a new token if none is available or if it has expired.
        /// </summary>
        /// <returns></returns>
        private string GetAccessToken()
        {
            // Lock to prevent multiple requests at once.
            lock (accessToken)
            {
                if (String.IsNullOrWhiteSpace(accessToken))
                {
                    Login();
                }
                else if (accessTokenExpireTime <= DateTime.Now)
                {
                    if (String.IsNullOrWhiteSpace(refreshToken))
                    {
                        Login();
                    }
                    else
                    {
                        Login(true);
                    }
                }

                return accessToken;
            }
        }

        /// <summary>
        /// DO NOT CALL THIS BY YOURSELF!
        /// Login to the Wiser API.
        /// This method is called when using <see cref="AccessToken"/> or <see cref="GetAccessToken"/> with a lock.
        /// </summary>
        private void Login(bool useRefreshToken = false)
        {
            var formData = new List<KeyValuePair<string, string>>()
            {
                new("subDomain", $"{wiserSettings.Subdomain}"),
                new("client_id", $"{wiserSettings.ClientId}"),
                new("client_secret", $"{wiserSettings.ClientSecret}"),
                new("isTestEnvironment", $"{wiserSettings.TestEnvironment}")
            };

            if (useRefreshToken)
            {
                formData.Add(new KeyValuePair<string, string>("grant_type", "refresh_token"));
                formData.Add(new KeyValuePair<string, string>("refresh_token", refreshToken));
            }
            else
            {
                formData.Add(new KeyValuePair<string, string>("grant_type", "password"));
                formData.Add(new KeyValuePair<string, string>("username", $"{wiserSettings.Username}"));
                formData.Add(new KeyValuePair<string, string>("password", $"{wiserSettings.Password}"));
            }

            var request = new HttpRequestMessage(HttpMethod.Post, $"{wiserSettings.WiserApiUrl}connect/token")
            {
                Content = new FormUrlEncodedContent(formData)
            };
            request.Headers.Add("Accept", "application/json");
            
            logService.LogInformation(logger, LogScopes.RunBody, logSettings, $"URL: {request.RequestUri}\nHeaders: {request.Headers}\nBody: {String.Join(' ', formData)}", "WiserService");
            
            using var client = new HttpClient();
            try
            {
                var response = client.Send(request);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    logService.LogCritical(logger, LogScopes.RunStartAndStop, logSettings, "Failed to login to the Wiser API.", "WiserService");
                    return;
                }

                using var reader = new StreamReader(response.Content.ReadAsStream());
                var body = reader.ReadToEnd();
                logService.LogInformation(logger, LogScopes.RunBody, logSettings, $"Response body: {body}", "WiserService");
                var wiserLoginResponse = JsonConvert.DeserializeObject<WiserLoginResponseModel>(body);

                accessToken = wiserLoginResponse.AccessToken;
                accessTokenExpireTime = DateTime.Now.AddSeconds(wiserLoginResponse.ExpiresIn).AddMinutes(-1); // Refresh 1 minute before expire.
                refreshToken = wiserLoginResponse.RefreshToken;
            }
            catch (Exception e)
            {
                logService.LogCritical(logger, LogScopes.RunStartAndStop, logSettings, $"Failed to login to the Wiser API.\n{e.Message}\n{e.StackTrace}", "WiserService");
            }
        }

        /// <inheritdoc />
        public async Task<List<TemplateSettingsModel>> RequestConfigurations()
        {
            // Lock cannot be used inside an async function. This way we can wait till the request has completed.
            return await Task.Run(() =>
            {
                var request = new HttpRequestMessage(HttpMethod.Get, $"{wiserSettings.WiserApiUrl}api/v3/templates/entire-tree-view?startFrom=SERVICES{(String.IsNullOrWhiteSpace(wiserSettings.ConfigurationPath) ? "" : $",{wiserSettings.ConfigurationPath}")}&environment={Environments.Live}");
                request.Headers.Add("Authorization", $"Bearer {AccessToken}");

                using var client = new HttpClient();
                try
                {
                    var response = client.Send(request);

                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        logService.LogCritical(logger, LogScopes.RunStartAndStop, logSettings, "Failed to get configurations from the Wiser API.", "WiserService");
                        return null;
                    }

                    using var reader = new StreamReader(response.Content.ReadAsStream());
                    var body = reader.ReadToEnd();
                    var templateTrees = JsonConvert.DeserializeObject<List<TemplateTreeViewModel>>(body);

                    return FlattenTree(templateTrees);
                }
                catch (Exception e)
                {
                    logService.LogCritical(logger, LogScopes.RunStartAndStop, logSettings, $"Failed to get configurations from the Wiser API.\n{e.Message}\n{e.StackTrace}", "WiserService");
                    return null;
                }
            });
        }

        private List<TemplateSettingsModel> FlattenTree(List<TemplateTreeViewModel> templateTrees)
        {
            var results = new List<TemplateSettingsModel>();

            foreach (var templateTree in templateTrees)
            {
                if (templateTree.HasChildren)
                {
                    results.AddRange(FlattenTree(templateTree.ChildNodes));
                }
                else
                {
                    results.Add(templateTree.TemplateSettings);
                }
            }

            return results;
        }
    }
}
