using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Communication.Interfaces;
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
        public async Task SetConfigurationAsync(OAuthConfigurationModel oAuthConfigurationModel)
        {
            configuration = oAuthConfigurationModel;

            if (configuration.OAuths == null || !configuration.OAuths.Any())
            {
                await logService.LogWarning(logger, LogScopes.StartAndStop, configuration.LogSettings, "An OAuth configuration has been added but it does not contain OAuths to setup. Consider removing the OAuth configuration.", LogName);
                return;
            }

            using var scope = serviceProvider.CreateScope();
            var objectsService = scope.ServiceProvider.GetRequiredService<IObjectsService>();

            // Check if there is already information stored in the database to use.
            foreach (var oAuth in configuration.OAuths)
            {
                oAuth.AccessToken = (await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_{nameof(oAuth.AccessToken)}"))?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
                oAuth.TokenType = await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_{nameof(oAuth.TokenType)}");
                oAuth.RefreshToken = (await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_{nameof(oAuth.RefreshToken)}"))?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
                oAuth.AuthorizationCode = (await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_{nameof(oAuth.AuthorizationCode)}"))?.DecryptWithAes(gclSettings.DefaultEncryptionKey);
                oAuth.AuthorizationCodeMailSent = String.Equals("true", await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_{nameof(oAuth.AuthorizationCodeMailSent)}"), StringComparison.OrdinalIgnoreCase);
                var expireTime = await objectsService.GetSystemObjectValueAsync($"WTS_{oAuth.ApiName}_{nameof(oAuth.ExpireTime)}");

                // Try to parse the DateTime. If it fails, then set it to DateTime.MinValue to prevent errors.
                if (!String.IsNullOrWhiteSpace(expireTime) && DateTime.TryParse(expireTime, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var expireTimeParsed))
                {
                    oAuth.ExpireTime = expireTimeParsed;
                }
                else
                {
                    oAuth.ExpireTime = DateTime.MinValue;
                }

                oAuth.LogSettings ??= configuration.LogSettings;
            }
        }

        /// <inheritdoc />
        public async Task<(OAuthState State, string AuthorizationHeaderValue, JToken ResponseBody, HttpStatusCode ResponseStatusCode)> GetAccessTokenAsync(string apiName, bool retryAfterWrongRefreshToken = true)
        {
            (OAuthState State, string AuthorizationHeaderValue, JToken ResponseBody, HttpStatusCode ResponseStatusCode) result = (OAuthState.NotEnoughInformation, null, null, HttpStatusCode.Unauthorized);
            using var scope = serviceProvider.CreateScope();
            var communicationsService = scope.ServiceProvider.GetRequiredService<ICommunicationsService>();

            var oAuthApi = configuration.OAuths.SingleOrDefault(oAuth => String.Equals(oAuth.ApiName, apiName, StringComparison.OrdinalIgnoreCase));
            if (oAuthApi == null)
            {
                return result;
            }

            // Lock to prevent multiple requests at once.
            await OauthApiLock.WaitAsync();

            // Check if a new access token needs to be requested and request it.
            try
            {
                if (!String.IsNullOrWhiteSpace(oAuthApi.AccessToken) && oAuthApi.ExpireTime > DateTime.UtcNow)
                {
                    result.State = OAuthState.UsingAlreadyExistingToken;
                }
                else
                {
                    var formData = new List<KeyValuePair<string, string>>();

                    // Setup correct authentication.
                    if (String.IsNullOrWhiteSpace(oAuthApi.AccessToken) || String.IsNullOrWhiteSpace(oAuthApi.RefreshToken))
                    {
                        result.State = OAuthState.AuthenticationFailed;
                        if (oAuthApi.OAuthJwt == null)
                        {
                            switch (oAuthApi.GrantType)
                            {
                                case OAuthGrantType.RefreshToken:
                                    formData.Add(new KeyValuePair<string, string>("refresh_token", oAuthApi.RefreshToken));
                                    formData.Add(new KeyValuePair<string, string>("grant_type", "refresh_token"));
                                    break;

                                case OAuthGrantType.AuthCode:
                                    // First build the redirect URL, we need it in both flows.
                                    var redirectUrl = new UriBuilder(oAuthApi.RedirectBaseUri) {Path = "oauth/handle-callback"};
                                    var queryStringBuilder = HttpUtility.ParseQueryString(redirectUrl!.Query);
                                    queryStringBuilder["apiName"] = apiName;
                                    redirectUrl.Query = queryStringBuilder.ToString()!;
                                    var redirectUrlString = redirectUrl.Uri.ToString();

                                    if (String.IsNullOrWhiteSpace(oAuthApi.AuthorizationCode))
                                    {
                                        if (oAuthApi.AuthorizationCodeMailSent)
                                        {
                                            // Mail has already been sent before, need to wait until the user has authenticated.
                                            result.State = OAuthState.WaitingForManualAuthentication;
                                            return result;
                                        }

                                        var authorizationUrl = new UriBuilder(oAuthApi.AuthorizationUrl);
                                        queryStringBuilder = HttpUtility.ParseQueryString(authorizationUrl.Query);
                                        queryStringBuilder["response_type"] = "code";
                                        queryStringBuilder["client_id"] = oAuthApi.ClientId;
                                        queryStringBuilder["state"] = apiName;
                                        queryStringBuilder["scope"] = String.Join(" ", oAuthApi.Scopes);
                                        queryStringBuilder["redirect_uri"] = redirectUrlString;
                                        queryStringBuilder["access_type"] = "offline";
                                        queryStringBuilder["prompt"] = "consent";
                                        authorizationUrl.Query = queryStringBuilder.ToString()!;
                                        await communicationsService.SendEmailAsync(oAuthApi.EmailAddressForAuthentication, "WTS OAuth2.0 Authentication", $"The Wiser Task Scheduler needs a (new) authentication token for the {oAuthApi.ApiName} API. But this requires manual authentication by a person (the first time only). Please authenticate your account by clicking the following link. The WTS will handle the rest afterwards. The link is: {authorizationUrl.Uri}");

                                        oAuthApi.AuthorizationCodeMailSent = true;
                                        await SaveToDatabaseAsync(oAuthApi);

                                        // End the function, as the user needs to authenticate first.
                                        result.State = OAuthState.WaitingForManualAuthentication;
                                        return result;
                                    }

                                    formData.Add(new KeyValuePair<string, string>("code", oAuthApi.AuthorizationCode));
                                    formData.Add(new KeyValuePair<string, string>("client_id", oAuthApi.ClientId));
                                    formData.Add(new KeyValuePair<string, string>("client_secret", oAuthApi.ClientSecret));
                                    formData.Add(new KeyValuePair<string, string>("redirect_uri", redirectUrlString));
                                    formData.Add(new KeyValuePair<string, string>("grant_type", "authorization_code"));
                                    break;

                                case OAuthGrantType.AuthCodeWithPKCE:
                                    throw new NotImplementedException("OAuthGrantType.AuthCodeWithPKCE is not supported yet");

                                case OAuthGrantType.Implicit:
                                    throw new NotImplementedException("OAuthGrantType.Implicit is not supported yet");

                                case OAuthGrantType.PasswordCredentials:
                                    await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Requesting new access token for '{apiName}' using username and password.", LogName);

                                    formData.Add(new KeyValuePair<string, string>("grant_type", "password"));
                                    formData.Add(new KeyValuePair<string, string>("username", oAuthApi.Username));
                                    formData.Add(new KeyValuePair<string, string>("password", oAuthApi.Password));

                                    break;

                                case OAuthGrantType.ClientCredentials:
                                    await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Requesting new access token for '{apiName}' using client credentials.", LogName);

                                    formData.Add(new KeyValuePair<string, string>("grant_type", "client_credentials"));

                                    if (oAuthApi.SendClientCredentialsInBody)
                                    {
                                        formData.Add(new KeyValuePair<string, string>("client_id", oAuthApi.ClientId));
                                        formData.Add(new KeyValuePair<string, string>("client_secret", oAuthApi.ClientSecret));
                                    }

                                    break;
                                case OAuthGrantType.NotSet:
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException(nameof(oAuthApi.GrantType), oAuthApi.GrantType, null);
                            }
                        }
                    }
                    else
                    {
                        await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Requesting new access token for '{apiName}' using refresh token.", LogName);

                        result.State = OAuthState.RefreshTokenFailed;
                        if (oAuthApi.OAuthJwt == null)
                        {
                            formData.Add(new KeyValuePair<string, string>("grant_type", "refresh_token"));
                            formData.Add(new KeyValuePair<string, string>("refresh_token", oAuthApi.RefreshToken));
                        }
                    }

                    string jwtToken = null;
                    if (oAuthApi.OAuthJwt != null)
                    {
                        var claims = new Dictionary<string, object>
                        {
                            { "exp", DateTimeOffset.Now.AddSeconds(oAuthApi.OAuthJwt.ExpirationTime).ToUnixTimeSeconds() },
                            { "iss", oAuthApi.OAuthJwt.Issuer },
                            { "sub", oAuthApi.OAuthJwt.Subject },
                            { "aud", oAuthApi.OAuthJwt.Audience }
                        };

                        // Add the custom claims.
                        foreach (var claim in oAuthApi.OAuthJwt.Claims)
                        {
                            // Ignore the reserved claims.
                            if (claim.Name.InList("exp", "iss", "sub", "aud"))
                            {
                                continue;
                            }

                            // If a data type is specified, then try to convert the value to that type.
                            Type type = null;
                            if (!String.IsNullOrWhiteSpace(claim.DataType))
                            {
                                type = Type.GetType(claim.DataType, false, true) ?? Type.GetType($"System.{claim.DataType}", false, true);
                            }

                            if (type != null)
                            {
                                try
                                {
                                    claims[claim.Name] = Convert.ChangeType(claim.Value, type);
                                }
                                catch (Exception exception)
                                {
                                    // If the conversion fails, then log it and use the string value instead.
                                    await logService.LogWarning(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Failed to convert claim value to specified type. Using string instead. Exception: {exception}", LogName);
                                    claims[claim.Name] = claim.Value;
                                }
                            }
                            else
                            {
                                // No data type specified, so just use the string value.
                                claims[claim.Name] = claim.Value;
                            }
                        }

                        // Load the certificate and create the token.
                        var certificate = new X509Certificate2(oAuthApi.OAuthJwt.CertificateLocation, oAuthApi.OAuthJwt.CertificatePassword);
                        jwtToken = Jose.JWT.Encode(claims, certificate.GetRSAPrivateKey(), Jose.JwsAlgorithm.RS256);
                    }

                    if (oAuthApi.FormKeyValues != null)
                    {
                        foreach (var keyValue in oAuthApi.FormKeyValues)
                        {
                            var value = keyValue.Value;
                            if (value.Equals("[{jwt_token}]"))
                            {
                                value = jwtToken ?? String.Empty;
                            }

                            formData.Add(new KeyValuePair<string, string>(keyValue.Key, value));
                        }
                    }

                    var request = new HttpRequestMessage(HttpMethod.Post, oAuthApi.Endpoint)
                    {
                        Content = new FormUrlEncodedContent(formData)
                    };

                    request.Headers.Add("Accept", "application/json");

                    if (!oAuthApi.SendClientCredentialsInBody && oAuthApi.GrantType == OAuthGrantType.ClientCredentials)
                    {
                        var authString = $"{oAuthApi.ClientId}:{oAuthApi.ClientSecret}";
                        var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(authString);
                        request.Headers.Add("Authorization", $"Basic {Convert.ToBase64String(plainTextBytes)}");
                    }

                    using var client = new HttpClient();
                    var response = await client.SendAsync(request);

                    using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
                    var json = await reader.ReadToEndAsync();
                    var body = JObject.Parse(json);

                    result.ResponseBody = body;
                    result.ResponseStatusCode = response.StatusCode;

                    if (!response.IsSuccessStatusCode)
                    {
                        await logService.LogError(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Failed to get access token for {oAuthApi.ApiName}. Received: {response.StatusCode}\n{json}", LogName);
                    }
                    else
                    {
                        oAuthApi.AccessToken = (string) body["access_token"];
                        oAuthApi.TokenType = (string) body["token_type"];
                        oAuthApi.RefreshToken = (string) body["refresh_token"];

                        if (body["expires_in"]?.Type == JTokenType.Integer)
                        {
                            oAuthApi.ExpireTime = DateTime.UtcNow.AddSeconds((int) body["expires_in"]);
                        }
                        else
                        {
                            oAuthApi.ExpireTime = DateTime.UtcNow.AddSeconds(Convert.ToInt32((string) body["expires_in"]));
                        }

                        oAuthApi.ExpireTime -= oAuthApi.ExpireTimeOffset;

                        await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"A new access token has been retrieved for {oAuthApi.ApiName} and is valid till {oAuthApi.ExpireTime.ToLocalTime()}", LogName);

                        result.State = OAuthState.SuccessfullyRequestedNewToken;
                    }
                }
            }
            finally
            {
                // Release the lock. This is in a finally to be 100% sure that it will always be released. Otherwise the application might freeze.
                OauthApiLock.Release();
            }

            // Finalize the result based on the state.
            switch (result.State)
            {
                case OAuthState.AuthenticationFailed:
                case OAuthState.RefreshTokenFailed when !retryAfterWrongRefreshToken:
                case OAuthState.WaitingForManualAuthentication:
                case OAuthState.NotEnoughInformation:
                    result.AuthorizationHeaderValue = null;
                    break;
                case OAuthState.RefreshTokenFailed:
                    // Retry to get the token with the login credentials if the refresh token was invalid.
                    await RequestWasUnauthorizedAsync(apiName);
                    result = await GetAccessTokenAsync(apiName, false);
                    break;
                case OAuthState.UsingAlreadyExistingToken:
                    result.AuthorizationHeaderValue = $"{oAuthApi.TokenType} {oAuthApi.AccessToken}";
                    break;
                case OAuthState.SuccessfullyRequestedNewToken:
                    // Reset the authorization code if authentication was successful, because it can only be used once.
                    oAuthApi.AuthorizationCode = null;

                    await SaveToDatabaseAsync(oAuthApi);
                    result.AuthorizationHeaderValue = $"{oAuthApi.TokenType} {oAuthApi.AccessToken}";
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(result.State), result.State.ToString(), null);
            }

            return result;
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

            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_{nameof(oAuthApi.AccessToken)}", String.IsNullOrWhiteSpace(oAuthApi.AccessToken) ? "" : oAuthApi.AccessToken.EncryptWithAes(gclSettings.DefaultEncryptionKey), false);
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_{nameof(oAuthApi.TokenType)}", oAuthApi.TokenType, false);
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_{nameof(oAuthApi.RefreshToken)}", String.IsNullOrWhiteSpace(oAuthApi.RefreshToken) ? "" : oAuthApi.RefreshToken.EncryptWithAes(gclSettings.DefaultEncryptionKey), false);
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_{nameof(oAuthApi.ExpireTime)}", oAuthApi.ExpireTime.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture), false);
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_{nameof(oAuthApi.AuthorizationCodeMailSent)}", oAuthApi.AuthorizationCodeMailSent.ToString(), false);
            await objectsService.SetSystemObjectValueAsync($"WTS_{oAuthApi.ApiName}_{nameof(oAuthApi.AuthorizationCode)}", String.IsNullOrWhiteSpace(oAuthApi.AuthorizationCode) ? "" : oAuthApi.AuthorizationCode.EncryptWithAes(gclSettings.DefaultEncryptionKey), false);
        }
    }
}