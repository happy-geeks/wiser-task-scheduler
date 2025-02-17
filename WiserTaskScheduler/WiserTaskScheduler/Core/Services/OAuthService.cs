using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Communication.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using Jose;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models.OAuth;

namespace WiserTaskScheduler.Core.Services;

public class OAuthService(IOptions<GclSettings> gclSettings, ILogService logService, ILogger<OAuthService> logger, IServiceProvider serviceProvider) : IOAuthService, ISingletonService
{
    private const string LogName = "OAuthService";

    private readonly GclSettings gclSettings = gclSettings.Value;

    private OAuthConfigurationModel configuration;

    // Semaphore is a locking system that can be used with async code.
    private static readonly SemaphoreSlim OauthApiLock = new(1, 1);

    /// <inheritdoc />
    public async Task SetConfigurationAsync(OAuthConfigurationModel oAuthConfigurationModel)
    {
        configuration = oAuthConfigurationModel;

        if (configuration.OAuths == null || configuration.OAuths.Length == 0)
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
                // We have a token and it's still valid, so just use that one.
                result.State = OAuthState.UsingAlreadyExistingToken;
            }
            else
            {
                string jwtToken = null;
                var formData = new List<KeyValuePair<string, string>>();

                // We either have no token yet, or it's expired, so we need to get a new one.
                result.State = OAuthState.AuthenticationFailed;
                if (oAuthApi.OAuthJwt == null)
                {
                    // If we have a refresh token, then we can always use that to get a new access token.
                    if (!String.IsNullOrWhiteSpace(oAuthApi.RefreshToken))
                    {
                        oAuthApi.GrantType = OAuthGrantType.RefreshToken;
                    }

                    switch (oAuthApi.GrantType)
                    {
                        case OAuthGrantType.RefreshToken:
                            await logService.LogInformation(logger, LogScopes.RunBody, oAuthApi.LogSettings, $"Requesting new access token for '{apiName}' using refresh token.", LogName);

                            result.State = OAuthState.RefreshTokenFailed;
                            formData.Add(new KeyValuePair<string, string>("grant_type", "refresh_token"));
                            formData.Add(new KeyValuePair<string, string>("refresh_token", oAuthApi.RefreshToken));
                            break;

                        case OAuthGrantType.AuthCode:
                            result = await SetupAuthorizationCodeAuthenticationAsync(result, oAuthApi, communicationsService, formData);
                            if (result.State == OAuthState.WaitingForManualAuthentication)
                            {
                                // If we're waiting for manual authentication, then we should stop the process here, because we can't continue without the authorization code.
                                return result;
                            }

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
                else
                {
                    var claims = new Dictionary<string, object>
                    {
                        {"exp", DateTimeOffset.Now.AddSeconds(oAuthApi.OAuthJwt.ExpirationTime).ToUnixTimeSeconds()},
                        {"iss", oAuthApi.OAuthJwt.Issuer},
                        {"sub", oAuthApi.OAuthJwt.Subject},
                        {"aud", oAuthApi.OAuthJwt.Audience}
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
                    var certificate = X509CertificateLoader.LoadPkcs12FromFile(oAuthApi.OAuthJwt.CertificateLocation, oAuthApi.OAuthJwt.CertificatePassword);
                    jwtToken = JWT.Encode(claims, certificate.GetRSAPrivateKey(), JwsAlgorithm.RS256);
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

                if (!oAuthApi.SendClientCredentialsInBody && !String.IsNullOrWhiteSpace(oAuthApi.ClientId) && !String.IsNullOrWhiteSpace(oAuthApi.ClientSecret) && oAuthApi.GrantType is OAuthGrantType.ClientCredentials or OAuthGrantType.RefreshToken)
                {
                    var authString = $"{oAuthApi.ClientId}:{oAuthApi.ClientSecret}";
                    var plainTextBytes = Encoding.UTF8.GetBytes(authString);
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

                    // Not all APIs return a new refresh token after using it, so only update it if it's present.
                    var newRefreshToken = (string) body["refresh_token"];
                    if (!String.IsNullOrWhiteSpace(newRefreshToken))
                    {
                        oAuthApi.RefreshToken = newRefreshToken;
                    }

                    oAuthApi.ExpireTime = body["expires_in"]?.Type == JTokenType.Integer
                        ? DateTime.UtcNow.AddSeconds((int) body["expires_in"])
                        : DateTime.UtcNow.AddSeconds(Convert.ToInt32((string) body["expires_in"]));

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

    /// <summary>
    /// Sets up / prepares the authentication for the authorization code flow.
    /// If we don't have an authorization code, then we need to email the user to authenticate.
    /// If we do have an authorization code, then we can use that to get the access token.
    /// </summary>
    /// <param name="result">The output of the <see cref="GetAccessTokenAsync"/> method.</param>
    /// <param name="oAuthApi">The <see cref="OAuthModel"/> with the settings for the OAUTH2 authentication.</param>
    /// <param name="communicationsService">The <see cref="ICommunicationsService"/> for sending e-mails.</param>
    /// <param name="formData">The form data for the get token request for the OAUTH2 authentication.</param>
    /// <returns>The current state of the OAUTH2 authentication. Please note that if this returns <see cref="OAuthState.WaitingForManualAuthentication"/>, then you should stop the <see cref="GetAccessTokenAsync"/> method from doing anything else by directly returning the result of this method.</returns>
    private async Task<(OAuthState State, string AuthorizationHeaderValue, JToken ResponseBody, HttpStatusCode ResponseStatusCode)> SetupAuthorizationCodeAuthenticationAsync((OAuthState State, string AuthorizationHeaderValue, JToken ResponseBody, HttpStatusCode ResponseStatusCode) result, OAuthModel oAuthApi, ICommunicationsService communicationsService, List<KeyValuePair<string, string>> formData)
    {
        // First build the redirect URL, we need it in both flows.
        var redirectUrl = new UriBuilder(oAuthApi.RedirectBaseUri) {Path = "oauth/handle-callback"};
        var queryStringBuilder = HttpUtility.ParseQueryString(redirectUrl!.Query);
        queryStringBuilder["apiName"] = oAuthApi.ApiName;
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
            queryStringBuilder["state"] = oAuthApi.ApiName;
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
        return result;
    }
}