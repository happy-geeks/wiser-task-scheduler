using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Wiser.Models;
using WiserTaskScheduler.Core.Models.ProductsApiUpdater;
using WiserTaskScheduler.Modules.Wiser.Interfaces;
namespace WiserTaskScheduler.Core.Services;

/// <summary>
/// A service to manage performing updates to the products api.
/// </summary>
public class ProductsApiUpdateService(
    IOptions<WtsSettings> wtsSettings,
    IServiceProvider serviceProvider,
    ILogService logService,
    ILogger<ProductsApiUpdateService> logger,
    IWiserService wiserService
) : IProductsApiUpdateService, ISingletonService
{
    private readonly string logName = $"ProductsApiUpdateService ({Environment.MachineName} - {wtsSettings.Value.Name})";

    private readonly ProductsApiUpdateServiceSettings productsApiUpdateServiceSettings = wtsSettings.Value.ProductsApiUpdateService;
    private readonly WiserSettings wiserSettings = wtsSettings.Value.Wiser;

    /// <inheritdoc />
    public LogSettings LogSettings { get; set; }

    /// <inheritdoc />
    public async Task UpdateProductsAsync()
    {
        if (!productsApiUpdateServiceSettings.Enabled)
        {
            // if the service is not enabled, we don't need to do anything.
            return;
        }

        using var scope = serviceProvider.CreateScope();
        await using var databaseConnection = scope.ServiceProvider.GetRequiredService<IDatabaseConnection>();

        var outOfDateCount = await GetOutOfDateCount();
        if (outOfDateCount > 0)
        {
           await RequestProductsApiUpdateAsync();
        }
    }

    /// <summary>
    /// retrieves the count of out of date products from the Wiser API.
    /// </summary>
    private async Task<int> GetOutOfDateCount()
    {
        try
        {
            var accessToken = await wiserService.GetAccessTokenAsync();
            var wiserApiBaseUrl = $"{wiserSettings.WiserApiUrl}{(wiserSettings.WiserApiUrl.EndsWith('/') ? "" : "/")}api/v3/";
            var outdatedProductsUrl = $"{wiserApiBaseUrl}products/count-outdated-products";

            var client = new HttpClient();
            var request = new HttpRequestMessage(HttpMethod.Get, outdatedProductsUrl);
            request.Headers.Add(HttpRequestHeader.Authorization.ToString(), $"Bearer {accessToken}");
            var response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                await logService.LogError(logger, LogScopes.RunBody, LogSettings,
                    $"Failed to retrieve out of date products, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'.","productsApiUpdateService");
            }

            using var reader = new StreamReader(await response.Content.ReadAsStreamAsync());
            var body = await reader.ReadToEndAsync();

            if (string.IsNullOrWhiteSpace(body))
            {
                await logService.LogError(logger, LogScopes.RunBody, LogSettings, "Failed to retrieve out of date products, response body is empty.", logName);
                return 0;
            }

            var jsonObject = System.Text.Json.JsonDocument.Parse(body);

            if (jsonObject.RootElement.TryGetProperty("count", out var countElement) && countElement.ValueKind == System.Text.Json.JsonValueKind.Number)
            {
                return countElement.GetInt32();
            }

            await logService.LogError(logger, LogScopes.RunBody, LogSettings, "Failed to retrieve out of date products, response body has no count.", logName);
            return 0;
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during GetOutOfDateCount products api: {exception}", logName);
        }

        return 0;
    }

    /// <summary>
    /// Requests an update of the products API by calling the Wiser API.
    /// </summary>
    private async Task RequestProductsApiUpdateAsync()
    {
        try
        {
            var accessToken = await wiserService.GetAccessTokenAsync();
            var wiserApiBaseUrl = $"{wiserSettings.WiserApiUrl}{(wiserSettings.WiserApiUrl.EndsWith('/') ? "" : "/")}api/v3/";
            var updateProductsUrl = $"{wiserApiBaseUrl}products/refresh-all";

            var client = new HttpClient();
            var request = new HttpRequestMessage(HttpMethod.Post, updateProductsUrl);
            request.Headers.Add(HttpRequestHeader.Authorization.ToString(), $"Bearer {accessToken}");

            var response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                await logService.LogError(logger, LogScopes.RunBody, LogSettings,
                    $"Failed to update products, server returned status '{response.StatusCode}' with reason '{response.ReasonPhrase}'.","productsApiUpdateService");
            }
        }
        catch (Exception exception)
        {
            await logService.LogError(logger, LogScopes.RunStartAndStop, LogSettings, $"an exception occured during product api update: {exception}", logName);
        }
    }
}