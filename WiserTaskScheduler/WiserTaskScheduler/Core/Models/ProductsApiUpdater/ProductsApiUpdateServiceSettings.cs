using System;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.RunSchemes.Enums;
using WiserTaskScheduler.Modules.RunSchemes.Models;

namespace WiserTaskScheduler.Core.Models.ProductsApiUpdater;

public class ProductsApiUpdateServiceSettings
{
    /// <summary>
    ///  Gets or sets a value indicating whether the <see cref="ProductsApiUpdateWorker"/> is enabled.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Gets or Sets the url of the products api.
    /// </summary>
    public string ProductsApiUrl { get; set; } = "https://wiserapi.configuratoren.nl/api/v3/products";

    /// <summary>
    /// Gets or sets the run scheme for the <see cref="ProductsApiUpdateWorker"/>.
    /// </summary>
    public RunSchemeModel RunScheme { get; set; } = new()
    {
        Type = RunSchemeTypes.Continuous,
        Delay = TimeSpan.FromMinutes(5),
        StartTime = TimeSpan.FromHours(4),
        StopTime = TimeSpan.FromMinutes((5 * 60) + 30)
    };
}