using System.Threading.Tasks;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces;

/// <summary>
/// A service to refresh products for the products api.
/// </summary>
public interface IProductsApiUpdateService
{
    /// <summary>
    /// Gets or sets the log settings that the products api update service needs to use.
    /// </summary>
    LogSettings LogSettings { get; set; }

    /// <summary>
    /// Update the api responses of the products.
    /// </summary>
    /// <returns></returns>
    Task UpdateProductsAsync();
}