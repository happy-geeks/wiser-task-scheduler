using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.ProductsApiUpdate.Interfaces;


/// <summary>
/// A service to perform updates to parents whose child items got updated.
/// </summary>
public interface IProductsApiUpdateService
{
    /*
     /// <summary>
    /// Gets or sets the log settings that the Parent Update service needs to use.
    /// </summary>
    LogSettings LogSettings { get; set; }
*/
    /// <summary>
    /// Checks the parent update table and applies updates where needed then clear it.
    /// </summary>
    /// <returns></returns>
    Task<JObject> Execute(ActionModel action, JObject resultSets, string configurationServiceName);

}