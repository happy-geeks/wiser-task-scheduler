using System.Threading.Tasks;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces
{
    /// <summary>
    /// A service to perform updates to parents whose child items got updated.
    /// </summary>
    public interface IParentUpdateService
    {
        /// <summary>
        /// Gets or sets the log settings that the Parent Update service needs to use.
        /// </summary>
        LogSettings LogSettings { get; set; }

        /// <summary>
        /// checks the parent update table and apply updates where needed then clear it
        /// </summary>
        /// <returns></returns>
        Task ParentsUpdateAsync();
    }
}
