using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Modules.Branches.Models
{
    /// <summary>
    /// A model for processing the branch queue.
    /// </summary>
    [XmlType("BranchQueue")]
    public class BranchQueueModel : ActionModel
    {
        /// <summary>
        /// Gets or sets the username of the user that should be used for creating and deleting branches.
        /// Normal users should not have permissions to create or drop a database, only this user should.
        /// </summary>
        public string UsernameForManagingBranches { get; set; }

        /// <summary>
        /// Gets or sets the password of the user that should be used for creating and deleting branches.
        /// Normal users should not have permissions to create or drop a database, only this user should.
        /// </summary>
        public string PasswordForManagingBranches { get; set; }
    }
}