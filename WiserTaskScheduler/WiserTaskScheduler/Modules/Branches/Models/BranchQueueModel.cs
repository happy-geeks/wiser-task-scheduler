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

        /// <summary>
        /// Gets or sets the ID of the mail template to get information from.
        /// This is for sending notifications about the status of the creation of a new branch.
        /// </summary>
        public ulong CreatedBranchTemplateId { get; set; }

        /// <summary>
        /// Gets or sets the ID of the mail template to get information from.
        /// This is for sending notifications about the status of the merging of a branch to the main branch.
        /// </summary>
        public ulong MergedBranchTemplateId { get; set; }

        /// <summary>
        /// Gets or sets the ID of the mail template to get information from.
        /// This is for sending notifications about the status of the deletion of a branch.
        /// </summary>
        public ulong DeletedBranchTemplateId { get; set; }
    }
}