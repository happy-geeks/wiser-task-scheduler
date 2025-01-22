using GeeksCoreLibrary.Modules.Branches.Models;
using MySqlConnector;

namespace WiserTaskScheduler.Modules.Branches.Interfaces;

/// <summary>
/// A service for handling the queue of branches, for creating and merging branches.
/// </summary>
public interface IBranchQueueService
{
    /// <summary>
    /// Gets the connection string for the branch based on the encrypted information in the <see cref="BranchActionBaseModel"/>.
    /// </summary>
    /// <param name="branchActionBaseModel">The object containing the information to be used to build the connection string.</param>
    /// <param name="database">The name of the database to connect to.</param>
    /// <param name="allowLoadLocalInfile">Whether to allow the use of the "local_infile" setting in the MySQL server.</param>
    /// <returns>Returns a <see cref="MySqlConnectionStringBuilder"/> with the given information.</returns>
    MySqlConnectionStringBuilder GetConnectionStringBuilderForBranch(BranchActionBaseModel branchActionBaseModel, string database, bool allowLoadLocalInfile = false);
}