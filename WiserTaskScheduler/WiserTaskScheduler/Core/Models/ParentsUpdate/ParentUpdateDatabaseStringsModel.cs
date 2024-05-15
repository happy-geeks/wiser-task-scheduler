using System.Xml.Serialization;

namespace WiserTaskScheduler.Core.Models.ParentsUpdate
{
    [XmlType("ParentUpdateDatabaseStrings")]
    public class ParentUpdateDatabaseStrings(string databaseName, string listQuery, string cleanupQuery, string optimizeQuery)
    {
        /// <summary>
        /// Gets or sets the database name.
        /// </summary>
        public string DatabaseName { get; set; } = databaseName;

        /// <summary>
        /// Gets or sets the list table query used to list all targeted tables in the parent update table.
        /// </summary>
        public string ListTableQuery { get; set; } = listQuery;


        /// <summary>
        /// Gets or sets cleanup query used to clear the table of updates after its done.
        /// </summary>
        public string CleanUpQuery { get; set; } = cleanupQuery;
        
        /// <summary>
        /// Gets or sets optimize query used to optimize the table 
        /// </summary>
        public string OptimizeQuery { get; set; } = optimizeQuery;
    }
}