namespace WiserTaskScheduler.Modules.Queries.Models
{
    public class CharacterEncodingModel
    {
        /// <summary>
        /// Gets or sets the character set to use for the query.
        /// </summary>
        public string CharacterSet { get; set; } = "utf8mb4";

        /// <summary>
        /// Gets or sets the collation to use for the query.
        /// </summary>
        public string Collation { get; set; } = "utf8mb4_general_ci";
    }
}
