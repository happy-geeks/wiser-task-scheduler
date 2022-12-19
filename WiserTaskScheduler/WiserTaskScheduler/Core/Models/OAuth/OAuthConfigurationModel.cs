using System.Xml.Serialization;

namespace WiserTaskScheduler.Core.Models.OAuth
{
    [XmlRoot("OAuthConfiguration")]
    public class OAuthConfigurationModel
    {
        /// <summary>
        /// Gets or sets the connection string that is used for queries.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the log settings.
        /// </summary>
        public LogSettings LogSettings { get; set; }

        /// <summary>
        /// Gets or sets the OAuths.
        /// </summary>
        [XmlArray("OAuths")]
        [XmlArrayItem(typeof(OAuthModel))]
        public OAuthModel[] OAuths { get; set; }
    }
}
