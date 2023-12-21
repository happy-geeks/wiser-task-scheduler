using System;
using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Models;

namespace WiserTaskScheduler.Modules.HttpApis.Models
{
    /// <summary>
    /// A model for an HTTP API.
    /// </summary>
    [XmlType("HttpApi")]
    public class HttpApiModel : ActionModel
    {
        /// <summary>
        /// Gets or sets the full URL for the call.
        /// </summary>
        [Required]
        public string Url { get; set; }

        /// <summary>
        /// Gets or sets the method to use.
        /// </summary>
        [Required]
        public string Method { get; set; }

        /// <summary>
        /// Gets or sets the OAuth service to get the access token from.
        /// </summary>
        public string OAuth { get; set; }

        /// <summary>
        /// Gets or sets if the HTTP API request needs to be requested once or more.
        /// If false the using result set needs to be set to an array.
        /// </summary>
        public bool SingleRequest { get; set; } = true;
        
        /// <summary>
        /// If True the connection will 'ignore' ssl validation errors
        /// </summary>
        public bool IgnoreSSLValidationErrors { get; set; } = false;

        /// <summary>
        /// If set will override the timeout value of the request with the default which is 100 seconds
        /// value is in seconds
        /// </summary>
        public int Timeout { get; set; } = -1;

        /// <summary>
        /// Gets or sets the property of a response from which the next URL is taken.
        /// </summary>
        public string NextUrlProperty { get; set; }

        /// <summary>
        /// Gets or sets additional headers to add before sending the request.
        /// </summary>
        [XmlArray("Headers")]
        [XmlArrayItem(typeof(HeaderModel))]
        public HeaderModel[] Headers { get; set; } = Array.Empty<HeaderModel>();

        /// <summary>
        /// Gets or sets the body to send with the request.
        /// </summary>
        public BodyModel Body { get; set; }

        /// <summary>
        /// Gets or sets the expected content type. This will force the result to be cast to this object.
        /// Set to null or an empty string to use the Content-Type header of the result (if there is one). 
        /// </summary>
        public string ResultContentType { get; set; }
    }
}
