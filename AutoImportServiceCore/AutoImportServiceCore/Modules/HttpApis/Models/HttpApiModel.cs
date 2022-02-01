﻿using System;
using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using AutoImportServiceCore.Core.Models;

namespace AutoImportServiceCore.Modules.HttpApis.Models
{
    /// <summary>
    /// A model for a HTTP API.
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
        /// Gets or sets if the HTTP API request needs to be requested once or more.
        /// If false the using result set needs to be set to an array.
        /// </summary>
        public bool SingleRequest { get; set; } = true;

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
    }
}