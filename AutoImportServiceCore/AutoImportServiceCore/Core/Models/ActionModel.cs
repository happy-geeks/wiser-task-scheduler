﻿using System;
using System.ComponentModel.DataAnnotations;

namespace AutoImportServiceCore.Core.Models
{
    /// <summary>
    /// An base for any action that the AIS can perform.
    /// </summary>
    public abstract class ActionModel
    {
        /// <summary>
        /// Gets or sets the time id of the run scheme this action needs to be executed in.
        /// </summary>
        [Required]
        public int TimeId { get; set; }

        /// <summary>
        /// Gets or sets the in what order this action needs to be executed.
        /// </summary>
        [Required]
        public int Order { get; set; }

        /// <summary>
        /// Gets or sets the name of the result set.
        /// </summary>
        public string ResultSetName { get; set; } = String.Empty;

        /// <summary>
        /// Gets or sets the result set to use for replacements.
        /// </summary>
        public string UseResultSet { get; set; } = String.Empty;

        /// <summary>
        /// Gets or sets which result set needs to be a specific value.
        /// Result set name and required status code, separated by a comma (,).
        /// </summary>
        public string OnlyWithStatusCode { get; set; }

        /// <summary>
        /// Gets or sets the log settings that apply to the action.
        /// </summary>
        public LogSettings LogSettings { get; set; }
    }
}
