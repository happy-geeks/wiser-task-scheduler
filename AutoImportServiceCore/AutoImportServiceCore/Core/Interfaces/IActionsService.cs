﻿using System.Collections.Generic;
using System.Threading.Tasks;
using AutoImportServiceCore.Core.Models;

namespace AutoImportServiceCore.Core.Interfaces
{
    /// <summary>
    /// A service for an action.
    /// </summary>
    public interface IActionsService
    {
        /// <summary>
        /// Initialize the service with information from the configuration.
        /// </summary>
        /// <param name="configuration">The configuration the service is based on.</param>
        /// <returns></returns>
        Task Initialize(ConfigurationModel configuration);

        /// <summary>
        /// Execute the action based on the type.
        /// </summary>
        /// <param name="action">The action to execute.</param>
        /// <param name="resultSets">The result sets from previous actions in the same run.</param>
        /// <returns></returns>
        Task<Dictionary<string, SortedDictionary<int, string>>> Execute(ActionModel action, Dictionary<string, Dictionary<string, SortedDictionary<int, string>>> resultSets);
    }
}
