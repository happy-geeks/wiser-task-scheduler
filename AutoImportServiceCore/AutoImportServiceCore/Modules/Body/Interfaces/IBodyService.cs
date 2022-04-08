﻿using System.Collections.Generic;
using AutoImportServiceCore.Modules.Body.Models;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Modules.Body.Interfaces
{
    /// <summary>
    /// A service to prepare bodies.
    /// </summary>
    public interface IBodyService
    {
        /// <summary>
        /// Generate the body based on a <see cref="BodyModel"/>.
        /// </summary>
        /// <param name="bodyModel">The <see cref="BodyModel"/> to generate the body from.</param>
        /// <param name="rows">The indexes/rows of the array, passed to be used if '[i]' is used in the key.</param>
        /// <param name="resultSets">The result sets to use to generate the body.</param>
        /// <returns>Returns the generated body.</returns>
        string GenerateBody(BodyModel bodyModel, List<int> rows, JObject resultSets);
    }
}
