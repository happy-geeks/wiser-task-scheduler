﻿using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Core.Helpers
{
    public static class ResultSetHelper
    {
        /// <summary>
        /// Get the correct object based on the key.
        /// </summary>
        /// <typeparam name="T">The type of JToken that needs to be returned.</typeparam>
        /// <param name="key">The key, separated by comma, to the required object.</param>
        /// <param name="rows">The indexes/rows of the array, to be used if for example '[i]' is used in the key.</param>
        /// <param name="usingResultSet">The result set from where to start te search.</param>
        /// <returns></returns>
        public static T GetCorrectObject<T>(string key, List<int> rows, JObject usingResultSet) where T : JToken
        {
            if (key == "")
            {
                return usingResultSet as T;
            }

            var keyParts = key.Split(".");

            // No next step left, return object as requested type.
            if (keyParts.Length == 1)
            {
                if (!key.EndsWith(']'))
                {
                    return (T) usingResultSet[key];
                }

                var arrayKey = key.Substring(0, key.IndexOf('['));
                var indexKey = GetIndex(keyParts, rows);
                return (T) usingResultSet[arrayKey][indexKey];
            }

            var remainingKey = key.Substring(key.IndexOf(".") + 1);

            // Object to step into is not an array.
            if (!keyParts[0].EndsWith("]"))
            {
                return GetCorrectObject<T>(remainingKey, rows, (JObject)usingResultSet[keyParts[0]]);
            }
            
            var index = GetIndex(keyParts, rows);
            return GetCorrectObject<T>(remainingKey, rows, (JObject)((JArray)usingResultSet[keyParts[0].Substring(0, keyParts[0].IndexOf('['))])[index]);
        }

        private static int GetIndex(string[] keyParts, List<int> rows)
        {
            var indexLetter = keyParts[0][keyParts[0].Length - 2];
            var index = 0;
            // If an index letter is used get the correct value based on letter, starting from 'i'.
            if (char.IsLetter(indexLetter))
            {
                index = rows[(int)indexLetter - 105];
            }
            // If a specific value is used for the array index use that instead.
            else
            {
                var indexIdentifier = keyParts[0].Substring(keyParts[0].IndexOf('['));
                index = Int32.Parse(indexIdentifier.Substring(1, indexIdentifier.Length - 2));
            }

            return index;
        }
    }
}
