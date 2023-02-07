using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Helpers;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Enums;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Helpers
{
    public static class ReplacementHelper
    {
        private static readonly List<int> emptyRows = new List<int>() {0, 0};
        public static List<int> EmptyRows => emptyRows;

        /// <summary>
        /// Prepare the given string for usage.
        /// The final string is returned in Item 1.
        /// Replaces parameter placeholders with an easy accessible name and stores that name in a list that is returned in Item 2.
        /// If a parameter is a collection it is replaced by a comma separated value directly.
        /// If a parameter is a single value it is replaced directly.
        /// If <see cref="insertValues"/> is set to false it will be replaced by a parameter '?{key}' and a list of keys and values will be returned in Item 3.
        /// </summary>
        /// <param name="originalString">The string to prepare.</param>
        /// <param name="usingResultSet">The result set that is used.</param>
        /// <param name="remainingKey">The remainder of they key (after the first .) to be used for collections.</param>
        /// <param name="hashAlgorithm">The algorithm to use for hashing.</param>
        /// <param name="hashRepresentation">The representation of hashed values.</param>
        /// <param name="insertValues">Insert values directly if it is a collection or a single value. Otherwise it will be replaced by a parameter '?{key}' and the value will be returned with the key in Item 3.</param>
        /// <param name="htmlEncode">If the values from the result set needs to be HTML encoded.</param>
        /// <returns></returns>
        public static Tuple<string, List<ParameterKey>, List<KeyValuePair<string, string>>> PrepareText(string originalString, JObject usingResultSet, string remainingKey, HashAlgorithms hashAlgorithm, HashRepresentations hashRepresentation, bool insertValues = true, bool htmlEncode = false)
        {
            var result = originalString;
            var parameterKeys = new List<ParameterKey>();
            var insertedParameters = new List<KeyValuePair<string, string>>();

            while (result.Contains("[{") && result.Contains("}]"))
            {
                var startIndex = result.IndexOf("[{") + 2;
                var endIndex = result.IndexOf("}]");

                var key = result.Substring(startIndex, endIndex - startIndex);
                var originalKey = key;
                
                var hashValue = false;
                if (key.Contains('#'))
                {
                    key = key.Replace("#", "");
                    hashValue = true;
                }

                if (key.Contains("[]"))
                {
                    key = key.Replace("[]", "");

                    var values = new List<string>();
                    var lastKeyIndex = key.LastIndexOf('.');
                    var keyToArray = GetKeyToArray(remainingKey, key);

                    var usingResultSetArray = ResultSetHelper.GetCorrectObject<JArray>(keyToArray, emptyRows, usingResultSet);
                    for (var i = 0; i < usingResultSetArray.Count; i++)
                    {
                        values.Add(GetValue(key.Substring(lastKeyIndex + 1), new List<int>() {i}, (JObject) usingResultSetArray[i], htmlEncode));
                    }

                    var value = String.Join(",", values);
                    if (hashValue)
                    {
                        value = HashValue(value, hashAlgorithm, hashRepresentation);
                    }

                    if (insertValues)
                    {
                        result = result.Replace($"[{{{originalKey}}}]", value);
                    }
                    else
                    {
                        var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                        result = result.Replace($"[{{{originalKey}}}]", $"?{parameterName}");
                        insertedParameters.Add(new KeyValuePair<string, string>(parameterName, value));
                    }
                }
                else if (key.Contains("<>"))
                {
                    key = key.Replace("<>", "");
                    var value = GetValue(key, emptyRows, ResultSetHelper.GetCorrectObject<JObject>(remainingKey, emptyRows, usingResultSet), htmlEncode);
                    if (hashValue)
                    {
                        value = HashValue(value, hashAlgorithm, hashRepresentation);
                    }
                    
                    if (insertValues)
                    {
                        result = result.Replace($"[{{{originalKey}}}]", value);
                    }
                    else
                    {
                        var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                        result = result.Replace($"[{{{originalKey}}}]", $"?{parameterName}");
                        insertedParameters.Add(new KeyValuePair<string, string>(parameterName, value));
                    }
                }
                else
                {
                    var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                    result = result.Replace($"[{{{originalKey}}}]", $"?{parameterName}");
                    parameterKeys.Add(new ParameterKey()
                    {
                        Key = key,
                        Hash = hashValue
                    });
                }
            }

            return new Tuple<string, List<ParameterKey>, List<KeyValuePair<string, string>>>(result, parameterKeys, insertedParameters);
        }

        /// <summary>
        /// Get the key to the array for a collection based on the remaining key and key.
        /// </summary>
        /// <param name="remainingKey">The remainder of they key (after the first .) of the using result set to be used for collections.</param>
        /// <param name="key">The key of the parameter.</param>
        /// <returns></returns>
        private static string GetKeyToArray(string remainingKey, string key)
        {
            var lastKeyIndex = key.LastIndexOf('.');

            var keyToArray = new StringBuilder();
            if (!String.IsNullOrWhiteSpace(remainingKey))
            {
                keyToArray.Append(remainingKey);
            }
            
            if (!String.IsNullOrWhiteSpace(remainingKey) && lastKeyIndex > 0)
            {
                keyToArray.Append('.');
            }

            if (lastKeyIndex > 0)
            {
                keyToArray.Append(key.Substring(0, lastKeyIndex));
            }

            return keyToArray.ToString();
        }

        /// <summary>
        /// Replaces the parameters in the given string with the corresponding values of the requested row.
        /// </summary>
        /// <param name="originalString">The string that needs replacements.</param>
        /// <param name="rows">The rows to use the values from.</param>
        /// <param name="parameterKeys">The keys of the parameters to replace.</param>
        /// <param name="usingResultSet">The result set that is used.</param>
        /// <param name="hashAlgorithm">The algorithm to use for hashing.</param>
        /// <param name="hashRepresentation">The representation of hashed values.</param>
        /// <param name="htmlEncode">If the values from the result set needs to be HTML encoded.</param>
        /// <returns></returns>
        public static string ReplaceText(string originalString, List<int> rows, List<ParameterKey> parameterKeys, JObject usingResultSet, HashAlgorithms hashAlgorithm, HashRepresentations hashRepresentation, bool htmlEncode = false)
        {
            if (String.IsNullOrWhiteSpace(originalString) || !parameterKeys.Any())
            {
                return originalString;
            }
            
            var result = originalString;
            
            foreach (var parameterKey in parameterKeys)
            {
                var key = parameterKey.Key;
                var value = GetValue(key, rows, usingResultSet, htmlEncode);

                if (parameterKey.Hash)
                {
                    value = HashValue(value, hashAlgorithm, hashRepresentation);
                }
                
                var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                result = result.Replace($"?{parameterName}", value);
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key">The key to get the value from.</param>
        /// <param name="rows">The rows to get the value from.</param>
        /// <param name="usingResultSet">The result set that is used.</param>
        /// <param name="htmlEncode">If the value from the result set needs to be HTML encoded.</param>
        /// <returns></returns>
        public static string GetValue(string key, List<int> rows, JObject usingResultSet, bool htmlEncode)
        {
            var keySplit = key.Split('?');
            var defaultValue = keySplit.Length == 2 ? keySplit[1] : null;
            
            string value;

            try
            {
                var result = ResultSetHelper.GetCorrectObject<JToken>(keySplit[0], rows, usingResultSet);
                value = result?.GetType() == typeof(JValue) ? (string) result : result?.ToString();

                if (value == null)
                {
                    throw new ResultSetException($"No value was found while processing the key in the result set and no default value is set. The key being processed is '{key}'.");
                }
            }
            catch (Exception)
            {
                if (defaultValue == null)
                {
                    throw;
                }

                value = defaultValue;
            }

            if (htmlEncode)
            {
                value = value.HtmlEncode();
            }

            return value;
        }

        /// <summary>
        /// Hash a value given a specific algorithm and return it in the given representation.
        /// </summary>
        /// <param name="value">The value to hash.</param>
        /// <param name="algorithm">The algorithm to use for hashing.</param>
        /// <param name="representation">The representation of the bytes to return.</param>
        /// <returns>Returns the value hashed with the algorithm converted to the given representation.</returns>
        public static string HashValue(string value, HashAlgorithms algorithm, HashRepresentations representation)
        {
            HashAlgorithm hashAlgorithm;
            
            switch (algorithm)
            {
                case HashAlgorithms.MD5:
                    hashAlgorithm = MD5.Create();
                    break;
                case HashAlgorithms.SHA256:
                    hashAlgorithm = SHA256.Create();
                    break;
                case HashAlgorithms.SHA384:
                    hashAlgorithm = SHA384.Create();
                    break;
                case HashAlgorithms.SHA512:
                    hashAlgorithm = SHA512.Create();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(algorithm), algorithm, null);
            }
            
            var bytes = Encoding.ASCII.GetBytes(value);
            var hashBytes = hashAlgorithm.ComputeHash(bytes);
            
            hashAlgorithm.Dispose();

            switch (representation)
            {
                case HashRepresentations.Base64:
                    return Convert.ToBase64String(hashBytes);
                case HashRepresentations.Hex:
                    return Convert.ToHexString(hashBytes);
                default:
                    throw new ArgumentOutOfRangeException(nameof(representation), representation, null);
            }
        }
    }
}
