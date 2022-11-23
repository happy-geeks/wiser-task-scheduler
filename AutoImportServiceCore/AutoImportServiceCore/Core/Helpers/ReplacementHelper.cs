using System;
using System.Collections.Generic;
using System.Text;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Helpers;
using Newtonsoft.Json.Linq;

namespace AutoImportServiceCore.Core.Helpers
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
        /// <param name="insertValues">Insert values directly if it is a collection or a single value. Otherwise it will be replaced by a parameter '?{key}' and the value will be returned with the key in Item 3.</param>
        /// <param name="htmlEncode">If the values from the result set needs to be HTML encoded.</param>
        /// <returns></returns>
        public static Tuple<string, List<string>, List<KeyValuePair<string, string>>> PrepareText(string originalString, JObject usingResultSet, string remainingKey, bool insertValues = true, bool htmlEncode = false)
        {
            var result = originalString;
            var parameterKeys = new List<string>();
            var insertedParameters = new List<KeyValuePair<string, string>>();

            while (result.Contains("[{") && result.Contains("}]"))
            {
                var startIndex = result.IndexOf("[{") + 2;
                var endIndex = result.IndexOf("}]");

                var key = result.Substring(startIndex, endIndex - startIndex);

                if (key.Contains("[]"))
                {
                    key = key.Replace("[]", "");

                    var values = new List<string>();
                    var lastKeyIndex = key.LastIndexOf('.');
                    var keyToArray = GetKeyToArray(remainingKey, key);

                    var usingResultSetArray = ResultSetHelper.GetCorrectObject<JArray>(keyToArray.ToString(), emptyRows, usingResultSet);
                    for (var i = 0; i < usingResultSetArray.Count; i++)
                    {
                        values.Add(GetValue(key.Substring(lastKeyIndex + 1), new List<int>() {i}, (JObject) usingResultSetArray[i], htmlEncode));
                    }

                    if (insertValues)
                    {
                        result = result.Replace($"[{{{key}[]}}]", String.Join(",", values));
                    }
                    else
                    {
                        var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                        result = result.Replace($"[{{{key}[]}}]", $"?{parameterName}");
                        insertedParameters.Add(new KeyValuePair<string, string>(parameterName, String.Join(',', values)));
                    }
                }
                else if (key.Contains("<>"))
                {
                    key = key.Replace("<>", "");
                    var value = GetValue(key, emptyRows, ResultSetHelper.GetCorrectObject<JObject>(remainingKey, emptyRows, usingResultSet), htmlEncode);
                    if (insertValues)
                    {
                        result = result.Replace($"[{{{key}<>}}]", value);
                    }
                    else
                    {
                        var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                        result = result.Replace($"[{{{key}<>}}]", $"?{parameterName}");
                        insertedParameters.Add(new KeyValuePair<string, string>(parameterName, value));
                    }
                }
                else
                {
                    var value = GetValue(key, emptyRows, usingResultSet, htmlEncode) ?? "";
                    if (insertValues)
                    {
                        result = result.Replace($"[{{{key}}}]", value);
                    }
                    else
                    {
                        var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                        result = result.Replace($"[{{{key}}}]", $"?{parameterName}");
                        insertedParameters.Add(new KeyValuePair<string, string>(parameterName, value));
                    }
                    
                    parameterKeys.Add(key);
                }
            }

            return new Tuple<string, List<string>, List<KeyValuePair<string, string>>>(result, parameterKeys, insertedParameters);
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
                keyToArray.Append(".");
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
        /// <param name="htmlEncode">If the values from the result set needs to be HTML encoded.</param>
        /// <returns></returns>
        public static string ReplaceText(string originalString, List<int> rows, List<string> parameterKeys, JObject usingResultSet, bool htmlEncode = false)
        {
            var result = originalString;
            
            foreach (var key in parameterKeys)
            {
                var parameterName = DatabaseHelpers.CreateValidParameterName(key);
                result = result.Replace($"?{parameterName}", GetValue(key, rows, usingResultSet, htmlEncode));
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
    }
}
