using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Body.Interfaces;
using WiserTaskScheduler.Modules.Body.Models;

namespace WiserTaskScheduler.Modules.Body.Services
{
    /// <summary>
    /// A service to prepare bodies.
    /// </summary>
    public class BodyService : IBodyService, IScopedService
    {
        private readonly IServiceProvider serviceProvider;

        public BodyService(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        /// <inheritdoc />
        public string GenerateBody(BodyModel bodyModel, List<int> rows, JObject resultSets, HashSettingsModel hashSettings, int forcedIndex = -1)
        {
            var finalBody = new StringBuilder();

            // Create a scope to get the string replacements service. It's needed to evaluate logic snippets.
            using var scope = serviceProvider.CreateScope();
            var stringReplacementsService = scope.ServiceProvider.GetRequiredService<IStringReplacementsService>();

            foreach (var bodyPart in bodyModel.BodyParts)
            {
                var body = bodyPart.Text;

                // If the part needs a result set, apply it.
                if (!String.IsNullOrWhiteSpace(bodyPart.UseResultSet))
                {
                    var keyParts = bodyPart.UseResultSet.Split('.');
                    var remainingKey = keyParts.Length > 1 ? bodyPart.UseResultSet.Substring(keyParts[0].Length + 1) : "";
                    var tuple = ReplacementHelper.PrepareText(bodyPart.Text, (JObject)resultSets[keyParts[0]], remainingKey, hashSettings);
                    body = tuple.Item1;
                    var parameterKeys = tuple.Item2;

                    if (parameterKeys.Count > 0)
                    {
                        // Replace body with values from first row.
                        if (bodyPart.SingleItem)
                        {
                            body = ReplacementHelper.ReplaceText(body, rows, parameterKeys, ResultSetHelper.GetCorrectObject<JObject>(bodyPart.UseResultSet, rows, resultSets), hashSettings);
                        }
                        // Replace and combine body with values for each row.
                        else
                        {
                            body = GenerateBodyCollection(body, bodyModel.ContentType, parameterKeys, ResultSetHelper.GetCorrectObject<JArray>(bodyPart.UseResultSet, rows, resultSets), hashSettings, bodyPart.ForceIndex ? forcedIndex : -1);
                        }
                    }
                }

                if (bodyPart.EvaluateLogicSnippets)
                {
                    // Evaluate [if]...[endif] snippets in the body.
                    body = stringReplacementsService.EvaluateTemplate(body);
                }

                finalBody.Append(body);
            }

            return finalBody.ToString();
        }

        /// <summary>
        /// Replace values in the body for each row and return the combined result.
        /// </summary>
        /// <param name="body">The body text to use for each row.</param>
        /// <param name="contentType">The content type that is being send in the request.</param>
        /// <param name="parameterKeys">The keys of the parameters that need to be replaced.</param>
        /// <param name="usingResultSet">The result set to get the values from.</param>
        /// <param name="hashSettings">The settings to use for hashing.</param>
        /// <param name="forcedIndex">The index a body part uses if it is set to use the forced index.</param>
        /// <returns></returns>
        private string GenerateBodyCollection(string body, string contentType, List<ParameterKeyModel> parameterKeys, JArray usingResultSet, HashSettingsModel hashSettings, int forcedIndex)
        {
            var separator = String.Empty;

            // Add a separator between each row result based on content type.
            switch (contentType)
            {
                case "application/json":
                    separator = ",";
                    break;
            }

            var bodyCollection = new StringBuilder();

            var rows = new List<int> { 0, 0 };
            var keyWithSecondLayer = parameterKeys.FirstOrDefault(parameterKey => parameterKey.Key.Contains("[j]"))?.Key;

            var startIndex = forcedIndex >= 0 ? forcedIndex : 0;
            var endIndex = forcedIndex >= 0 ? forcedIndex + 1 : usingResultSet.Count;

            // Perform the query for each row in the result set that is being used.
            for (var i = startIndex; i < endIndex; i++)
            {
                rows[0] = i;

                if (keyWithSecondLayer == null)
                {
                    var bodyWithValues = ReplacementHelper.ReplaceText(body, rows, parameterKeys, (JObject) usingResultSet[i], hashSettings);
                    bodyCollection.Append($"{(i > 0 ? separator : "")}{bodyWithValues}");
                    continue;
                }

                var secondLayerArray = ResultSetHelper.GetCorrectObject<JArray>($"{keyWithSecondLayer.Substring(0, keyWithSecondLayer.IndexOf("[j]"))}", rows, (JObject) usingResultSet[i]);

                for (var j = 0; j < secondLayerArray.Count; j++)
                {
                    rows[1] = j;
                    var bodyWithValues = ReplacementHelper.ReplaceText(body, rows, parameterKeys, (JObject)usingResultSet[i], hashSettings);
                    bodyCollection.Append($"{(i > startIndex || j > 0 ? separator : "")}{bodyWithValues}");
                }
            }

            // Add collection syntax based on content type.
            switch (contentType)
            {
                case "application/json":
                    return $"[{bodyCollection}]";
                default:
                    return bodyCollection.ToString();
            }
        }
    }
}
