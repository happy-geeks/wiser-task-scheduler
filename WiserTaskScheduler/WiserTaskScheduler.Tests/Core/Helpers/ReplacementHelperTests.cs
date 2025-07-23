using GeeksCoreLibrary.Core.Models;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Tests.Core.Helpers;

[Category("Core")]
public class ReplacementHelperTests
{
    private readonly JObject resultSet = new JObject
    {
        ["Value"] = "Test1",
        ["Body"] = new JObject
        {
            ["Value"] = "Test2",
            ["Array"] = new JArray
            {
                new JValue("Index0"),
                new JValue("Index1"),
                new JValue("Index2")
            },
            ["ObjectArray"] = new JArray
            {
                new JObject
                {
                    ["Value"] = "Test3",
                    ["Array"] = new JArray
                    {
                        new JValue("InnerArrayIndex0"),
                        new JValue("InnerArrayIndex1"),
                        new JValue("InnerArrayIndex2")
                    }
                },
                new JObject
                {
                    ["Value"] = "Test4"
                }
            }
        },
        ["RawBody"] = "<html><body><h1>Test</h1></body></html>"
    };

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.PrepareText_ValidValues_ReturnsCorrectValues_TestCases))]
    public void PrepareText_ValidValues_ReturnsCorrectValues(string originalString, string remainingKey, HashSettingsModel hashSettings, bool insertValues, bool htmlEncode, string expectedValue, List<ParameterKeyModel> expectedParameterKeys, List<KeyValuePair<string, string>> expectedInsertedParameters)
    {
        // Act
        var actual = ReplacementHelper.PrepareText(originalString, resultSet, remainingKey, hashSettings, insertValues, htmlEncode);
        var actualPreparedText = actual.Item2.Aggregate(actual.Item1, (current, parameterKey) => current.Replace(parameterKey.ReplacementKey, parameterKey.Key));
        actualPreparedText = actual.Item3.Aggregate(actualPreparedText, (current, insertedParameter) => current.Replace($"?{insertedParameter.Key}", insertedParameter.Value));

        // Assert
        actual.Item1.Should().NotBeNull("because we are only testing values present in the result set.");
        actualPreparedText.Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
        actual.Item2.Should().NotBeNull("because we are only testing values present in the result set.");
        actual.Item2.Count.Should().Be(expectedParameterKeys.Count, "because the parameter keys should be correct.");

        for (var i = 0; i < actual.Item2.Count; i++)
        {
            actual.Item2[i].Key.Should().Be(expectedParameterKeys[i].Key, "because the parameter key should be the same.");
        }

        actual.Item3.Should().NotBeNull("because we are only testing values present in the result set.");
        actual.Item3.Count.Should().Be(expectedInsertedParameters.Count, "because the inserted parameters should be correct.");

        for (var i = 0; i < actual.Item3.Count; i++)
        {
            actual.Item3[i].Key.Should().StartWith(expectedInsertedParameters[i].Key, "because the inserted parameter key should be the same.");
            actual.Item3[i].Value.Should().Be(expectedInsertedParameters[i].Value, "because the inserted parameter value should be the same.");
        }
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.GetValue_ValidPath_ReturnsCorrectValue_TestCases))]
    public void GetValue_ValidPath_ReturnsCorrectValue(string key, List<int> rows, bool htmlEncode, string expectedValue)
    {
        // Act
        var actual = ReplacementHelper.GetValue(key, rows, resultSet, htmlEncode);

        // Assert
        actual.Should().NotBeNull("because we are only testing values present in the result set.");
        actual.Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.GetValue_InvalidPath_ReturnsDefaultValue_TestCases))]
    public void GetValue_InvalidPath_ReturnsDefaultValue(string key, string expectedValue)
    {
        // Act
        var actual = ReplacementHelper.GetValue(key, null, resultSet, false);

        // Assert
        actual.Should().NotBeNull("because we are only testing values present in the result set.");
        actual.Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.GetValue_InvalidPath_ThrowsException_TestCases))]
    public void GetValue_InvalidPath_ThrowsException(string key, List<int> rows)
    {
        // Act
        var actual = () => ReplacementHelper.GetValue(key, rows, resultSet, false);

        // Assert
        actual.Should().Throw<ResultSetException>("because the path is invalid.");
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    private class TestCases
    {
        public static IEnumerable<TestCaseData> PrepareText_ValidValues_ReturnsCorrectValues_TestCases
        {
            get
            {
                yield return new TestCaseData("", "", null, false, false, "", new List<ParameterKeyModel>(), new List<KeyValuePair<string, string>>());
                yield return new TestCaseData("Hello, World!", "", null, false, false, "Hello, World!", new List<ParameterKeyModel>(), new List<KeyValuePair<string, string>>());
                yield return new TestCaseData("[{Value}]", "", null, false, false, "?Value", new List<ParameterKeyModel> { new() { Key = "Value"} }, new List<KeyValuePair<string, string>>());
                yield return new TestCaseData("[{Value<>}]", "", null, false, false, "Test1", new List<ParameterKeyModel>(), new List<KeyValuePair<string, string>> { new("Value", "Test1") });
                yield return new TestCaseData("[{Value<>}]", "", null, true, false, "Test1", new List<ParameterKeyModel>(), new List<KeyValuePair<string, string>>());
            }
        }

        public static IEnumerable<TestCaseData> GetValue_ValidPath_ReturnsCorrectValue_TestCases
        {
            get
            {
                yield return new TestCaseData("Value", new List<int> { 0, 0 }, false, "Test1");
                yield return new TestCaseData("Body.Value", new List<int> { 0, 0 }, false, "Test2");
                yield return new TestCaseData("Body.Array[0]", new List<int> { 0, 0 }, false, "Index0");
                yield return new TestCaseData("Body.Array[1]", new List<int> { 0, 0 }, false, "Index1");
                yield return new TestCaseData("Body.Array[2]", new List<int> { 0, 0 }, false, "Index2");
                yield return new TestCaseData("Body.Array[i]", new List<int> { 1, 0 }, false, "Index1");
                yield return new TestCaseData("Body.ObjectArray[i].Value", new List<int> { 0, 0 }, false, "Test3");
                yield return new TestCaseData("Body.ObjectArray[i].Array[0]", new List<int> { 0, 0 }, false, "InnerArrayIndex0");
                yield return new TestCaseData("Body.ObjectArray[i].Array[j]", new List<int> { 0, 1 }, false, "InnerArrayIndex1");
                yield return new TestCaseData("RawBody", new List<int> { 0, 0 }, false, "<html><body><h1>Test</h1></body></html>");
                yield return new TestCaseData("RawBody", new List<int> { 0, 0 }, true, "&lt;html&gt;&lt;body&gt;&lt;h1&gt;Test&lt;/h1&gt;&lt;/body&gt;&lt;/html&gt;");
            }
        }

        public static IEnumerable<TestCaseData> GetValue_InvalidPath_ReturnsDefaultValue_TestCases
        {
            get
            {
                yield return new TestCaseData("Invalid?", "");
                yield return new TestCaseData("Invalid?Not found", "Not found");
                yield return new TestCaseData("Body.Array[3]?DBNull", "DBNull");
            }
        }

        public static IEnumerable<TestCaseData> GetValue_InvalidPath_ThrowsException_TestCases
        {
            get
            {
                yield return new TestCaseData("Invalid.Value", new List<int> { 0, 0 });
                yield return new TestCaseData("Body.Invalid.Value", new List<int> { 0, 0 });
                yield return new TestCaseData("Body.Array[3]", new List<int> { 0, 0 });
                yield return new TestCaseData("Body.Array[i]", new List<int> { 3, 0 });
                yield return new TestCaseData("Body.ObjectArray[i].Array[3]", new List<int> { 0, 0 });
                yield return new TestCaseData("Body.ObjectArray[i].Array[j]", new List<int> { 0, 3 });
            }
        }
    }
}