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