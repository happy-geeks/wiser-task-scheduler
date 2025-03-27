using DocumentFormat.OpenXml.Office2013.PowerPoint.Roaming;
using FluentAssertions;
using GeeksCoreLibrary.Core.Models;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Tests.Core.Helpers;

public class ReplacementHelperTests
{
    private static IEnumerable<TestCaseData> TestCases
    {
        get
        {
            yield return new TestCaseData("Value", "Test1", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.Value", "Test2", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.Array[0]", "Index0", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.Array[1]", "Index1", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.Array[2]", "Index2", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.Array[i]", "Index1", new List<int> { 1, 0 }, false);
            yield return new TestCaseData("Body.ObjectArray[i].Value", "Test3", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.ObjectArray[i].Array[0]", "InnerArrayIndex0", new List<int> { 0, 0 }, false);
            yield return new TestCaseData("Body.ObjectArray[i].Array[j]", "InnerArrayIndex1", new List<int> { 0, 1 }, false);
            yield return new TestCaseData("RawBody", "&lt;html&gt;&lt;body&gt;&lt;h1&gt;Test&lt;/h1&gt;&lt;/body&gt;&lt;/html&gt;", new List<int> { 0, 0 }, true);
        }
    }

    private JObject resultSet;

    [OneTimeSetUp]
    public void Setup()
    {
        resultSet = new JObject
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
    }

    [TestCaseSource(nameof(TestCases))]
    public void GetValue_ValidPath_ReturnsCorrectValue(string key, string expectedValue, List<int> rows, bool htmlEncode)
    {
        // Act
        var actual = ReplacementHelper.GetValue(key, rows, resultSet, htmlEncode);

        // Assert
        actual.Should().NotBeNull("because we are only testing values present in the result set.");
        actual.Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCase("Invalid?", "")]
    [TestCase("Invalid?Not found", "Not found")]
    [TestCase("Body.Array[3]?DBNull", "DBNull")]
    public void GetValue_InvalidPath_ReturnsDefaultValue(string key, string expectedValue)
    {
        // Act
        var actual = ReplacementHelper.GetValue(key, null, resultSet, false);

        // Assert
        actual.Should().NotBeNull("because we are only testing values present in the result set.");
        actual.Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCase("Invalid.Value")]
    [TestCase("Body.Invalid.Value")]
    [TestCase("Body.Array[3]")]
    [TestCase("Body.Array[i]", 3, 0)]
    [TestCase("Body.ObjectArray[i].Array[3]", 0, 3)]
    [TestCase("Body.ObjectArray[i].Array[j]", 0, 3)]
    public void GetValue_InvalidPath_ThrowsException(string key, int indexI = 0, int indexJ = 0)
    {
        // Act
        var actual = () => ReplacementHelper.GetValue(key, null, resultSet, false);

        // Assert
        actual.Should().Throw<ResultSetException>("because the path is invalid.");
    }
}