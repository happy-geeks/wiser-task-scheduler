using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Tests.Core.Helpers;

[Category("Core")]
public class ResultSetHelperTests
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
    [TestCaseSource(typeof(TestCases), nameof(TestCases.GetCorrectObject_ValidPath_ReturnsCorrectObject_TestCases))]
    public void GetCorrectObject_ValidPath_ReturnsCorrectObject(string key, List<int> rows, string expectedValue)
    {
        // Act
        var actual = ResultSetHelper.GetCorrectObject<JValue>(key, rows, resultSet);

        // Assert
        actual.Should<JValue>().NotBeNull("because we are only testing values present in the result set.");
        ((string)actual!).Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.GetCorrectObject_InvalidPath_ReturnsNull_TestCases))]
    public void GetCorrectObject_InvalidPath_ReturnsNull(string key)
    {
        // Act
        var actual = ResultSetHelper.GetCorrectObject<JValue>(key, null, resultSet);

        // Assert
        actual.Should<JValue>().BeNull("because the path is invalid.");
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.GetCorrectObject_InvalidPath_ThrowsResultSetException_TestCases))]
    public void GetCorrectObject_InvalidPath_ThrowsResultSetException(string key, List<int> rows)
    {
        // Act
        var actual = () => ResultSetHelper.GetCorrectObject<JValue>(key, rows, resultSet);

        // Assert
        actual.Should().Throw<ResultSetException>("because the path is invalid.");
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    private class TestCases
    {
        public static IEnumerable<TestCaseData> GetCorrectObject_ValidPath_ReturnsCorrectObject_TestCases
        {
            get
            {
                yield return new TestCaseData("Value", new List<int> { 0, 0 }, "Test1");
                yield return new TestCaseData("Body.Value", new List<int> { 0, 0 }, "Test2");
                yield return new TestCaseData("Body.Array[0]", new List<int> { 0, 0 }, "Index0");
                yield return new TestCaseData("Body.Array[1]", new List<int> { 0, 0 }, "Index1");
                yield return new TestCaseData("Body.Array[2]", new List<int> { 0, 0 }, "Index2");
                yield return new TestCaseData("Body.Array[i]", new List<int> { 1, 0 }, "Index1");
                yield return new TestCaseData("Body.ObjectArray[i].Value", new List<int> { 0, 0 }, "Test3");
                yield return new TestCaseData("Body.ObjectArray[i].Array[0]", new List<int> { 0, 0 }, "InnerArrayIndex0");
                yield return new TestCaseData("Body.ObjectArray[i].Array[j]", new List<int> { 0, 1 }, "InnerArrayIndex1");
                yield return new TestCaseData("RawBody", new List<int> { 0, 0 }, "<html><body><h1>Test</h1></body></html>");
            }
        }

        public static IEnumerable<TestCaseData> GetCorrectObject_InvalidPath_ReturnsNull_TestCases
        {
            get
            {
                yield return new TestCaseData("");
                yield return new TestCaseData("Invalid");
                yield return new TestCaseData("Body.Invalid");
            }
        }

        public static IEnumerable<TestCaseData> GetCorrectObject_InvalidPath_ThrowsResultSetException_TestCases
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