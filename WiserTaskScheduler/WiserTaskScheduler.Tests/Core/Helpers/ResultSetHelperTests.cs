using FluentAssertions;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Helpers;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Tests.Core.Helpers;

public class ResultSetHelperTests
{
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
            }
        };
    }

    [Test]
    [TestCase("Value", "Test1")]
    [TestCase("Body.Value", "Test2")]
    [TestCase("Body.Array[0]", "Index0")]
    [TestCase("Body.Array[1]", "Index1")]
    [TestCase("Body.Array[2]", "Index2")]
    [TestCase("Body.Array[i]", "Index1", 1, 0)]
    [TestCase("Body.ObjectArray[i].Value", "Test3", 0, 0)]
    [TestCase("Body.ObjectArray[i].Array[0]", "InnerArrayIndex0", 0, 0)]
    [TestCase("Body.ObjectArray[i].Array[j]", "InnerArrayIndex1", 0, 1)]
    public void GetCorrectObject_ValidPath_ReturnsCorrectObject(string key, string expectedValue, int indexI = 0, int indexJ = 0)
    {
        // Act
        var actual = ResultSetHelper.GetCorrectObject<JValue>(key, [indexI, indexJ], resultSet);

        // Assert
        actual.Should<JValue>().NotBeNull("because we are only testing values present in the result set.");
        ((string)actual!).Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCase("")]
    [TestCase("Invalid")]
    [TestCase("Body.Invalid")]
    public void GetCorrectObject_InvalidPath_ReturnsNull(string key)
    {
        // Act
        var actual = ResultSetHelper.GetCorrectObject<JValue>(key, null, resultSet);

        // Assert
        actual.Should<JValue>().BeNull("because the path is invalid.");
    }

    [Test]
    [TestCase("Invalid.Value")]
    [TestCase("Body.Invalid.Value")]
    [TestCase("Body.Array[3]")]
    [TestCase("Body.Array[i]", 3, 0)]
    [TestCase("Body.ObjectArray[i].Array[3]", 0, 3)]
    [TestCase("Body.ObjectArray[i].Array[j]", 0, 3)]
    public void GetCorrectObject_InvalidPath_ThrowsResultSetException(string key, int indexI = 0, int indexJ = 0)
    {
        // Act
        Action act = () => ResultSetHelper.GetCorrectObject<JValue>(key, [indexI, indexJ], resultSet);

        // Assert
        act.Should().Throw<ResultSetException>("because the path is invalid.");
    }
}