using FluentAssertions;
using Newtonsoft.Json.Linq;
using WiserTaskScheduler.Core.Helpers;

namespace WiserTaskScheduler.Tests.Core.Helpers;

public class ResultSetHelperTests
{
    private JObject resultSet;

    [OneTimeSetUp]
    public void Setup()
    {
        resultSet = new JObject
        {
            ["Value"] = "Test",
            ["Body"] = new JObject
            {
                ["Value"] = "Test"
            }
        };
    }

    [Test]
    [TestCase("Value", "Test")]
    [TestCase("Body.Value", "Test")]
    public void GetCorrectObject_BasicPath_ReturnsCorrectObject(string key, string expectedValue)
    {
        // Act
        var actual = ResultSetHelper.GetCorrectObject<JValue>(key, null, resultSet);

        // Assert
        actual.Should<JValue>().NotBeNull("because we are only testing values present in the result set.");
        ((string)actual!).Should().BeEquivalentTo(expectedValue, "because the value should be correct.");
    }

    [Test]
    [TestCase("Invalid")]
    [TestCase("Body.Invalid")]
    public void GetCorrectObject_InvalidBasicPath_ReturnsNull(string key)
    {
        // Act
        var actual = ResultSetHelper.GetCorrectObject<JValue>(key, null, resultSet);

        // Assert
        actual.Should<JValue>().BeNull("because the path is invalid.");
    }

    [Test]
    [TestCase("Invalid.Value")]
    [TestCase("Body.Invalid.Value")]
    public void GetCorrectObject_InvalidBasicPath_ThrowsException(string key)
    {
        // Act
        Action act = () => ResultSetHelper.GetCorrectObject<JValue>(key, null, resultSet);

        // Assert
        act.Should().Throw<Exception>("because the path is invalid.");
    }
}