using GeeksCoreLibrary.Core.Models;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Queries.Services;

namespace WiserTaskScheduler.Tests.Modules;

[Category("Modules")]
public class QueriesServiceTests
{
    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.InitializeAsync_ValidConfiguration_Initializes_TestCases))]
    public void InitializeAsync_ValidConfiguration_Initializes(ConfigurationModel configuration, HashSet<string> tablesToOptimize, IOptions<GclSettings> gclSettings)
    {
        // Arrange
        var queriesService = new QueriesService(null, null, null, gclSettings);

        // Act
        var task = queriesService.InitializeAsync(configuration, tablesToOptimize);
        task.Wait();

        // Assert
        task.IsCompleted.Should().BeTrue();
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.InitializeAsync_InvalidConfiguration_ThrowsException_TestCases))]
    public async Task InitializeAsync_InvalidConfiguration_ThrowsException(ConfigurationModel configuration, HashSet<string> tablesToOptimize, IOptions<GclSettings> gclSettings)
    {
        // Arrange
        var queriesService = new QueriesService(null, null, null, gclSettings);

        // Act
        var actual = () => queriesService.InitializeAsync(configuration, tablesToOptimize);

        // Assert
        await actual.Should().ThrowAsync<ArgumentException>("because no connection string was provided.");
    }

    [SuppressMessage("ReSharper", "InconsistentNaming")]
    private class TestCases
    {
        public static IEnumerable<TestCaseData> InitializeAsync_ValidConfiguration_Initializes_TestCases
        {
            get
            {
                yield return new TestCaseData(new ConfigurationModel()
                {
                    ConnectionString = "data source=localhost;initial catalog=TestDB;user id=TestUser;password=TestPassword",
                    ServiceName = "TestService"
                }, null, Options.Create(new GclSettings()));
                yield return new TestCaseData(new ConfigurationModel()
                {
                    ServiceName = "TestService"
                }, null, Options.Create(new GclSettings()
                {
                    ConnectionString = "data source=localhost;user id=TestUser;password=TestPassword"
                }));
                yield return new TestCaseData(new ConfigurationModel()
                {
                    ConnectionString = "data source=localhost;initial catalog=TestDB;user id=TestUser;password=TestPassword",
                    ServiceName = "TestService"
                }, null, Options.Create(new GclSettings()
                {
                    ConnectionString = "data source=localhost;user id=TestUser;password=TestPassword"
                }));
            }
        }

        public static IEnumerable<TestCaseData> InitializeAsync_InvalidConfiguration_ThrowsException_TestCases
        {
            get
            {
                yield return new TestCaseData(new ConfigurationModel()
                {
                    ServiceName = "TestService"
                }, null, Options.Create(new GclSettings()));
            }
        }
    }
}