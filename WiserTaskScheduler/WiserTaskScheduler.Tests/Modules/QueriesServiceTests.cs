using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Services;
using WiserTaskScheduler.Modules.Queries.Models;
using WiserTaskScheduler.Modules.Queries.Services;
using WiserTaskScheduler.Tests.Mocks;

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
        Assert.That(task.IsCompleted, Is.True);
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.InitializeAsync_InvalidConfiguration_ThrowsException_TestCases))]
    public async Task InitializeAsync_InvalidConfiguration_ThrowsException(ConfigurationModel configuration, HashSet<string> tablesToOptimize, IOptions<GclSettings> gclSettings)
    {
        // Arrange
        var queriesService = new QueriesService(null, null, null, gclSettings);

        // Assert
        Assert.Throws<ArgumentException>(() => queriesService.InitializeAsync(configuration, tablesToOptimize), "because no connection string was provided.");
    }

    [Test]
    [TestCaseSource(typeof(TestCases), nameof(TestCases.Execute_ValidConfiguration_PerformsQuery_TestCases))]
    public async Task Execute_ValidConfiguration_PerformsQuery(ConfigurationModel configuration, HashSet<string> tablesToOptimize, IOptions<GclSettings> gclSettings, JObject resultSets, string expectedResult)
    {
        // Arrange
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddScoped<IDatabaseConnection, MockDatabaseConnection>();
        serviceCollection.AddScoped<ILogger<QueriesService>, MockLogger<QueriesService>>();
        var serviceProvider = serviceCollection.BuildServiceProvider();

        var queriesService = new QueriesService(new LogService(serviceProvider, null, Options.Create(new WtsSettings())), serviceProvider.GetRequiredService<ILogger<QueriesService>>(), serviceProvider, gclSettings);
        await queriesService.InitializeAsync(configuration, tablesToOptimize);

        // Act
        var result = await queriesService.Execute(configuration.Queries[0], resultSets, "Unit test");

        // Assert
        var actual = result.ToString(Formatting.None);
        Assert.That(result, Is.Not.Null, "because the result should not be null.");
        Assert.That(actual, Is.EquivalentTo(expectedResult), "because the query should match the expected result.");
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

        public static IEnumerable<TestCaseData> Execute_ValidConfiguration_PerformsQuery_TestCases
        {
            get
            {
                yield return new TestCaseData(new ConfigurationModel()
                {
                    ConnectionString = "data source=localhost;initial catalog=TestDB;user id=TestUser;password=TestPassword",
                    ServiceName = "TestService",
                    Queries = new []
                    {
                        new QueryModel()
                        {
                            TimeId = 1,
                            Order = 1,
                            Query = """
                                    # TestQuery1
                                    SELECT 1 AS testValue
                                    """
                        }
                    }
                },
                null,
                Options.Create(new GclSettings()),
                new JObject(),
                """
                {"Results":[{"testValue":"1"}]}
                """);
            }
        }
    }
}