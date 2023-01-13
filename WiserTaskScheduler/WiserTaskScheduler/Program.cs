using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using GeeksCoreLibrary.Components.Account.Interfaces;
using GeeksCoreLibrary.Components.Account.Services;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Interfaces;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Core.Services;
using GeeksCoreLibrary.Modules.Branches.Interfaces;
using GeeksCoreLibrary.Modules.Branches.Services;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.Databases.Services;
using GeeksCoreLibrary.Modules.GclReplacements.Interfaces;
using GeeksCoreLibrary.Modules.GclReplacements.Services;
using GeeksCoreLibrary.Modules.Languages.Interfaces;
using GeeksCoreLibrary.Modules.Languages.Services;
using GeeksCoreLibrary.Modules.Objects.Interfaces;
using GeeksCoreLibrary.Modules.Objects.Services;
using GeeksCoreLibrary.Modules.Payments.Interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Serilog;
using SlackNet.AspNetCore;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Workers;

namespace WiserTaskScheduler
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            await CreateHostBuilder(args).Build().RunAsync();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .UseWindowsService((options) =>
                {
                    options.ServiceName = "Auto import service";
                })
                .ConfigureAppConfiguration((hostingContext, config) =>
                {
                    config.SetBasePath(AppContext.BaseDirectory);
                    config.Sources
                        .OfType<Microsoft.Extensions.Configuration.Json.JsonConfigurationSource>()
                        .Where(x => x.Path == "appsettings.json");

                    // We need to build here already, so that we can read the base directory for secrets.
                    hostingContext.Configuration = config.Build();

                    var secretsBasePath = hostingContext.Configuration.GetSection("Wts").GetValue<string>("SecretsBaseDirectory");

                    config.AddJsonFile($"{secretsBasePath}wts-appsettings-secrets.json", false, false)
                            .AddJsonFile($"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json", true, true);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    ConfigureSettings(hostContext.Configuration, services);
                    ConfigureHostedServices(services);
                    ConfigureWtsServices(services, hostContext);
                    
                    Log.Logger = new LoggerConfiguration()
                        .ReadFrom.Configuration(hostContext.Configuration)
                        .CreateLogger();
                    services.AddLogging(builder => { builder.AddSerilog(); });
                });

        private static void ConfigureSettings(IConfiguration configuration, IServiceCollection services)
        {
            services.Configure<GclSettings>(configuration.GetSection("Gcl"));
            services.Configure<WtsSettings>(configuration.GetSection("Wts"));
        }

        private static void ConfigureHostedServices(IServiceCollection services)
        {
            services.AddHostedService<MainWorker>();
            services.AddHostedService<CleanupWorker>();
        }

        private static void ConfigureWtsServices(IServiceCollection services, HostBuilderContext hostContext)
        {
            services.AddScoped<ConfigurationsWorker>();
            services.AddScoped<IDatabaseConnection, MySqlDatabaseConnection>();
            services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();
            services.AddScoped<IObjectsService, ObjectsService>();
            services.AddScoped<IDatabaseHelpersService, MySqlDatabaseHelpersService>();
            services.AddScoped<IStringReplacementsService, StringReplacementsService>();
            services.AddScoped<ILanguagesService, LanguagesService>();
            services.AddScoped<IAccountsService, AccountsService>();
            services.AddScoped<IBranchesService, BranchesService>();
            services.AddScoped<IRolesService, RolesService>();
            
#if DEBUG
            services.AddSingleton(new SlackEndpointConfiguration());
#else
            services.AddSingleton(new SlackEndpointConfiguration().UseSigningSecret(signingSecret));
#endif
            var slackToken = hostContext.Configuration.GetSection("Wts").GetSection("SlackSettings").GetValue<string>("SlackAccessToken");
            services.AddSlackNet(c => c.UseApiToken(slackToken));

            // Configure automatic scanning of classes for dependency injection.
            services.Scan(scan => scan
                // We start out with all types in the current assembly.
                .FromCallingAssembly()
                // AddClasses starts out with all public, non-abstract types in this assembly.
                // These types are then filtered by the delegate passed to the method.
                // In this case, we filter out only the classes that are assignable to ITransientService.
                .AddClasses(classes => classes.AssignableTo<ITransientService>())
                // We then specify what type we want to register these classes as.
                // In this case, we want to register the types as all of its implemented interfaces.
                // So if a type implements 3 interfaces; A, B, C, we'd end up with three separate registrations.
                .AsImplementedInterfaces()
                // And lastly, we specify the lifetime of these registrations.
                .WithTransientLifetime()
                // Here we start again, with a new full set of classes from the assembly above.
                // This time, filtering out only the classes assignable to IScopedService.
                .AddClasses(classes => classes.AssignableTo<IScopedService>())
                .AsImplementedInterfaces()
                .WithScopedLifetime()
                // Here we start again, with a new full set of classes from the assembly above.
                // This time, filtering out only the classes assignable to IScopedService.
                .AddClasses(classes => classes.AssignableTo<ISingletonService>())
                .AsImplementedInterfaces()
                .WithSingletonLifetime()

                // Payment service providers need to be added with their own type, otherwise the factory won't work.
                .AddClasses(classes => classes.AssignableTo<IPaymentServiceProviderService>())
                .AsSelf()
                .WithScopedLifetime());
        }
    }
}
