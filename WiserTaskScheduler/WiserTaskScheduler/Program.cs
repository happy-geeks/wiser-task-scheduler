using System;
using System.IO;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using GeeksCoreLibrary.Modules.Databases.Interfaces;
using GeeksCoreLibrary.Modules.Ftps.Handlers;
using GeeksCoreLibrary.Modules.Payments.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using SlackNet.AspNetCore;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Core.Workers;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Branches.Services;

// Configure the application itself.
var applicationBuilder = Host.CreateApplicationBuilder(args);
applicationBuilder.Environment.ApplicationName = "Wiser Task Scheduler";
applicationBuilder.Environment.ContentRootPath = AppContext.BaseDirectory;
applicationBuilder.Services.AddWindowsService(options => { options.ServiceName = "Wiser Task Scheduler"; });

// Configure the app settings.
var wtSettings = applicationBuilder.Configuration.GetSection("Wts").Get<WtsSettings>();
applicationBuilder.Configuration.AddJsonFile(Path.Combine(wtSettings.SecretsBaseDirectory, "wts-appsettings-secrets.json"), false, false);
applicationBuilder.Configuration.AddJsonFile($"appsettings.{applicationBuilder.Environment.EnvironmentName}.json", true, true);

// Add our settings to the IOptions pattern.
applicationBuilder.Services.Configure<GclSettings>(applicationBuilder.Configuration.GetSection("Gcl"));
applicationBuilder.Services.Configure<WtsSettings>(applicationBuilder.Configuration.GetSection("Wts"));

// Reload the settings after adding the secret files.
wtSettings = applicationBuilder.Configuration.GetSection("Wts").Get<WtsSettings>();

// Add the main background workers.
applicationBuilder.Services.AddHostedService<MainWorker>();
applicationBuilder.Services.AddHostedService<CleanupWorker>();
applicationBuilder.Services.AddHostedService<UpdateParentsWorker>();

applicationBuilder.Services.AddSingleton<IBranchBatchLoggerService, BranchBatchLoggerService>();
applicationBuilder.Services.AddHostedService(provider => (BranchBatchLoggerService)provider.GetRequiredService<IBranchBatchLoggerService>());

if (wtSettings.AutoProjectDeploy.IsEnabled)
{
    // Only add the auto project deploy worker if it is enabled.
    applicationBuilder.Services.AddHostedService<AutoProjectDeployWorker>();
}

// This is not added as a hosted service, because these workers will be started and stopped dynamically via the MainWorker.
applicationBuilder.Services.AddScoped<ConfigurationsWorker>();

applicationBuilder.Services.AddGclServices(applicationBuilder.Configuration, false, false, false);

// Services for the FTP handler factory from the GCL.
applicationBuilder.Services.AddScoped<FtpsHandler>();
applicationBuilder.Services.AddScoped<SftpHandler>();

// If there is a bot token provided for Slack, add the service.
var slackBotToken = wtSettings.SlackSettings.BotToken;
if (!String.IsNullOrWhiteSpace(slackBotToken))
{
    applicationBuilder.Services.AddSingleton(new SlackEndpointConfiguration());
    applicationBuilder.Services.AddSlackNet(c => c.UseApiToken(slackBotToken));
}

// Configure automatic scanning of classes for dependency injection.
applicationBuilder.Services.Scan(scan => scan
    // We start out with all types in the current assembly.
    .FromEntryAssembly()
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

// Configure Serilog.
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(applicationBuilder.Configuration)
    .CreateLogger();

applicationBuilder.Services.AddLogging(builder => { builder.AddSerilog(); });

// Build the application, do database migrations and finally run the application.
var host = applicationBuilder.Build();
using var scope = host.Services.CreateScope();
var databaseHelpersService = scope.ServiceProvider.GetRequiredService<IDatabaseHelpersService>();
await databaseHelpersService.CheckAndUpdateTablesAsync([WiserTableNames.WtsLogs, WiserTableNames.WtsServices]);

await host.RunAsync();