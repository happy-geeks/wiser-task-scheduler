using AutoUpdater.Interfaces;
using AutoUpdater.Models;
using AutoUpdater.Services;
using AutoUpdater.Workers;
using GeeksCoreLibrary.Core.Extensions;
using GeeksCoreLibrary.Core.Models;
using Microsoft.Extensions.Configuration.Json;
using Serilog;
using SlackNet.AspNetCore;

var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options => { options.ServiceName = "WTS Auto Updater"; })
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
        config.SetBasePath(AppContext.BaseDirectory);
        config.Sources
            .OfType<JsonConfigurationSource>()
            .Where(x => x.Path == "appsettings.json");

        // We need to build here already, so that we can read the base directory for secrets.
        hostingContext.Configuration = config.Build();

        var secretsBasePath = hostingContext.Configuration.GetSection("Updater").GetValue<string>("SecretsBaseDirectory");

        config.AddJsonFile(Path.Combine(secretsBasePath, "updater-appsettings-secrets.json"), false, false)
            .AddJsonFile($"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json", true, true);
    })
    .ConfigureServices((hostContext, services) =>
    {
        Log.Logger = new LoggerConfiguration()
            .ReadFrom.Configuration(hostContext.Configuration)
            .CreateLogger();
        services.AddLogging(builder => { builder.AddSerilog(); });

        services.Configure<GclSettings>(hostContext.Configuration.GetSection("Gcl"));
        services.Configure<UpdateSettings>(hostContext.Configuration.GetSection("Updater"));

        var slackSettings = hostContext.Configuration.GetSection("Updater").GetSection("SlackSettings");
        services.Configure<SlackSettings>(slackSettings);

        services.AddHostedService<UpdateWorker>();

        services.AddSingleton<ISlackChatService, SlackChatService>();
        services.AddSingleton<IUpdateService, UpdateService>();
        services.AddHttpContextAccessor();
        services.AddGclServices(hostContext.Configuration, false, false, false);

        // If there is a bot token provided for Slack add the service.
        var slackBotToken = slackSettings.GetValue<string>("BotToken");
        if (!String.IsNullOrWhiteSpace(slackBotToken))
        {
            services.AddSingleton(new SlackEndpointConfiguration());
            services.AddSlackNet(c => c.UseApiToken(slackBotToken));
        }
    })
    .Build();

await host.RunAsync();