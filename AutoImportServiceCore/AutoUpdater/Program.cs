using AutoUpdater.Interfaces;
using AutoUpdater.Models;
using AutoUpdater.Services;
using AutoUpdater.Workers;
using GeeksCoreLibrary.Components.Account.Interfaces;
using GeeksCoreLibrary.Components.Account.Services;
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
using Microsoft.AspNetCore.Http;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
        config.SetBasePath(AppContext.BaseDirectory);
        config.Sources
            .OfType<Microsoft.Extensions.Configuration.Json.JsonConfigurationSource>()
            .Where(x => x.Path == "appsettings.json");

        // We need to build here already, so that we can read the base directory for secrets.
        hostingContext.Configuration = config.Build();

        var secretsBasePath = hostingContext.Configuration.GetSection("Updater").GetValue<string>("SecretsBaseDirectory");

        config.AddJsonFile($"{secretsBasePath}updater-appsettings-secrets.json", false, false)
            .AddJsonFile($"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json", true, true);
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.Configure<UpdateSettings>(hostContext.Configuration.GetSection("Updater"));
        services.AddHostedService<UpdateWorker>();

        services.AddSingleton<IUpdateService, UpdateService>();
        services.AddScoped<IDatabaseConnection, MySqlDatabaseConnection>();
        services.AddScoped<IHttpContextAccessor, HttpContextAccessor>();
        services.AddScoped<IObjectsService, ObjectsService>();
        services.AddScoped<IDatabaseHelpersService, MySqlDatabaseHelpersService>();
        services.AddScoped<IStringReplacementsService, StringReplacementsService>();
        services.AddScoped<ILanguagesService, LanguagesService>();
        services.AddScoped<IAccountsService, AccountsService>();
        services.AddScoped<IBranchesService, BranchesService>();
    })
    .Build();

await host.RunAsync();