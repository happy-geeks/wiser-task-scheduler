using AutoUpdater.Workers;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services => { services.AddHostedService<UpdateWorker>(); })
    .Build();

await host.RunAsync();