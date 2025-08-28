using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Workers;

public class ProductsApiUpdateWorker : BaseWorker
{
    private const string LogName = "ProductsApiUpdateService";

    private readonly IProductsApiUpdateService productsApiUpdateService;

    /// <summary>
    /// Creates a new instance of <see cref="ProductsApiUpdateWorker"/>.
    /// </summary>
    /// <param name="wtsSettings">The settings of the WTS for the run scheme.</param>
    /// <param name="productsApiUpdateService">The service to handle the products api for the WTS.</param>
    /// <param name="baseWorkerDependencyAggregate">The aggregate containing the dependencies needed by the <see cref="BaseWorker"/>.</param>
    public ProductsApiUpdateWorker(IOptions<WtsSettings> wtsSettings, IProductsApiUpdateService productsApiUpdateService, IBaseWorkerDependencyAggregate baseWorkerDependencyAggregate) : base(baseWorkerDependencyAggregate)
    {
        Initialize(LogName, wtsSettings.Value.ProductsApiUpdateService.RunScheme, wtsSettings.Value.ServiceFailedNotificationEmails, true);
        RunScheme.LogSettings ??= new LogSettings();

        this.productsApiUpdateService = productsApiUpdateService;

        this.productsApiUpdateService.LogSettings = RunScheme.LogSettings;
    }

    /// <inheritdoc />
    protected override async Task ExecuteActionAsync(CancellationToken stoppingToken)
    {
        await productsApiUpdateService.UpdateProductsAsync();
    }
}