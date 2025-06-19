using System;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using WiserTaskScheduler.Core.Interfaces;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Branches.Interfaces;
using WiserTaskScheduler.Modules.Branches.Models;
using WiserTaskScheduler.Modules.CleanupItems.Interfaces;
using WiserTaskScheduler.Modules.CleanupItems.Models;
using WiserTaskScheduler.Modules.CleanupWiserHistory.Interfaces;
using WiserTaskScheduler.Modules.CleanupWiserHistory.Models;
using WiserTaskScheduler.Modules.Communications.Interfaces;
using WiserTaskScheduler.Modules.Communications.Models;
using WiserTaskScheduler.Modules.DocumentStoreRead.Interfaces;
using WiserTaskScheduler.Modules.DocumentStoreRead.Models;
using WiserTaskScheduler.Modules.Ftps.Interfaces;
using WiserTaskScheduler.Modules.Ftps.Models;
using WiserTaskScheduler.Modules.GenerateCommunications.Interfaces;
using WiserTaskScheduler.Modules.GenerateCommunications.Models;
using WiserTaskScheduler.Modules.GenerateFiles.Interfaces;
using WiserTaskScheduler.Modules.GenerateFiles.Models;
using WiserTaskScheduler.Modules.HttpApis.Interfaces;
using WiserTaskScheduler.Modules.HttpApis.Models;
using WiserTaskScheduler.Modules.ImportFiles.Interfaces;
using WiserTaskScheduler.Modules.ImportFiles.Models;
using WiserTaskScheduler.Modules.ProductsApiUpdate.Interfaces;
using WiserTaskScheduler.Modules.ProductsApiUpdate.Models;
using WiserTaskScheduler.Modules.Queries.Interfaces;
using WiserTaskScheduler.Modules.Queries.Models;
using WiserTaskScheduler.Modules.ServerMonitors.Interfaces;
using WiserTaskScheduler.Modules.ServerMonitors.Models;
using WiserTaskScheduler.Modules.SlackMessages.Interfaces;
using WiserTaskScheduler.Modules.SlackMessages.Models;
using WiserTaskScheduler.Modules.WiserImports.Interfaces;
using WiserTaskScheduler.Modules.WiserImports.Models;

namespace WiserTaskScheduler.Core.Factories;

/// <summary>
/// A factory to create the correct service for an action.
/// </summary>
public class ActionsServiceFactory(IServiceProvider serviceProvider) : IActionsServiceFactory, IScopedService
{
    /// <inheritdoc />
    public IActionsService GetActionsServiceForAction(ActionModel action)
    {
        return action switch
        {
            QueryModel => serviceProvider.GetRequiredService<IQueriesService>() as IActionsService,
            HttpApiModel => serviceProvider.GetRequiredService<IHttpApisService>() as IActionsService,
            GenerateFileModel => serviceProvider.GetRequiredService<IGenerateFileService>() as IActionsService,
            ImportFileModel => serviceProvider.GetRequiredService<IImportFilesService>() as IActionsService,
            CleanupItemModel => serviceProvider.GetRequiredService<ICleanupItemsService>() as IActionsService,
            BranchQueueModel => serviceProvider.GetRequiredService<IBranchQueueService>() as IActionsService,
            WiserImportModel => serviceProvider.GetRequiredService<IWiserImportsService>() as IActionsService,
            CommunicationModel => serviceProvider.GetRequiredService<ICommunicationsService>() as IActionsService,
            ServerMonitorModel => serviceProvider.GetRequiredService<IServerMonitorsService>() as IActionsService,
            FtpModel => serviceProvider.GetRequiredService<IFtpsService>() as IActionsService,
            CleanupWiserHistoryModel => serviceProvider.GetRequiredService<ICleanupWiserHistoryService>() as IActionsService,
            GenerateCommunicationModel => serviceProvider.GetRequiredService<IGenerateCommunicationsService>() as IActionsService,
            DocumentStoreReadModel => serviceProvider.GetRequiredService<IDocumentStoreReadService>() as IActionsService,
            SlackMessageModel => serviceProvider.GetRequiredService<ISlackMessageService>() as IActionsService,
            ProductsApiUpdateModel => serviceProvider.GetRequiredService<IProductsApiUpdateService>() as IActionsService,
            _ => throw new ArgumentOutOfRangeException(nameof(action), action.ToString(), null)
        };
    }
}