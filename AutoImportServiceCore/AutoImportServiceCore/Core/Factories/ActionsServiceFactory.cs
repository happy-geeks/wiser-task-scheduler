using System;
using AutoImportServiceCore.Core.Interfaces;
using AutoImportServiceCore.Core.Models;
using AutoImportServiceCore.Modules.Ftps.Interfaces;
using AutoImportServiceCore.Modules.Ftps.Models;
using AutoImportServiceCore.Modules.GenerateFiles.Interfaces;
using AutoImportServiceCore.Modules.GenerateFiles.Models;
using AutoImportServiceCore.Modules.HttpApis.Interfaces;
using AutoImportServiceCore.Modules.HttpApis.Models;
using AutoImportServiceCore.Modules.ImportFiles.Interfaces;
using AutoImportServiceCore.Modules.ImportFiles.Models;
using AutoImportServiceCore.Modules.Queries.Interfaces;
using AutoImportServiceCore.Modules.Queries.Models;
using GeeksCoreLibrary.Core.DependencyInjection.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace AutoImportServiceCore.Core.Factories
{
    /// <summary>
    /// A factory to create the correct service for an action.
    /// </summary>
    public class ActionsServiceFactory : IActionsServiceFactory, IScopedService
    {
        private readonly IServiceProvider serviceProvider;

        /// <summary>
        /// Creates a new instance of <see cref="ActionsServiceFactory"/>.
        /// </summary>
        /// <param name="serviceProvider"></param>
        public ActionsServiceFactory(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        /// <inheritdoc />
        public IActionsService GetActionsServiceForAction(ActionModel action)
        {
            switch (action)
            {
                case QueryModel:
                    return serviceProvider.GetRequiredService<IQueriesService>() as IActionsService;
                case HttpApiModel:
                    return serviceProvider.GetRequiredService<IHttpApisService>() as IActionsService;
                case GenerateFileModel:
                    return serviceProvider.GetRequiredService<IGenerateFileService>() as IActionsService;
                case ImportFileModel:
                    return serviceProvider.GetRequiredService<IImportFilesService>() as IActionsService;
                case FtpModel:
                    return serviceProvider.GetRequiredService<IFtpsService>() as IActionsService;
                default:
                    throw new ArgumentOutOfRangeException(nameof(action), action.ToString());
            }
        }
    }
}
