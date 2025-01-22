using WiserTaskScheduler.Core.Models;

namespace WiserTaskScheduler.Core.Interfaces;

/// <summary>
/// A factory to create the correct service for an action.
/// </summary>
public interface IActionsServiceFactory
{
    /// <summary>
    /// Gets the correct service for an action.
    /// </summary>
    /// <param name="action">The action to create a service for.</param>
    /// <returns></returns>
    IActionsService GetActionsServiceForAction(ActionModel action);
}