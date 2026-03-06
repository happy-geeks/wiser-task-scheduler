using System.Threading.Tasks;
using WiserTaskScheduler.Core.Interfaces;

namespace WiserTaskScheduler.Core.Services;

public class DummyNotificationService : INotificationService
{
    public Task SendChannelMessageAsync(string message, string[] replies = null, string recipient = null, string messageHash = null)
    {
        return Task.CompletedTask;
    }
}