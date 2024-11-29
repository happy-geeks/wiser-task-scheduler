using System;
using System.Threading;
using System.Threading.Tasks;

namespace WiserTaskScheduler.Core.Helpers;

public class TaskHelpers
{
    /// <summary>
    /// Wait for the specified amount of time. There is a maximum amount of time that a task can be delayed, so this method will split the time into smaller parts if the time is too long.
    /// </summary>
    /// <param name="timeToWait">The total time to wait.</param>
    /// <param name="stoppingToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that you can await.</returns>
    public static async Task WaitAsync(TimeSpan timeToWait, CancellationToken stoppingToken)
    {
        // TODO: This method is still untested. It should be tested to see if it works as expected.
        var remainingTime = timeToWait;

        while (remainingTime.TotalMilliseconds > 0)
        {
            var timeToWaitNow = remainingTime;
            if (remainingTime.TotalMilliseconds > Int32.MaxValue)
            {
                timeToWaitNow = new TimeSpan(0, 0, 0, 0, Int32.MaxValue);
            }
            
            remainingTime -= timeToWaitNow;
            await Task.Delay(timeToWaitNow, stoppingToken);
        }
    }
}