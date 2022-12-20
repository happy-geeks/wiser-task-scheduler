namespace WiserTaskScheduler.Core.Enums
{
    /// <summary>
    /// Scopes of different stages in the WTS to limit logging.
    /// </summary>
    public enum LogScopes
    {
        /// <summary>
        /// Used for logs during startup and shutdown.
        /// </summary>
        StartAndStop,

        /// <summary>
        /// Used for logs when a run is starting and stopping.
        /// </summary>
        RunStartAndStop,

        /// <summary>
        /// Used for logs within the run.
        /// </summary>
        RunBody
    }
}
