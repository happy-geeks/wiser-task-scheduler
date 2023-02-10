namespace WiserTaskScheduler.Core.Models;

public class ParameterKeyModel
{
    /// <summary>
    /// Gets or sets the key for the parameter.
    /// </summary>
    public string Key { get; set; }

    /// <summary>
    /// Gets or sets if the value needs to be provided as a hash.
    /// </summary>
    public bool Hash { get; set; }
}