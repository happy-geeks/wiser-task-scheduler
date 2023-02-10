using System.Diagnostics;

namespace AutoUpdater.Models;

public class VersionModel
{
    /// <summary>
    /// The major part of the version.
    /// </summary>
    public int Major { get; set; }

    /// <summary>
    /// The minor part of the version.
    /// </summary>
    public int Minor { get; set; }

    /// <summary>
    /// The build part of the version.
    /// </summary>
    public int Build { get; set; }

    /// <summary>
    /// The revision part of the version.
    /// </summary>
    public int Revision { get; set; }
    
    public Version Version => new(ToString());

    /// <summary>
    /// True if the current version contains breaking changes, aut update will be prevented.
    /// </summary>
    public bool ContainsBreakingChanges { get; set; }

    /// <inheritdoc />
    public override string ToString()
    {
        return $"{Major}.{Minor}.{Build}.{Revision}";
    }
}