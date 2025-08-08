using System.Text.RegularExpressions;

namespace WiserTaskScheduler.Modules.Branches.Helpers;

public static partial class PrecompiledRegexes
{
    /// <summary>
    /// Matches a styled output identifier in the format "number-text".
    /// E.g. "123-ExampleOutput".
    /// </summary>
    [GeneratedRegex(@"^(\d+)-(.+)$")]
    public static partial Regex StyledOutputIdentifierRegex { get; }

    /// <summary>
    /// Matches a styled output body in the format "{StyledOutput~number}" or "{StyledOutput~number~parameter~value}".
    /// E.g. "{StyledOutput~123}" or "{StyledOutput~123~param~value}".
    /// The digit after "{StyledOutput~" is captured as a group as the whole digit. Preventing issues with mapping "{StyledOutput~10" as "{StyledOutput~1".
    /// </summary>
    [GeneratedRegex(@"\{StyledOutput~(\d+)")]
    public static partial Regex StyledOutputBodyRegex { get; }
}