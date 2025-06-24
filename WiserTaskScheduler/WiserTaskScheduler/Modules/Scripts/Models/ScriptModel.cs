using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;
using WiserTaskScheduler.Core.Models;
using WiserTaskScheduler.Modules.Scripts.Enums;

namespace WiserTaskScheduler.Modules.Scripts.Models;

/// <summary>
/// A model for a script.
/// </summary>
[XmlType("Script")]
public class ScriptModel : ActionModel
{
    /// <summary>
    /// Gets or sets the interpreter to use.
    /// </summary>
    [Required]
    public Interpreters Interpreter { get; set; }

    /// <summary>
    /// Gets or sets the script to execute.
    /// </summary>
    [Required]
    public string Script { get; set; }
}