using System.Collections.Generic;
using GeeksCoreLibrary.Core.Models;

namespace AutoImportServiceCore.Modules.WiserImports.Models;

public class ImportDataModel
{
    /// <summary>
    /// Gets or sets the item that needs to be imported.
    /// </summary>
    public WiserItemModel Item { get; set; }
    
    /// <summary>
    /// Gets or sets the links that needs to be imported with the item.
    /// </summary>
    public List<WiserItemLinkModel> Links { get; set; }
    
    /// <summary>
    /// Gets or sets the files that needs to be imported with the item.
    /// </summary>
    public List<WiserItemFileModel> Files { get; set; }
}