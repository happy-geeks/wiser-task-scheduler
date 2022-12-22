using System;
using System.Data;

namespace WiserTaskScheduler.Modules.WiserImports.Models;

public class ImportRowModel
{
    /// <summary>
    /// Gets or sets the ID of the import.
    /// </summary>
    public int Id { get; set; }

    /// <summary>
    /// Gets or sets the name of the import.
    /// </summary>
    public string ImportName { get; set; }

    /// <summary>
    /// Gets or sets the raw data to be imported.
    /// </summary>
    public string RawData { get; set; }

    /// <summary>
    /// Gets or sets the ID of the user that created the import.
    /// </summary>
    public ulong UserId { get; set; }
    
    /// <summary>
    /// Gets or sets the name of the user that created the import.
    /// </summary>
    public string Username { get; set; }

    /// <summary>
    /// Gets or sets the ID of the customer for the path to the file folder.
    /// </summary>
    public int CustomerId { get; set; }

    /// <summary>
    /// Gets or sets the sub domain the import is performed on.
    /// </summary>
    public string SubDomain { get; set; }

    /// <summary>
    /// Create a new <see cref="ImportRowModel"/> from a <see cref="DataRow"/>.
    /// </summary>
    /// <param name="row">The <see cref="DataRow"/> to retrieve the information from.</param>
    public ImportRowModel(DataRow row)
    {
        Id = row.Field<int>("id");
        ImportName = row.Field<string>("name");
        RawData = row.Field<string>("data");
        UserId = Convert.ToUInt64(row.Field<object>("user_id"));
        Username = row.Field<string>("added_by");
        CustomerId = row.Field<int>("customer_id");
        SubDomain = row.Field<string>("sub_domain");
    }
}