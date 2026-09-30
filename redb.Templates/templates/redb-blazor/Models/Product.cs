using redb.Core.Attributes;

namespace RedbBlazor.Models;

/// <summary>
/// The Props of a product. The object's name is not here: it is the object's own field
/// (<c>RedbObject.Name</c>). Each property is stored in a typed column, so filters and sorting on
/// them run in the database.
/// </summary>
[RedbScheme("Product")]
public class Product
{
    public string Category { get; set; } = string.Empty;

    public decimal Price { get; set; }

    public bool InStock { get; set; }

    public string? Description { get; set; }
}
