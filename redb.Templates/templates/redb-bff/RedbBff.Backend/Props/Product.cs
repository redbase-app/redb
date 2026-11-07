using redb.Core.Attributes;

namespace RedbBff.Backend.Props;

/// <summary>
/// The Props of a product. The object's name is not here: it is the object's own field
/// (<c>RedbObject.Name</c>). Each property is stored in a typed column, so filters and sorting on them run
/// in the database.
/// </summary>
[RedbScheme("Product")]
public class Product
{
    public string Category { get; set; } = string.Empty;

    public decimal Price { get; set; }

    public bool InStock { get; set; }

    public string? Description { get; set; }
}

/// <summary>
/// A node of the category tree. The hierarchy itself is part of the object, not of the Props: a node is a
/// <c>TreeRedbObject&lt;Category&gt;</c>, and deleting a node deletes its subtree.
/// </summary>
[RedbScheme("Category")]
public class Category
{
    public int SortOrder { get; set; }
}
