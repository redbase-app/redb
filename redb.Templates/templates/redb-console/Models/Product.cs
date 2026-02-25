using redb.Core.Attributes;

namespace RedbApp.Models;

/// <summary>
/// Example product with typed Props.
/// RedBase stores each property in a real typed column — not JSON.
/// </summary>
[RedbScheme("Product")]
public class Product
{
    public decimal Price { get; set; }

    public string Category { get; set; } = string.Empty;

    public bool InStock { get; set; }

    public string[] Tags { get; set; } = [];
}

/// <summary>
/// Category tree node.
/// Use <c>CreateChildAsync</c> to build hierarchies.
/// RedBase stores parent-child via <c>_id_parent</c> FK with ON DELETE CASCADE.
/// </summary>
[RedbScheme("Category")]
public class Category
{
    public int SortOrder { get; set; }
}
