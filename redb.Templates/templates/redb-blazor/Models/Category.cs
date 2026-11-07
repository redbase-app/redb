using redb.Core.Attributes;

namespace RedbBlazor.Models;

/// <summary>
/// A node of the category tree. The hierarchy itself (parent, children) is part of the object, not of
/// the Props: a node is a <c>TreeRedbObject&lt;Category&gt;</c>, and deleting a node deletes its subtree.
/// </summary>
[RedbScheme("Category")]
public class Category
{
    public int SortOrder { get; set; }
}
