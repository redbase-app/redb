using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Self-referencing node for the V4 Л1 lazy-reference scenarios (LAZY_REFERENCES_PLAN §6):
/// one scheme gives chains of any depth (<see cref="Next"/>) and collections of references
/// (<see cref="Children"/>). The [RedbScheme] marker matters: the Pro stub materializer resolves
/// the CLR type for a nested reference through the attribute-driven registry.
/// </summary>
[RedbScheme]
public class LazyNodeProps
{
    public string? Label { get; set; }

    /// <summary>A single reference — the stub scenario.</summary>
    public RedbObject<LazyNodeProps>? Next { get; set; }

    /// <summary>A collection of references — the list-of-stubs scenario.</summary>
    public List<RedbObject<LazyNodeProps>>? Children { get; set; }

    /// <summary>
    /// A nested class: depth counts reference hops, never class nesting, so a class field is loaded
    /// completely at any depth — by the SQL builders and the Pro materializer alike.
    /// </summary>
    public LazyNodeMeta? Meta { get; set; }
}

public class LazyNodeMeta
{
    public string? Tag { get; set; }
}
