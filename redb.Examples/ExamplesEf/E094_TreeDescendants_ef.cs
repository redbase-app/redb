using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E094_TreeDescendants"/>. All descendants of a node, at any depth, need a
/// recursive query (WITH RECURSIVE); EF Core has no LINQ translation for it. The alternatives are raw SQL
/// or loading level by level with a round trip per level, neither of which answers in one query as redb does.
/// </summary>
[ExampleMeta("E094_ef", "EF: Tree Descendants - All", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E094_TreeDescendants_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E094_ef", "EF: Tree Descendants - All", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for descendants at any depth"));
}
