using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E175_TreeQueryGroupByWindow"/>: grouping and window functions over the whole subtree of a root.</summary>
[ExampleMeta("E175_ef", "EF: TreeQuery - GroupBy + Window", "Tree", ExampleTier.Pro, 1, "EF", "Tree")]
public class E175_TreeQueryGroupByWindow_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E175_ef", "EF: TreeQuery - GroupBy + Window", ExampleTier.Pro,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
