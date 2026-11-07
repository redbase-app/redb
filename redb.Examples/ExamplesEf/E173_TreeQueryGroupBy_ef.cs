using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E173_TreeQueryGroupBy"/>: a grouping over the whole subtree of a root.</summary>
[ExampleMeta("E173_ef", "EF: TreeQuery - GroupBy", "Tree", ExampleTier.Free, 1, "EF", "Tree")]
public class E173_TreeQueryGroupBy_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E173_ef", "EF: TreeQuery - GroupBy", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
