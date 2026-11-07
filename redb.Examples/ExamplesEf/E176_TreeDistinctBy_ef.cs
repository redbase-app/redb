using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E176_TreeDistinctBy"/>: a distinct over the whole subtree of a root.</summary>
[ExampleMeta("E176_ef", "EF: TreeQuery - DistinctBy", "Tree", ExampleTier.Pro, 1, "EF", "Tree")]
public class E176_TreeDistinctBy_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E176_ef", "EF: TreeQuery - DistinctBy", ExampleTier.Pro,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
