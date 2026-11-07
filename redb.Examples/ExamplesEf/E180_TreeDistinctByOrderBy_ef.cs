using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E180_TreeDistinctByOrderBy"/>: a distinct and sorted page of the whole subtree of a root.</summary>
[ExampleMeta("E180_ef", "EF: TreeQuery - DistinctBy + OrderBy", "Tree", ExampleTier.Pro, 1, "EF", "Tree")]
public class E180_TreeDistinctByOrderBy_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E180_ef", "EF: TreeQuery - DistinctBy + OrderBy", ExampleTier.Pro,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
