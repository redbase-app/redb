using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E177_TreeDistinctByRedb"/>: a distinct by a header field over the whole subtree of a root.</summary>
[ExampleMeta("E177_ef", "EF: TreeQuery - DistinctByRedb", "Tree", ExampleTier.Pro, 1, "EF", "Tree")]
public class E177_TreeDistinctByRedb_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E177_ef", "EF: TreeQuery - DistinctByRedb", ExampleTier.Pro,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
