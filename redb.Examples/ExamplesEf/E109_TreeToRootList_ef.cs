using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E109_TreeToRootList"/>: whole trees assembled from their roots.</summary>
[ExampleMeta("E109_ef", "EF: Tree Query - ToRootListAsync", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E109_TreeToRootList_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E109_ef", "EF: Tree Query - ToRootListAsync", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
