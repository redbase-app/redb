using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E174_TreeQueryWindow"/>: window functions over the whole subtree of a root.</summary>
[ExampleMeta("E174_ef", "EF: TreeQuery - Window Functions", "Tree", ExampleTier.Free, 1, "EF", "Tree")]
public class E174_TreeQueryWindow_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E174_ef", "EF: TreeQuery - Window Functions", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
