using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E102_TreeIsDescendant"/>: whether a node lies anywhere below another.</summary>
[ExampleMeta("E102_ef", "EF: Tree - IsDescendantOf", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E102_TreeIsDescendant_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E102_ef", "EF: Tree - IsDescendantOf", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
