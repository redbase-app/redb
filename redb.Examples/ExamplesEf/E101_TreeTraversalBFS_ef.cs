using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E101_TreeTraversalBFS"/>: breadth-first walk over a tree loaded to a given depth.</summary>
[ExampleMeta("E101_ef", "EF: Tree Traversal - BFS", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E101_TreeTraversalBFS_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E101_ef", "EF: Tree Traversal - BFS", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
