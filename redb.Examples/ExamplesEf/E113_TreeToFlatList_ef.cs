using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E113_TreeToFlatList"/>: a filtered subtree limited to a given depth, as a flat list.</summary>
[ExampleMeta("E113_ef", "EF: Tree Query - ToFlatListAsync", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E113_TreeToFlatList_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E113_ef", "EF: Tree Query - ToFlatListAsync", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
