using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E112_TreeStats"/>: statistics of a tree loaded to a given depth.</summary>
[ExampleMeta("E112_ef", "EF: Tree - GetStats", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E112_TreeStats_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E112_ef", "EF: Tree - GetStats", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
