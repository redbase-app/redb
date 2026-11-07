using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E105_TreeQueryWithRoot"/>: a subtree query limited to a given depth.</summary>
[ExampleMeta("E105_ef", "EF: Tree Query - Scoped to Root", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E105_TreeQueryWithRoot_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E105_ef", "EF: Tree Query - Scoped to Root", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
