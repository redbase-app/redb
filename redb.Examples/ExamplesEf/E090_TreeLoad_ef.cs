using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E090_TreeLoad"/>: a subtree loaded to a given depth.</summary>
[ExampleMeta("E090_ef", "EF: Tree Load - Full Hierarchy", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E090_TreeLoad_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E090_ef", "EF: Tree Load - Full Hierarchy", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
