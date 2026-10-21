using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E099_TreeQueryLevel"/>: the nodes at a given level below a root.</summary>
[ExampleMeta("E099_ef", "EF: Tree Query - By Level", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E099_TreeQueryLevel_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E099_ef", "EF: Tree Query - By Level", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
