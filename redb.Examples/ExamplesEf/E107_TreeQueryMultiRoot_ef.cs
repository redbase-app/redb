using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E107_TreeQueryMultiRoot"/>: subtrees of several roots limited to a given depth.</summary>
[ExampleMeta("E107_ef", "EF: Tree Query - Multiple Roots", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E107_TreeQueryMultiRoot_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E107_ef", "EF: Tree Query - Multiple Roots", ExampleTier.Free,
            "EF Core has no tree query with a depth parameter: every depth needs its own hand-written chain of navigations"));
}
