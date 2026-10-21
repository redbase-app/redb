using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E093_TreePath"/>: the path from a node up to the root.</summary>
[ExampleMeta("E093_ef", "EF: Tree Path - Breadcrumbs", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E093_TreePath_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E093_ef", "EF: Tree Path - Breadcrumbs", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
