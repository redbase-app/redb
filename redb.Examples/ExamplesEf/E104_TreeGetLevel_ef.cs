using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E104_TreeGetLevel"/>: the depth of a node from its root.</summary>
[ExampleMeta("E104_ef", "EF: Tree - GetLevel", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E104_TreeGetLevel_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E104_ef", "EF: Tree - GetLevel", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
