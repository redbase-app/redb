using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E108_TreeToTreeList"/>: found nodes with their parent chain up to the root.</summary>
[ExampleMeta("E108_ef", "EF: Tree Query - ToTreeListAsync", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E108_TreeToTreeList_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E108_ef", "EF: Tree Query - ToTreeListAsync", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
