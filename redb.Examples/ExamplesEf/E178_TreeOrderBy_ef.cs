using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E178_TreeOrderBy"/>: a sorted page of the whole subtree of a root.</summary>
[ExampleMeta("E178_ef", "EF: TreeQuery - OrderBy", "Tree", ExampleTier.Free, 1, "EF", "Tree")]
public class E178_TreeOrderBy_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E178_ef", "EF: TreeQuery - OrderBy", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
