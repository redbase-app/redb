using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E160_TreeQueryExpressions"/>: a filter over the whole subtree of a root.</summary>
[ExampleMeta("E160_ef", "EF: TreeQuery - Expressions", "Tree", ExampleTier.Free, 1, "EF", "Tree")]
public class E160_TreeQueryExpressions_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E160_ef", "EF: TreeQuery - Expressions", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
