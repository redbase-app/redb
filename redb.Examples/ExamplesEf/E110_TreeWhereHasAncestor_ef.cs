using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E110_TreeWhereHasAncestor"/>: nodes filtered by a condition on any ancestor.</summary>
[ExampleMeta("E110_ef", "EF: Tree Query - WhereHasAncestor", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E110_TreeWhereHasAncestor_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E110_ef", "EF: Tree Query - WhereHasAncestor", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
