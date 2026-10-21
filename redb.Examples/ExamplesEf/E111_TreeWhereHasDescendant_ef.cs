using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E111_TreeWhereHasDescendant"/>: nodes filtered by a condition on any descendant.</summary>
[ExampleMeta("E111_ef", "EF: Tree Query - WhereHasDescendant", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E111_TreeWhereHasDescendant_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E111_ef", "EF: Tree Query - WhereHasDescendant", ExampleTier.Free,
            "EF Core has no recursive query (WITH RECURSIVE) for a tree of any depth"));
}
