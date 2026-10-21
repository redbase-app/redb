using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E201_NullCheckBaseIdAmbiguity"/>: a regression probe of redb's own SQL builder.</summary>
[ExampleMeta("E201_ef", "EF: NullCheck + base Id: ambiguous _id in PVT CTE", "Query", ExampleTier.Pro, 1, "EF", "Debug")]
public class E201_NullCheckBaseIdAmbiguity_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E201_ef", "EF: NullCheck + base Id: ambiguous _id in PVT CTE", ExampleTier.Pro,
            "Diagnostic of redb's own SQL builders, no task to repeat in EF Core"));
}
