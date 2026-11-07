using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E195_GroupByWindowFiltered"/>: RANK over grouped totals, which EF Core cannot express.</summary>
[ExampleMeta("E195_ef", "EF: GroupBy + Window - With Filter", "Analytics", ExampleTier.Pro, 1, "EF", "Window", "GroupBy")]
public class E195_GroupByWindowFiltered_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E195_ef", "EF: GroupBy + Window - With Filter", ExampleTier.Pro,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}
