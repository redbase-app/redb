using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E191_WindowFiltered"/>: ROW_NUMBER over a partition after a filter, which EF Core cannot express.</summary>
[ExampleMeta("E191_ef", "EF: Window - With Filter", "Analytics", ExampleTier.Free, 1, "EF", "Window")]
public class E191_WindowFiltered_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E191_ef", "EF: Window - With Filter", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}
