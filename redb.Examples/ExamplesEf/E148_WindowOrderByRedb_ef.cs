using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E148_WindowOrderByRedb"/>: a running COUNT over a window frame, which EF Core cannot express.</summary>
[ExampleMeta("E148_ef", "EF: Window - OrderByRedb", "Analytics", ExampleTier.Free, 1, "EF", "Window")]
public class E148_WindowOrderByRedb_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E148_ef", "EF: Window - OrderByRedb", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}
