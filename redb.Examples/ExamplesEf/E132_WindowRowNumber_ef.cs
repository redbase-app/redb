using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E132_WindowRowNumber"/>. EF Core does not translate window functions
/// (ROW_NUMBER() OVER (PARTITION BY ...)): dotnet/efcore#12747 is open since 2018 and absent from the
/// EF Core 10 release notes. Doing it anyway takes raw SQL or a third-party package, neither of which is
/// EF out of the box.
/// </summary>
[ExampleMeta("E132_ef", "EF: Window - RowNumber", "Analytics", ExampleTier.Free, 1, "EF", "Window")]
public class E132_WindowRowNumber_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E132_ef", "EF: Window - RowNumber", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}
