using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E133_WindowRunningSum"/>. EF Core does not translate window functions (dotnet/efcore#12747), open since 2018 and absent from the EF Core 10 release notes. Doing it anyway takes raw SQL or a third-party package, neither of which is EF out of the box.
/// </summary>
[ExampleMeta("E133_ef", "EF: Window - Running Sum", "Analytics", ExampleTier.Free, 1, "EF")]
public class E133_WindowRunningSum_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E133_ef", "EF: Window - Running Sum", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}