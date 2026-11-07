using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E137_WindowFirstLast"/>. EF Core does not translate window functions (dotnet/efcore#12747), open since 2018 and absent from the EF Core 10 release notes. Doing it anyway takes raw SQL or a third-party package, neither of which is EF out of the box.
/// </summary>
[ExampleMeta("E137_ef", "EF: Window - FirstValue/LastValue", "Analytics", ExampleTier.Free, 1, "EF")]
public class E137_WindowFirstLast_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E137_ef", "EF: Window - FirstValue/LastValue", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}