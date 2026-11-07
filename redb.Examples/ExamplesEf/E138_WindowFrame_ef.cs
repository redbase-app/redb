using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E138_WindowFrame"/>. EF Core does not translate window functions (dotnet/efcore#12747), open since 2018 and absent from the EF Core 10 release notes. Doing it anyway takes raw SQL or a third-party package, neither of which is EF out of the box.
/// </summary>
[ExampleMeta("E138_ef", "EF: Window - Frame (Sliding)", "Analytics", ExampleTier.Free, 1, "EF")]
public class E138_WindowFrame_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E138_ef", "EF: Window - Frame (Sliding)", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}