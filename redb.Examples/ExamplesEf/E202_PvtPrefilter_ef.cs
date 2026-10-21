using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E202_PvtPrefilter"/>: redb's SQL with and without its own prefilter step.</summary>
[ExampleMeta("E202_ef", "EF: PVT prefilter: SQL with and without the cutting step", "Query", ExampleTier.Pro, 1, "EF", "Debug")]
public class E202_PvtPrefilter_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E202_ef", "EF: PVT prefilter: SQL with and without the cutting step", ExampleTier.Pro,
            "Diagnostic of redb's own SQL builders, no task to repeat in EF Core"));
}
