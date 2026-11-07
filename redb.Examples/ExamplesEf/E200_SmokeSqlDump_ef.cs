using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E200_SmokeSqlDump"/>: a dump of redb's own SQL for diffing two redb builders.</summary>
[ExampleMeta("E200_ef", "EF: SQL dump for smoke cases (Pro vs PVT)", "Query", ExampleTier.Pro, 1, "EF", "Debug")]
public class E200_SmokeSqlDump_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E200_ef", "EF: SQL dump for smoke cases (Pro vs PVT)", ExampleTier.Pro,
            "Diagnostic of redb's own SQL builders, no task to repeat in EF Core"));
}
