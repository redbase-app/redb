using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E204_TreePrefilter"/>: filtered counts inside a subtree of any depth.</summary>
[ExampleMeta("E204_ef", "EF: PVT prefilter in tree queries", "Trees", ExampleTier.Pro, 1, "EF", "Tree")]
public class E204_TreePrefilter_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E204_ef", "EF: PVT prefilter in tree queries", ExampleTier.Pro,
            "EF Core has no recursive query (WITH RECURSIVE) for a subtree at any depth"));
}
