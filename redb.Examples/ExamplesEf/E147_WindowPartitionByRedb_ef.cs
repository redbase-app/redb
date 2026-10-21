using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E147_WindowPartitionByRedb"/>: ROW_NUMBER over a partition, which EF Core cannot express.</summary>
[ExampleMeta("E147_ef", "EF: Window - PartitionByRedb", "Analytics", ExampleTier.Free, 1, "EF", "Window")]
public class E147_WindowPartitionByRedb_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E147_ef", "EF: Window - PartitionByRedb", ExampleTier.Free,
            "EF Core does not translate window functions (dotnet/efcore#12747)"));
}
