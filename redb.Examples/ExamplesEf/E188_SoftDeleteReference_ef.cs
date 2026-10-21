using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E188_SoftDeleteReference"/>: a reference to a soft-deleted object. EF Core has no soft delete.</summary>
[ExampleMeta("E188_ef", "EF: Soft-Delete - Nested Reference Not Materialized", "Core", ExampleTier.Free, 1, "EF", "SoftDelete")]
public class E188_SoftDeleteReference_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E188_ef", "EF: Soft-Delete - Nested Reference Not Materialized", ExampleTier.Free,
            "EF Core has no soft delete (trash) for objects"));
}
