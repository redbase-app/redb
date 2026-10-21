using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E003_UniqueKeys"/>: a business key that soft delete releases and a typed violation
/// naming the key. EF Core has no soft delete and reports a unique violation as a provider exception.
/// </summary>
[ExampleMeta("E003_ef", "EF: Unique Keys - [RedbUnique] and value_unique", "CRUD", ExampleTier.Free, 1, "EF", "Unique")]
public class E003_UniqueKeys_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E003_ef", "EF: Unique Keys - [RedbUnique] and value_unique", ExampleTier.Free,
            "EF Core has no soft delete that releases a unique key and no typed unique violation"));
}
