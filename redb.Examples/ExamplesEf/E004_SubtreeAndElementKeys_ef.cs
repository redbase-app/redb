using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E004_SubtreeAndElementKeys"/>: unique keys over the content of a nested class, an
/// ordered list and collection elements, plus scheme tags. EF Core has a unique index over columns only.
/// </summary>
[ExampleMeta("E004_ef", "EF: Unique Keys - nested, subtree and element keys, [RedbTags]", "CRUD", ExampleTier.Free, 1, "EF", "Unique")]
public class E004_SubtreeAndElementKeys_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E004_ef", "EF: Unique Keys - nested, subtree and element keys, [RedbTags]", ExampleTier.Free,
            "EF Core has no unique key over the content of a nested class or a list"));
}
