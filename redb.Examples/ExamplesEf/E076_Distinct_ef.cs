using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E076_Distinct"/>. redb removes objects whose whole content is the same. In EF the content of an employee is spread over some forty tables; Distinct on an entity query compares the rows of the root table, key included, so it never removes anything. Comparing whole graphs would mean grouping by every column of every table, which no EF query expresses.
/// </summary>
[ExampleMeta("E076_ef", "EF: Distinct - Unique by Props", "Query", ExampleTier.Free, 1, "EF")]
public class E076_Distinct_ef : EfExampleBase
{
    public override Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts) =>
        Task.FromResult(NotSupported("E076_ef", "EF: Distinct - Unique by Props", ExampleTier.Free,
            "EF Core has no distinct by the content of a whole object graph"));
}