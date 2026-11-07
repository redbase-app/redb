using Microsoft.EntityFrameworkCore;
using redb.Core;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// Base class of the EF Core twins of the examples (redb.Export/doc/PLAN_EF_BENCHMARK.md). A twin carries
/// the id of its redb example with the suffix <c>_ef</c>, works on the flat database through EF Core only,
/// and returns the same result shape, so the runner prints both side by side.
/// </summary>
public abstract class EfExampleBase : ExampleBase
{
    /// <summary>
    /// Factory of contexts over the flat database, set by Program before any EF example runs. A twin
    /// creates a new context for each unit of work, as EF recommends; the model is built once per process.
    /// </summary>
    public static IDbContextFactory<FlatContext>? Contexts { get; set; }

    /// <inheritdoc />
    public sealed override Task<ExampleResult> RunAsync(IRedbService redb) =>
        RunEfAsync(Contexts ?? throw new InvalidOperationException(
            "The EF context factory is not set: EF examples run only from Program."));

    /// <summary>Runs the example through EF Core.</summary>
    public abstract Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts);

    /// <summary>
    /// Result of a twin whose task EF Core cannot do out of the box. No raw SQL stands in for the missing
    /// feature: the comparison table says so instead of showing a time.
    /// </summary>
    protected static ExampleResult NotSupported(string id, string title, ExampleTier tier, string reason) => new()
    {
        Id = id,
        Title = title,
        Tier = tier,
        Success = true,
        NotSupported = true,
        ElapsedMs = 0,
        Output = [reason]
    };
}
