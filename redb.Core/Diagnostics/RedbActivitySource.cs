using System.Diagnostics;

namespace redb.Core.Diagnostics;

/// <summary>
/// Centralized telemetry identity for REDB. The engine declares each database command on an
/// <see cref="ActivitySource"/> named <see cref="SourceName"/>; an OpenTelemetry consumer subscribes
/// with <c>.AddSource(RedbActivitySource.SourceName)</c> and a span per command appears.
/// <para>
/// The source is inert until something subscribes: with no listener nothing is emitted and a command
/// is run straight through, so REDB adds no trace by itself and no statement text or parameters ever
/// leave the process in a span. This is the shape EF Core uses for its own
/// <c>Microsoft.EntityFrameworkCore</c> source: the engine owns the source, the application opts in
/// by name.
/// </para>
/// </summary>
public static class RedbActivitySource
{
    /// <summary>Canonical source name used for OpenTelemetry tracing instrumentation.</summary>
    public const string SourceName = "redb.Core";

    /// <summary>The shared activity source instance.</summary>
    public static readonly ActivitySource Source = new(SourceName, GetVersion());

    private static string GetVersion()
        => typeof(RedbActivitySource).Assembly.GetName().Version?.ToString() ?? "0.0.0";
}
