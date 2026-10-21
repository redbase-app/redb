namespace redb.Examples.Output;

/// <summary>
/// License tier for examples.
/// </summary>
public enum ExampleTier
{
    Free = 0,
    Pro = 1,
    Enterprise = 2
}

/// <summary>
/// Result of running an example. Used for unified output.
/// </summary>
public record ExampleResult
{
    /// <summary>Example ID (E001, E010).</summary>
    public required string Id { get; init; }

    /// <summary>Example title.</summary>
    public required string Title { get; init; }

    /// <summary>License tier.</summary>
    public required ExampleTier Tier { get; init; }

    /// <summary>Success flag.</summary>
    public required bool Success { get; init; }

    /// <summary>Execution time in milliseconds.</summary>
    public required long ElapsedMs { get; init; }

    /// <summary>Number of objects processed/found.</summary>
    public int? Count { get; init; }

    /// <summary>Short output lines (2-3 max).</summary>
    public required string[] Output { get; init; }

    /// <summary>Generated SQL (if ToSqlStringAsync was called).</summary>
    public string? Sql { get; init; }

    /// <summary>Error message if failed.</summary>
    public string? Error { get; init; }

    /// <summary>Database round trips the example made; filled by the runner, null when not counted.</summary>
    public int? Queries { get; init; }

    /// <summary>
    /// The library cannot do this out of the box (an EF twin of a window-function or recursive-tree example).
    /// Not a failure: the reason is in <see cref="Output"/>, there is no time to compare.
    /// </summary>
    public bool NotSupported { get; init; }
}
