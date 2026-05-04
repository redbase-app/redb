using redb.Core.Attributes;

namespace redb.Examples.Models;

/// <summary>
/// Platform-specific test result (Count, Time, Status).
/// Used as value in Dictionary for each platform.
/// </summary>
public class PlatformResult
{
    /// <summary>Number of objects processed/found.</summary>
    public int? Count { get; set; }

    /// <summary>Execution time in milliseconds.</summary>
    public int? Time { get; set; }

    /// <summary>Status: "OK", "FAIL", or error message.</summary>
    public string? Status { get; set; }
}

/// <summary>
/// Props for storing example test results in redb database.
/// Key (E001, E002...) stored in RedbObject._value_string.
/// Title stored in RedbObject._name.
/// </summary>
[RedbScheme("ExampleResult")]
public class ExampleResultProps
{
    /// <summary>License tier: "Free", "Pro", "Enterprise".</summary>
    public string Tier { get; set; } = string.Empty;

    /// <summary>
    /// Results by platform: "mssql", "mssql.pro", "postgres", "postgres.pro".
    /// Each platform has Count, Time, Status.
    /// </summary>
    public Dictionary<string, PlatformResult>? Results { get; set; }
}
