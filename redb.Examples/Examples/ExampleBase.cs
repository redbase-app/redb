using System.Diagnostics;
using redb.Core;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Attribute to mark example classes with metadata.
/// Used for parsing examples into ExampleProps for website.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class ExampleMetaAttribute(
    string id,
    string title,
    string category,
    ExampleTier tier = ExampleTier.Free,
    int difficulty = 1,
    params string[] tags) : Attribute
{
    /// <summary>Unique example ID (E001, E010).</summary>
    public string Id { get; } = id;

    /// <summary>Example title for display.</summary>
    public string Title { get; } = title;

    /// <summary>Category for navigation (CRUD, Query, Trees, etc).</summary>
    public string Category { get; } = category;

    /// <summary>License tier (Free, Pro, Enterprise).</summary>
    public ExampleTier Tier { get; } = tier;

    /// <summary>Difficulty level 1-5.</summary>
    public int Difficulty { get; } = difficulty;

    /// <summary>Tags for filtering (SaveAsync, Where, etc).</summary>
    public string[] Tags { get; } = tags;

    /// <summary>Display order on website (set via property).</summary>
    public int Order { get; set; }

    /// <summary>Related API methods for linking to documentation (e.g., "IRedbService.Query", "ITreeQueryable.WhereRoots").</summary>
    public string[] RelatedApis { get; set; } = [];
}

/// <summary>
/// Base class for all examples.
/// </summary>
public abstract class ExampleBase
{
    /// <summary>
    /// Run the example and return result.
    /// </summary>
    public abstract Task<ExampleResult> RunAsync(IRedbService redb);

    /// <summary>
    /// Get metadata from attribute.
    /// </summary>
    public ExampleMetaAttribute? GetMeta()
    {
        return GetType().GetCustomAttributes(typeof(ExampleMetaAttribute), false)
            .FirstOrDefault() as ExampleMetaAttribute;
    }

    /// <summary>
    /// Helper to create success result with count.
    /// </summary>
    protected static ExampleResult Ok(string id, string title, ExampleTier tier, long elapsedMs, int count, string[] output, string? sql = null)
    {
        return new ExampleResult
        {
            Id = id,
            Title = title,
            Tier = tier,
            Success = true,
            ElapsedMs = elapsedMs,
            Count = count,
            Output = output,
            Sql = sql
        };
    }

    /// <summary>
    /// Helper to create success result without count (for setup/CRUD).
    /// </summary>
    protected static ExampleResult Ok(string id, string title, ExampleTier tier, long elapsedMs, string[] output, string? sql = null)
    {
        return new ExampleResult
        {
            Id = id,
            Title = title,
            Tier = tier,
            Success = true,
            ElapsedMs = elapsedMs,
            Output = output,
            Sql = sql
        };
    }

    /// <summary>
    /// Helper to create failure result.
    /// </summary>
    protected static ExampleResult Fail(string id, string title, ExampleTier tier, long elapsedMs, string error)
    {
        return new ExampleResult
        {
            Id = id,
            Title = title,
            Tier = tier,
            Success = false,
            ElapsedMs = elapsedMs,
            Output = [],
            Error = error
        };
    }
}
