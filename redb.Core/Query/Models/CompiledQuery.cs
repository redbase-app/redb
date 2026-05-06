namespace redb.Core.Query.Models;

/// <summary>
/// Compiled SQL query with template and metadata.
/// Used for caching and debugging.
/// </summary>
/// <param name="SqlTemplate">SQL template with parameter placeholders ($1, @p0, etc.)</param>
/// <param name="Fields">Information about fields included in the query</param>
/// <param name="DebugComment">Debug comment with parameter values (EF Core style)</param>
public record CompiledQuery(
    string SqlTemplate,
    IReadOnlyList<FieldInfo> Fields,
    string DebugComment
);

