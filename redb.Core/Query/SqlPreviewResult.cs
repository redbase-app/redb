namespace redb.Core.Query;

/// <summary>
/// Result of SQL preview function execution.
/// Used for mapping result from get_search_sql_preview() and related functions.
/// </summary>
public class SqlPreviewResult
{
    /// <summary>
    /// Final SQL query as text.
    /// </summary>
    public string sql_preview { get; set; } = string.Empty;
}

