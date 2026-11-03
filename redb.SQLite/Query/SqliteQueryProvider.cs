using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using redb.SQLite.Data;
using redb.SQLite.Sql;

namespace redb.SQLite.Query;

/// <summary>
/// SQLite query provider implementation.
/// Inherits from QueryProviderBase and provides SQLite-specific components.
/// </summary>
public partial class SqliteQueryProvider : QueryProviderBase
{
    /// <summary>
    /// The first row of <paramref name="sql"/> as a JSON object, or null when there is none. SQLite has no
    /// row_to_json, so the analytics read their rows through IRedbContext.QueryRowsAsJsonAsync (one command and one
    /// telemetry span, as on the other providers); they used to read on the raw connection, which opened no span.
    /// </summary>
    private async Task<string?> FirstRowAsJsonAsync(string sql)
    {
        var json = await _context.QueryRowsAsJsonAsync(sql, System.Array.Empty<object>());
        using var rows = System.Text.Json.JsonDocument.Parse(json);
        return rows.RootElement.GetArrayLength() == 0 ? null : rows.RootElement[0].GetRawText();
    }
    public SqliteQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISchemeSyncProvider? schemeSync = null,
        ISqlDialect? dialect = null)
        : base(context, serializer, dialect ?? new SqliteDialect(), lazyPropsLoader, configuration, logger, schemeSync)
    {
    }
}
