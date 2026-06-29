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
    /// Underlying SQLite connection, used by the analytics partials to package
    /// dynamic-shaped result rows to JSON in C#. SQLite has no row_to_json /
    /// json_agg, so the PVT-compiled inner SQL is executed here and its rows are
    /// serialized via the reader (column names from the reader) — the SQLite analog
    /// of "SELECT json_agg(row_to_json(t)) FROM (sql) t".
    /// </summary>
    private SqliteRedbConnection SqliteConn => (SqliteRedbConnection)_context.Db;
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
