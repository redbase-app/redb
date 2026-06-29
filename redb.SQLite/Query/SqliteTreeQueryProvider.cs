using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using redb.SQLite.Sql;

namespace redb.SQLite.Query;

/// <summary>
/// SQLite tree query provider implementation.
/// Inherits from TreeQueryProviderBase and provides SQLite-specific components.
/// </summary>
public partial class SqliteTreeQueryProvider : TreeQueryProviderBase
{
    public SqliteTreeQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISqlDialect? dialect = null,
        string? cacheDomain = null,
        ISchemeSyncProvider? schemeSync = null)
        : base(context, serializer, dialect ?? new SqliteDialect(), cacheDomain, lazyPropsLoader, configuration, logger, schemeSync)
    {
    }
    
    /// <summary>
    /// Creates query provider for delegation.
    /// </summary>
    protected override IRedbQueryProvider CreateQueryProvider()
    {
        return new SqliteQueryProvider(_context, _serializer, _lazyPropsLoader, _configuration, _logger, _schemeSync);
    }
    
    /// <summary>
    /// Creates tree queryable instance.
    /// </summary>
    protected override IRedbQueryable<TProps> CreateTreeQueryable<TProps>(TreeQueryContext<TProps> context)
    {
        return new SqliteTreeQueryable<TProps>(this, context, _filterParser, _orderingParser);
    }
}
