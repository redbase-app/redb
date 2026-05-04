using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.Parsing;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using redb.Postgres.Sql;

namespace redb.Postgres.Query;

/// <summary>
/// PostgreSQL tree query provider implementation.
/// Inherits from TreeQueryProviderBase and provides PostgreSQL-specific components.
/// </summary>
public class PostgresTreeQueryProvider : TreeQueryProviderBase
{
    public PostgresTreeQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISqlDialect? dialect = null,
        string? cacheDomain = null,
        ISchemeSyncProvider? schemeSync = null)
        : base(context, serializer, dialect ?? new PostgreSqlDialect(), cacheDomain, lazyPropsLoader, configuration, logger, schemeSync)
    {
    }
    
    /// <summary>
    /// Creates PostgreSQL-specific filter expression parser.
    /// </summary>
    protected override IFilterExpressionParser CreateFilterParser()
    {
        return new FilterExpressionParser();
    }
    
    /// <summary>
    /// Creates query provider for delegation.
    /// </summary>
    protected override IRedbQueryProvider CreateQueryProvider()
    {
        return new PostgresQueryProvider(_context, _serializer, _lazyPropsLoader, _configuration, _logger, _schemeSync);
    }
    
    /// <summary>
    /// Creates tree queryable instance.
    /// </summary>
    protected override IRedbQueryable<TProps> CreateTreeQueryable<TProps>(TreeQueryContext<TProps> context)
    {
        return new PostgresTreeQueryable<TProps>(this, context, _filterParser, _orderingParser);
    }
}
