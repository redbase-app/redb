using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.Parsing;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using redb.MSSql.Sql;

namespace redb.MSSql.Query;

/// <summary>
/// MS SQL Server tree query provider implementation.
/// Inherits from TreeQueryProviderBase and provides MSSQL-specific components.
/// </summary>
public class MssqlTreeQueryProvider : TreeQueryProviderBase
{
    public MssqlTreeQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISqlDialect? dialect = null,
        string? cacheDomain = null,
        ISchemeSyncProvider? schemeSync = null)
        : base(context, serializer, dialect ?? new MsSqlDialect(), cacheDomain, lazyPropsLoader, configuration, logger, schemeSync)
    {
    }
    
    /// <summary>
    /// Creates MSSQL-specific filter expression parser.
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
        return new MssqlQueryProvider(_context, _serializer, _lazyPropsLoader, _configuration, _logger, _schemeSync);
    }
    
    /// <summary>
    /// Creates tree queryable instance.
    /// </summary>
    protected override IRedbQueryable<TProps> CreateTreeQueryable<TProps>(TreeQueryContext<TProps> context)
    {
        return new MssqlTreeQueryable<TProps>(this, context, _filterParser, _orderingParser);
    }
}
