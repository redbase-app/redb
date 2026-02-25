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
/// PostgreSQL query provider implementation.
/// Inherits from QueryProviderBase and provides PostgreSQL-specific components.
/// </summary>
public partial class PostgresQueryProvider : QueryProviderBase
{
    public PostgresQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISchemeSyncProvider? schemeSync = null,
        ISqlDialect? dialect = null)
        : base(context, serializer, dialect ?? new PostgreSqlDialect(), lazyPropsLoader, configuration, logger, schemeSync)
    {
    }
    
    /// <summary>
    /// Creates PostgreSQL-specific filter expression parser.
    /// </summary>
    protected override IFilterExpressionParser CreateFilterParser()
    {
        return new FilterExpressionParser();
    }
}
