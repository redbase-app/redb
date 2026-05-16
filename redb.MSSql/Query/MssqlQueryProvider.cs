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
/// MS SQL Server query provider implementation.
/// Inherits from QueryProviderBase and provides MSSQL-specific components.
/// </summary>
public class MssqlQueryProvider : QueryProviderBase
{
    public MssqlQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISchemeSyncProvider? schemeSync = null,
        ISqlDialect? dialect = null)
        : base(context, serializer, dialect ?? new MsSqlDialect(), lazyPropsLoader, configuration, logger, schemeSync)
    {
    }
    
    /// <summary>
    /// Creates MSSQL-specific filter expression parser.
    /// </summary>
    protected override IFilterExpressionParser CreateFilterParser()
    {
        return new FilterExpressionParser();
    }
}
