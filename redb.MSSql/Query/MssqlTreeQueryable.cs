using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.FacetFilters;
using redb.Core.Query.QueryExpressions;

namespace redb.MSSql.Query;

/// <summary>
/// MS SQL Server tree queryable implementation.
/// Inherits from TreeQueryableBase and provides MSSQL-specific components.
/// </summary>
public class MssqlTreeQueryable<TProps> : TreeQueryableBase<TProps>
    where TProps : class, new()
{
    public MssqlTreeQueryable(
        ITreeQueryProvider provider,
        TreeQueryContext<TProps> context,
        IFilterExpressionParser filterParser,
        IOrderingExpressionParser orderingParser)
        : base(provider, context, filterParser, orderingParser, new FacetFilterBuilder())
    {
    }
    
    /// <summary>
    /// Creates a new instance with the specified context.
    /// </summary>
    protected override TreeQueryableBase<TProps> CreateInstance(TreeQueryContext<TProps> context)
    {
        return new MssqlTreeQueryable<TProps>(_treeProvider, context, _filterParser, _orderingParser);
    }
}

