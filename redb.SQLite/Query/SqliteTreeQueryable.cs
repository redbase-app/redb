using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.FacetFilters;
using redb.Core.Query.QueryExpressions;

namespace redb.SQLite.Query;

/// <summary>
/// SQLite tree queryable implementation.
/// Inherits from TreeQueryableBase and provides SQLite-specific components.
/// </summary>
public class SqliteTreeQueryable<TProps> : TreeQueryableBase<TProps>
    where TProps : class, new()
{
    public SqliteTreeQueryable(
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
        return new SqliteTreeQueryable<TProps>(_treeProvider, context, _filterParser, _orderingParser);
    }
}
