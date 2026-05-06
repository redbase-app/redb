using redb.Core.Query;
using redb.Core.Query.Base;
using redb.Core.Query.FacetFilters;
using redb.Core.Query.QueryExpressions;

namespace redb.Postgres.Query;

/// <summary>
/// PostgreSQL tree queryable implementation.
/// Inherits from TreeQueryableBase and provides PostgreSQL-specific components.
/// </summary>
public class PostgresTreeQueryable<TProps> : TreeQueryableBase<TProps>
    where TProps : class, new()
{
    public PostgresTreeQueryable(
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
        return new PostgresTreeQueryable<TProps>(_treeProvider, context, _filterParser, _orderingParser);
    }
}
