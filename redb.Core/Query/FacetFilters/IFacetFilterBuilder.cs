using System.Collections.Generic;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query.FacetFilters;

/// <summary>
/// JSON filter builder for search_objects_with_facets.
/// </summary>
public interface IFacetFilterBuilder
{
    /// <summary>
    /// Build JSON for facet_filters from FilterExpression.
    /// </summary>
    string BuildFacetFilters(FilterExpression? filter);
    
    /// <summary>
    /// Build JSON for order from OrderingExpression.
    /// </summary>
    string BuildOrderBy(IReadOnlyList<OrderingExpression> orderings);
    
    /// <summary>
    /// Build query parameters (limit, offset).
    /// </summary>
    QueryParameters BuildQueryParameters(int? limit = null, int? offset = null);
}

/// <summary>
/// Query parameters.
/// </summary>
public record QueryParameters(
    int? Limit = null,
    int? Offset = null
);
