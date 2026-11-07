using System;
using System.Collections.Generic;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// A grouping over the elements of an array, as every provider receives it: what to group, how to aggregate, which
/// objects take part and, for <c>WithWindow</c>, the window over the groups. One request serves execution and SQL
/// preview, so the previewed SQL is the SQL that runs.
/// </summary>
public sealed class ArrayGroupQuery
{
    /// <summary>Scheme of the objects that own the array.</summary>
    public required long SchemeId { get; init; }

    /// <summary>
    /// Path to the array whose elements are grouped, through classes and arrays, joined with dots:
    /// <c>Contacts</c>, <c>Contacts.Metadata</c>, <c>HomeAddress.Building.Amenities</c>. Keys and aggregates read the
    /// elements of the last array in the path.
    /// </summary>
    public required string ArrayPath { get; init; }

    /// <summary>
    /// Group keys: paths of element fields (<c>Type</c>, <c>Address.City</c>) or <see cref="GroupFieldRequest.ElementPath"/>
    /// for the element of an array of simple values. Never empty.
    /// </summary>
    public required IReadOnlyList<GroupFieldRequest> GroupFields { get; init; }

    /// <summary>Aggregates over the elements of each group, with their aliases.</summary>
    public required IReadOnlyList<AggregateRequest> Aggregations { get; init; }

    /// <summary>Condition on the owning objects (the query's Where), or null.</summary>
    public FilterExpression? Filter { get; init; }

    /// <summary>
    /// Condition on the owning objects in the facet JSON of the free provider, or null. Used by the free tree path
    /// to pass the objects of a subtree; the Pro provider refuses a non-empty value.
    /// </summary>
    public string? FilterJson { get; init; }

    /// <summary>HAVING in the JSON of <c>HavingPredicateParser</c>, or null.</summary>
    public string? HavingJson { get; init; }

    /// <summary>Window over the groups (<c>WithWindow</c>), or null.</summary>
    public ArrayGroupWindow? Window { get; init; }

    /// <summary>
    /// Refuses a request no generator can answer: no array path, no key, a key or aggregate without a path or alias,
    /// a window without functions. Every provider calls it first, so none of them answers such a request with nothing.
    /// </summary>
    public void EnsureValid()
    {
        if (string.IsNullOrWhiteSpace(ArrayPath))
            throw new ArgumentException("An array grouping needs the path of the array.", nameof(ArrayPath));
        if (GroupFields.Count == 0)
            throw new ArgumentException("An array grouping needs at least one key.", nameof(GroupFields));
        foreach (var g in GroupFields)
        {
            if (string.IsNullOrWhiteSpace(g.FieldPath))
                throw new ArgumentException("A key of an array grouping has no field path.", nameof(GroupFields));
            if (string.IsNullOrWhiteSpace(g.Alias))
                throw new ArgumentException($"The key '{g.FieldPath}' of an array grouping has no alias.", nameof(GroupFields));
        }
        foreach (var a in Aggregations)
        {
            if (string.IsNullOrWhiteSpace(a.FieldPath))
                throw new ArgumentException($"The aggregate '{a.Alias}' of an array grouping has no field path.", nameof(Aggregations));
            if (string.IsNullOrWhiteSpace(a.Alias))
                throw new ArgumentException($"The aggregate {a.Function}({a.FieldPath}) of an array grouping has no alias.", nameof(Aggregations));
            if (a.ConditionJson != null && a.Function != AggregateFunction.Count)
                throw new ArgumentException($"Only a count takes a condition; {a.Function}({a.FieldPath}) has one.", nameof(Aggregations));
        }
        if (Window != null && Window.Functions.Count == 0)
            throw new ArgumentException("A window over an array grouping needs at least one window function.", nameof(Window));
    }
}

/// <summary>
/// Window functions over the rows of an array grouping, in the form the grouped window of GroupBy uses.
/// </summary>
public sealed class ArrayGroupWindow
{
    /// <summary>Window functions with their aliases. Never empty.</summary>
    public required IReadOnlyList<WindowFuncRequest> Functions { get; init; }

    /// <summary>PARTITION BY: group aliases or aggregate references.</summary>
    public required IReadOnlyList<WindowFieldRequest> PartitionBy { get; init; }

    /// <summary>ORDER BY: group aliases or aggregate references.</summary>
    public required IReadOnlyList<WindowOrderRequest> OrderBy { get; init; }
}
