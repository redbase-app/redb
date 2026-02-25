using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using redb.Core.Models.Contracts;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Window specification for grouped queries.
/// </summary>
public class GroupedWindowSpec<TKey, TProps> : IGroupedWindowSpec<TKey, TProps>
    where TProps : class, new()
{
    internal List<LambdaExpression> PartitionByExpressions { get; } = [];
    internal List<(LambdaExpression Expression, bool Descending)> OrderByExpressions { get; } = [];

    public IGroupedWindowSpec<TKey, TProps> PartitionBy<TField>(
        Expression<Func<TKey, TField>> keySelector)
    {
        PartitionByExpressions.Add(keySelector);
        return this;
    }

    public IGroupedWindowSpec<TKey, TProps> OrderBy<TField>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TField>> orderSelector)
    {
        OrderByExpressions.Add((orderSelector, false));
        return this;
    }

    public IGroupedWindowSpec<TKey, TProps> OrderByDesc<TField>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TField>> orderSelector)
    {
        OrderByExpressions.Add((orderSelector, true));
        return this;
    }
}
