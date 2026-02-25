using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Query.QueryExpressions;

/// <summary>
/// Parser for converting OrderBy expressions to OrderingExpression
/// </summary>
public interface IOrderingExpressionParser
{
    /// <summary>
    /// Parse sorting expression (Props fields)
    /// </summary>
    OrderingExpression ParseOrdering<TProps, TKey>(Expression<Func<TProps, TKey>> keySelector, SortDirection direction) where TProps : class;
    
    /// <summary>
    /// 🆕 Parse sorting expression by base IRedbObject fields (id, name, date_create, etc.)
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    OrderingExpression ParseRedbOrdering<TKey>(Expression<Func<IRedbObject, TKey>> keySelector, SortDirection direction);
    
    /// <summary>
    /// Parse multiple sorting
    /// </summary>
    IReadOnlyList<OrderingExpression> ParseMultipleOrderings<TProps>(IEnumerable<(LambdaExpression KeySelector, SortDirection Direction)> orderings) where TProps : class;
}
