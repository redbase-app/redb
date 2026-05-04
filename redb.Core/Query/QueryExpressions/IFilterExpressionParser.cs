using System;
using System.Linq.Expressions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Query.QueryExpressions;

/// <summary>
/// Parser for converting Where expressions to FilterExpression
/// </summary>
public interface IFilterExpressionParser
{
    /// <summary>
    /// Parse lambda expression for filtering by Props fields
    /// </summary>
    FilterExpression ParseFilter<TProps>(Expression<Func<TProps, bool>> predicate) where TProps : class;
    
    /// <summary>
    /// Parse lambda expression for filtering by base IRedbObject fields
    /// (Id, Name, ParentId, DateCreate, ValueLong, ValueString, etc.)
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    FilterExpression ParseRedbFilter(Expression<Func<IRedbObject, bool>> predicate);
}
