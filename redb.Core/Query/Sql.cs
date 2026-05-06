using System;

namespace redb.Core.Query;

/// <summary>
/// SQL functions for use in LINQ expressions.
/// Pro Only: Throws RedbProRequiredException in Open Source version.
/// </summary>
public static class Sql
{
    /// <summary>
    /// Call custom SQL function.
    /// </summary>
    /// <typeparam name="TResult">Return value type</typeparam>
    /// <param name="functionName">SQL function name (e.g.: "COALESCE", "POWER", "my_custom_func")</param>
    /// <param name="args">Function arguments (properties, constants, expressions)</param>
    /// <returns>Function result (used only for typing)</returns>
    /// <example>
    /// COALESCE(pvt."Stock", 0)
    /// .Where(p => Sql.Function&lt;long&gt;("COALESCE", p.Stock, 0) > 50)
    /// 
    /// POWER(pvt."Age", 2)
    /// .Where(p => Sql.Function&lt;double&gt;("POWER", p.Age, 2) > 100)
    /// 
    /// my_custom_func(pvt."Name", pvt."Code")
    /// .Where(p => Sql.Function&lt;string&gt;("my_custom_func", p.Name, p.Code) == "result")
    /// </example>
    public static TResult Function<TResult>(string functionName, params object?[] args)
    {
        throw new InvalidOperationException(
            "Sql.Function<T>() can only be used within LINQ expressions (Where, WhereRedb, etc.). " +
            "It cannot be called directly.");
    }
}

