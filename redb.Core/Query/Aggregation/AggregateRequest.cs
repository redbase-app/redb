using System.Collections.Generic;

namespace redb.Core.Query.Aggregation;

/// <summary>
/// Request for batch aggregation of multiple fields.
/// </summary>
public class AggregateRequest
{
    /// <summary>Field path (e.g. "Price" or "Items[].Price")</summary>
    public string FieldPath { get; set; } = "";
    
    /// <summary>Aggregation function</summary>
    public AggregateFunction Function { get; set; }
    
    /// <summary>Result alias</summary>
    public string? Alias { get; set; }
}

/// <summary>
/// Batch aggregation result.
/// </summary>
public class AggregateResult
{
    /// <summary>Results by aliases</summary>
    public Dictionary<string, object?> Values { get; set; } = new();
    
    /// <summary>Get value by alias</summary>
    public T? Get<T>(string alias) where T : struct
    {
        if (Values.TryGetValue(alias, out var value) && value != null)
        {
            return (T)Convert.ChangeType(value, typeof(T));
        }
        return null;
    }
    
    /// <summary>Get value by alias (nullable)</summary>
    public T? GetNullable<T>(string alias) where T : struct
    {
        if (Values.TryGetValue(alias, out var value))
        {
            if (value == null) return null;
            return (T)Convert.ChangeType(value, typeof(T));
        }
        return null;
    }
}
