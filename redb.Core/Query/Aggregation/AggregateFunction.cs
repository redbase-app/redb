namespace redb.Core.Query.Aggregation;

/// <summary>
/// Aggregate function types for EAV fields.
/// </summary>
public enum AggregateFunction
{
    /// <summary>Sum of numeric values</summary>
    Sum,
    
    /// <summary>Arithmetic average</summary>
    Average,
    
    /// <summary>Minimum value</summary>
    Min,
    
    /// <summary>Maximum value</summary>
    Max,
    
    /// <summary>Record count</summary>
    Count
}
