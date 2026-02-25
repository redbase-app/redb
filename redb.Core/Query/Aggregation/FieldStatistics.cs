namespace redb.Core.Query.Aggregation;

/// <summary>
/// Result of GetStatisticsAsync - all aggregates for field.
/// </summary>
public class FieldStatistics<T> where T : struct
{
    /// <summary>Sum of values</summary>
    public decimal Sum { get; set; }
    
    /// <summary>Average value</summary>
    public decimal Average { get; set; }
    
    /// <summary>Minimum value</summary>
    public T? Min { get; set; }
    
    /// <summary>Maximum value</summary>
    public T? Max { get; set; }
    
    /// <summary>Record count</summary>
    public int Count { get; set; }
    
    public override string ToString()
    {
        return $"Sum={Sum}, Avg={Average:F2}, Min={Min}, Max={Max}, Count={Count}";
    }
}
