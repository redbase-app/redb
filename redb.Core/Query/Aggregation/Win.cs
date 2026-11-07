namespace redb.Core.Query.Aggregation;

/// <summary>
/// Static helper for window functions
/// Used in Select to create ROW_NUMBER(), RANK(), SUM() OVER(), etc. expressions.
/// </summary>
public static class Win
{
    // ===== RANKING FUNCTIONS =====
    
    /// <summary>
    /// ROW_NUMBER() OVER (...)
    /// </summary>
    public static long RowNumber() 
        => throw new NotSupportedException("Win.RowNumber must be processed in WindowQueryable");
    
    /// <summary>
    /// RANK() OVER (...)
    /// </summary>
    public static long Rank() 
        => throw new NotSupportedException("Win.Rank must be processed in WindowQueryable");
    
    /// <summary>
    /// DENSE_RANK() OVER (...)
    /// </summary>
    public static long DenseRank() 
        => throw new NotSupportedException("Win.DenseRank must be processed in WindowQueryable");
    
    /// <summary>
    /// NTILE(n) OVER (...)
    /// </summary>
    public static int Ntile(int buckets) 
        => throw new NotSupportedException("Win.Ntile must be processed in WindowQueryable");
    
    // ===== AGGREGATE FUNCTIONS =====
    
    /// <summary>
    /// SUM(field) OVER (...)
    /// </summary>
    public static decimal Sum(decimal value) 
        => throw new NotSupportedException("Win.Sum must be processed in WindowQueryable");
    public static long Sum(long value) 
        => throw new NotSupportedException("Win.Sum must be processed in WindowQueryable");
    public static int Sum(int value) 
        => throw new NotSupportedException("Win.Sum must be processed in WindowQueryable");
    public static double Sum(double value) 
        => throw new NotSupportedException("Win.Sum must be processed in WindowQueryable");
    
    /// <summary>
    /// AVG(field) OVER (...)
    /// </summary>
    public static double Avg(decimal value) 
        => throw new NotSupportedException("Win.Avg must be processed in WindowQueryable");
    public static double Avg(long value) 
        => throw new NotSupportedException("Win.Avg must be processed in WindowQueryable");
    public static double Avg(int value) 
        => throw new NotSupportedException("Win.Avg must be processed in WindowQueryable");
    public static double Avg(double value) 
        => throw new NotSupportedException("Win.Avg must be processed in WindowQueryable");
    
    /// <summary>
    /// COUNT(*) OVER (...)
    /// </summary>
    public static int Count() 
        => throw new NotSupportedException("Win.Count must be processed in WindowQueryable");
    
    /// <summary>
    /// MIN(field) OVER (...)
    /// </summary>
    public static T Min<T>(T value) 
        => throw new NotSupportedException("Win.Min must be processed in WindowQueryable");
    
    /// <summary>
    /// MAX(field) OVER (...)
    /// </summary>
    public static T Max<T>(T value) 
        => throw new NotSupportedException("Win.Max must be processed in WindowQueryable");
    
    // ===== OFFSET FUNCTIONS =====
    
    /// <summary>
    /// LAG(field) OVER (...)
    /// </summary>
    public static T Lag<T>(T value) 
        => throw new NotSupportedException("Win.Lag must be processed in WindowQueryable");
    
    /// <summary>
    /// LEAD(field) OVER (...)
    /// </summary>
    public static T Lead<T>(T value) 
        => throw new NotSupportedException("Win.Lead must be processed in WindowQueryable");
    
    /// <summary>
    /// FIRST_VALUE(field) OVER (...)
    /// </summary>
    public static T FirstValue<T>(T value) 
        => throw new NotSupportedException("Win.FirstValue must be processed in WindowQueryable");
    
    /// <summary>
    /// LAST_VALUE(field) OVER (...)
    /// </summary>
    public static T LastValue<T>(T value) 
        => throw new NotSupportedException("Win.LastValue must be processed in WindowQueryable");
}
