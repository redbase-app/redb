namespace redb.Core.Query.Aggregation;

/// <summary>
/// Static helper for creating aggregate expressions in Select
/// Allows writing: .Select(x => new { Total = Agg.Sum(x.Props.Price) })
/// </summary>
public static class Agg
{
    /// <summary>
    /// Sum of field values
    /// </summary>
    /// <example>
    /// .Select(x => new { Total = Agg.Sum(x.Props.Price) })
    /// .Select(x => new { ItemsTotal = Agg.Sum(x.Props.Items.Select(i => i.Price)) })
    /// </example>
    public static decimal Sum(decimal value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static decimal Sum(decimal? value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static long Sum(long value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static long Sum(long? value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static int Sum(int value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static int Sum(int? value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static double Sum(double value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static double Sum(double? value) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    
    /// <summary>
    /// Sum of values from a collection (for arrays)
    /// </summary>
    public static decimal Sum(IEnumerable<decimal> values) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static decimal Sum(IEnumerable<decimal?> values) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static long Sum(IEnumerable<long> values) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static int Sum(IEnumerable<int> values) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");
    public static double Sum(IEnumerable<double> values) => throw new NotSupportedException("Agg.Sum must be processed in ProjectionFieldExtractor");

    /// <summary>
    /// Average value of a field
    /// </summary>
    public static decimal Average(decimal value) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static decimal Average(decimal? value) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static double Average(long value) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static double Average(int value) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static double Average(double value) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    
    /// <summary>
    /// Average value from a collection (for arrays)
    /// </summary>
    public static decimal Average(IEnumerable<decimal> values) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static double Average(IEnumerable<long> values) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static double Average(IEnumerable<int> values) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");
    public static double Average(IEnumerable<double> values) => throw new NotSupportedException("Agg.Average must be processed in ProjectionFieldExtractor");

    /// <summary>
    /// Minimum value of a field
    /// </summary>
    public static T Min<T>(T value) => throw new NotSupportedException("Agg.Min must be processed in ProjectionFieldExtractor");
    
    /// <summary>
    /// Minimum value from a collection
    /// </summary>
    public static T Min<T>(IEnumerable<T> values) => throw new NotSupportedException("Agg.Min must be processed in ProjectionFieldExtractor");

    /// <summary>
    /// Maximum value of a field
    /// </summary>
    public static T Max<T>(T value) => throw new NotSupportedException("Agg.Max must be processed in ProjectionFieldExtractor");
    
    /// <summary>
    /// Maximum value from a collection
    /// </summary>
    public static T Max<T>(IEnumerable<T> values) => throw new NotSupportedException("Agg.Max must be processed in ProjectionFieldExtractor");

    /// <summary>
    /// Count of elements in a collection
    /// </summary>
    public static int Count<T>(IEnumerable<T> values) => throw new NotSupportedException("Agg.Count must be processed in ProjectionFieldExtractor");
    
    /// <summary>
    /// Count of records (COUNT(*))
    /// </summary>
    public static int Count() => throw new NotSupportedException("Agg.Count must be processed in AggregateAsync");
    
    // ===== GROUPBY OVERLOADS =====
    
    /// <summary>
    /// Sum in a group: Agg.Sum(g, x => x.Stock)
    /// </summary>
    public static TValue Sum<TKey, TProps, TValue>(Grouping.IRedbGrouping<TKey, TProps> group, System.Linq.Expressions.Expression<Func<TProps, TValue>> selector) 
        where TProps : class, new()
        => throw new NotSupportedException("Agg.Sum for GroupBy");
    
    /// <summary>
    /// Average in a group: Agg.Average(g, x => x.Age)
    /// </summary>
    public static double Average<TKey, TProps, TValue>(Grouping.IRedbGrouping<TKey, TProps> group, System.Linq.Expressions.Expression<Func<TProps, TValue>> selector) 
        where TProps : class, new()
        => throw new NotSupportedException("Agg.Average for GroupBy");
    
    /// <summary>
    /// Minimum in a group: Agg.Min(g, x => x.Price)
    /// </summary>
    public static TValue Min<TKey, TProps, TValue>(Grouping.IRedbGrouping<TKey, TProps> group, System.Linq.Expressions.Expression<Func<TProps, TValue>> selector) 
        where TProps : class, new()
        => throw new NotSupportedException("Agg.Min for GroupBy");
    
    /// <summary>
    /// Maximum in a group: Agg.Max(g, x => x.Price)
    /// </summary>
    public static TValue Max<TKey, TProps, TValue>(Grouping.IRedbGrouping<TKey, TProps> group, System.Linq.Expressions.Expression<Func<TProps, TValue>> selector) 
        where TProps : class, new()
        => throw new NotSupportedException("Agg.Max for GroupBy");
    
    /// <summary>
    /// Count in a group: Agg.Count(g)
    /// </summary>
    public static int Count<TKey, TProps>(Grouping.IRedbGrouping<TKey, TProps> group) 
        where TProps : class, new()
        => throw new NotSupportedException("Agg.Count for GroupBy");
}
