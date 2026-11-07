namespace redb.Core.Query.Grouping;

/// <summary>
/// Represents group of objects with common key.
/// Used in SelectAsync to access Key and aggregations via Agg.*
/// </summary>
public interface IRedbGrouping<out TKey, TProps>
{
    /// <summary>
    /// Group key (value by which grouped).
    /// </summary>
    TKey Key { get; }
}

