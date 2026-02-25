using redb.Core.Models.Contracts;

namespace redb.Core.Materialization;

/// <summary>
/// Materializes Props objects from database values.
/// Implementation-specific: Pro uses PVT values, Open-Source uses JSON from SQL functions.
/// </summary>
public interface IPropsMaterializer
{
    /// <summary>
    /// Batch materialize Props for multiple objects.
    /// Loads values from database and populates Props property on each object.
    /// </summary>
    /// <typeparam name="TProps">Props class type</typeparam>
    /// <param name="objects">Objects to materialize Props for</param>
    /// <param name="projectedStructureIds">
    /// Optional: structure IDs to include in projection.
    /// If null, all fields are loaded.
    /// </param>
    Task MaterializeManyAsync<TProps>(
        IReadOnlyList<IRedbObject> objects,
        IEnumerable<long>? projectedStructureIds = null) where TProps : class, new();
}
