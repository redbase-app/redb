using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Entities;

namespace redb.Core.Providers
{
    /// <summary>
    /// Interface for lazy loading of RedbObject Props.
    /// </summary>
    public interface ILazyPropsLoader
    {
        /// <summary>
        /// Synchronous Props loading (for Properties getter).
        /// </summary>
        /// <param name="objectId">Object ID</param>
        /// <param name="schemeId">Scheme ID</param>
        /// <returns>Object Props</returns>
        TProps LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new();
        
        /// <summary>
        /// Asynchronous Props loading (for explicit preload LoadPropsAsync).
        /// </summary>
        /// <param name="objectId">Object ID</param>
        /// <param name="schemeId">Scheme ID</param>
        /// <returns>Object Props</returns>
        Task<TProps> LoadPropsAsync<TProps>(long objectId, long schemeId) where TProps : class, new();
        
        /// <summary>
        /// BULK Props loading for multiple objects with caching and parallelism.
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects) where TProps : class, new();
        
        /// <summary>
        /// OPTIMIZED Props loading with structure_ids filter.
        /// Loads only specified fields from _values (for Select projections).
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="projectedStructureIds">HashSet of structure_ids for _values filtering (null = all)</param>
        Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects, 
            HashSet<long>? projectedStructureIds) where TProps : class, new();
        
        /// <summary>
        /// BULK Props loading with custom depth for nested RedbObject.
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="propsDepth">Maximum depth for nested RedbObject loading (null = use config default)</param>
        Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            int? propsDepth) where TProps : class, new();
        
        /// <summary>
        /// OPTIMIZED Props loading with structure_ids filter and custom depth.
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="projectedStructureIds">HashSet of structure_ids for _values filtering (null = all)</param>
        /// <param name="propsDepth">Maximum depth for nested RedbObject loading (null = use config default)</param>
        Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            HashSet<long>? projectedStructureIds,
            int? propsDepth) where TProps : class, new();
    }
}

