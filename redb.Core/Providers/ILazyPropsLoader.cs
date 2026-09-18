using System.Collections.Generic;
using System.Threading;
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
        /// <returns>Object Props, or null when the target is gone (moved to the trash) - V4 (L.3, LAZY �4.8)</returns>
        TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new();

        /// <summary>
        /// The database (cache domain) this loader reads. A loader bound to one scope names its database: the reference
        /// stubs it materializes then carry the one scope-free loader of that database, which loads on the live scope of
        /// whoever reads them (owner decision 2026-09-15). Null for a loader bound to no database.
        /// </summary>
        string? CacheDomain => null;

        /// <summary>
        /// The context (one scope, one connection) this loader is bound to. The stubs and list items it materializes keep
        /// the service of that scope as their origin: when no redb scope is current for the reader, they load on the origin
        /// while it lives and the instance is not shared (owner decision 2026-09-15). Null for a loader bound to no scope.
        /// </summary>
        Data.IRedbContext? ScopeContext => null;
        
        /// <summary>
        /// Asynchronous Props loading (for explicit preload LoadPropsAsync).
        /// </summary>
        /// <param name="objectId">Object ID</param>
        /// <param name="schemeId">Scheme ID</param>
        /// <returns>Object Props, or null when the target is gone (moved to the trash) - V4 (L.3, LAZY �4.8)</returns>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId,
        CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// BULK Props loading for multiple objects with caching and parallelism.
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects,
        CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// OPTIMIZED Props loading with structure_ids filter.
        /// Loads only specified fields from _values (for Select projections).
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="projectedStructureIds">HashSet of structure_ids for _values filtering (null = all)</param>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects, 
            HashSet<long>? projectedStructureIds,
        CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// BULK Props loading with custom depth for nested RedbObject.
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="propsDepth">Maximum depth for nested RedbObject loading (null = use config default)</param>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            int? propsDepth,
        CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// OPTIMIZED Props loading with structure_ids filter and custom depth.
        /// </summary>
        /// <param name="objects">List of objects to load Props for</param>
        /// <param name="projectedStructureIds">HashSet of structure_ids for _values filtering (null = all)</param>
        /// <param name="propsDepth">Maximum depth for nested RedbObject loading (null = use config default)</param>
        /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
        Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            HashSet<long>? projectedStructureIds,
            int? propsDepth,
        CancellationToken cancellationToken = default) where TProps : class, new();
    }
}

