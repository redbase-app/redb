using System;
using System.Threading.Tasks;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// Typed interface for REDB objects with specific properties type
    /// Extends base IRedbObject adding type-safe access to properties
    /// Provides IntelliSense and compile-time type checking
    /// </summary>
    /// <typeparam name="TProps">Type of object properties class</typeparam>
    public interface IRedbObject<TProps> : IRedbObject where TProps : class
    {
        /// <summary>
        /// Typed access to object properties
        /// Replaces the need to work with raw JSON or type casting
        /// </summary>
        TProps Props { get; set; }

        /// <summary>
        /// Get scheme for TProps type (with cache usage)
        /// Convenient method for getting metadata of specific type
        /// </summary>
        Task<IRedbScheme> GetSchemeForTypeAsync();
        
        /// <summary>
        /// Get scheme structures for TProps type (with cache usage)
        /// Returns encapsulated structures from scheme
        /// </summary>
        Task<IReadOnlyCollection<IRedbStructure>> GetStructuresForTypeAsync();
        
        /// <summary>
        /// Get structure by field name for TProps type
        /// Uses fast name lookup in encapsulated scheme
        /// </summary>
        Task<IRedbStructure?> GetStructureByNameAsync(string fieldName);
        
        /// <summary>
        /// Recompute hash based on current TProps type properties
        /// Typed version of RecomputeHash() considering specific type
        /// </summary>
        void RecomputeHashForType();
        
        /// <summary>
        /// Get new hash based on current properties without changing object
        /// Typed version of GetComputedHash()
        /// </summary>
        Guid ComputeHashForType();
        
        /// <summary>
        /// Check if current hash matches TProps type properties
        /// Typed data integrity check
        /// </summary>
        bool IsHashValidForType();
        
        /// <summary>
        /// Create object copy with same metadata but new properties
        /// Useful for creating similar objects
        /// </summary>
        IRedbObject<TProps> CloneWithProperties(TProps newProperties);
        
        /// <summary>
        /// Clear metadata cache for TProps type
        /// Convenient method for cache invalidation at object level
        /// </summary>
        void InvalidateCacheForType();
        
        /// <summary>
        /// Preload metadata cache for TProps type
        /// Useful for performance optimization
        /// </summary>
        Task WarmupCacheForTypeAsync();
    }
}
