using System;
using System.Threading.Tasks;
using redb.Core.Caching;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using System.Collections.Generic;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for scheme and structure management (Code-First).
    /// </summary>
    public interface ISchemeSyncProvider
    {
        /// <summary>
        /// Domain-bound metadata cache for this provider.
        /// </summary>
        GlobalMetadataCache Cache { get; }
        
        /// <summary>
        /// Domain-bound list cache for this provider.
        /// </summary>
        GlobalListCache ListCache { get; }
        
        /// <summary>
        /// Domain-bound props/object cache for this provider.
        /// </summary>
        GlobalPropsCache PropsCache { get; }
        
        // ===== METHODS WITH CONTRACTS =====

        /// <summary>
        /// Create/get scheme by name from Props type.
        /// If schemeName = null, uses TProps class name.
        /// </summary>
        Task<IRedbScheme> EnsureSchemeFromTypeAsync<TProps>() where TProps : class;

        /// <summary>
        /// Synchronize scheme structures from Props type (by default deletes extra fields).
        /// </summary>
        Task<List<IRedbStructure>> SyncStructuresFromTypeAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true) where TProps : class;

        /// <summary>
        /// Simplified scheme synchronization method with auto-detection of name and alias.
        /// Scheme name and alias determined from RedbSchemeAttribute.
        /// </summary>
        Task<IRedbScheme> SyncSchemeAsync<TProps>() where TProps : class;
        
        // ===== SCHEME SEARCH =====
        
        /// <summary>
        /// Get scheme by ID.
        /// </summary>
        Task<IRedbScheme?> GetSchemeByIdAsync(long schemeId);
        
        /// <summary>
        /// Get scheme by name.
        /// </summary>
        Task<IRedbScheme?> GetSchemeByNameAsync(string schemeName);
        
        /// <summary>
        /// Get scheme by C# class type.
        /// Uses class name for scheme search.
        /// </summary>
        Task<IRedbScheme?> GetSchemeByTypeAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme from cache synchronously (no DB call).
        /// Returns null if not in cache. Use after InitializeAsync() to ensure cache is warm.
        /// </summary>
        IRedbScheme? GetSchemeFromCache<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme from cache synchronously (no DB call).
        /// Returns null if not in cache.
        /// </summary>
        IRedbScheme? GetSchemeFromCache(string schemeName);
        
        /// <summary>
        /// Get scheme by C# class type.
        /// Uses class name for scheme search.
        /// </summary>
        Task<IRedbScheme?> GetSchemeByTypeAsync(Type type);
        
        /// <summary>
        /// Load scheme by C# class type (throws exception if not found).
        /// </summary>
        Task<IRedbScheme> LoadSchemeByTypeAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Load scheme by C# class type (throws exception if not found).
        /// </summary>
        Task<IRedbScheme> LoadSchemeByTypeAsync(Type type);
        
        /// <summary>
        /// Get all schemes.
        /// </summary>
        Task<List<IRedbScheme>> GetSchemesAsync();
        
        /// <summary>
        /// Get scheme structures.
        /// </summary>
        Task<List<IRedbStructure>> GetStructuresAsync(IRedbScheme scheme);
        
        /// <summary>
        /// Get scheme structures by C# class type.
        /// </summary>
        Task<List<IRedbStructure>> GetStructuresByTypeAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme structures by C# class type.
        /// </summary>
        Task<List<IRedbStructure>> GetStructuresByTypeAsync(Type type);
        
        // ===== SCHEME EXISTENCE CHECKS =====
        
        /// <summary>
        /// Check if scheme exists for C# class type.
        /// </summary>
        Task<bool> SchemeExistsForTypeAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Check if scheme exists for C# class type.
        /// </summary>
        Task<bool> SchemeExistsForTypeAsync(Type type);
        
        /// <summary>
        /// Check if scheme exists by name.
        /// </summary>
        Task<bool> SchemeExistsByNameAsync(string schemeName);
        
        // ===== UTILITY METHODS =====
        
        /// <summary>
        /// Get scheme name for C# class type.
        /// Considers attributes and namespaces.
        /// </summary>
        string GetSchemeNameForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme name for C# class type.
        /// Considers attributes and namespaces.
        /// </summary>
        string GetSchemeNameForType(Type type);
        
        /// <summary>
        /// Get scheme alias for C# class type.
        /// Extracts from RedbSchemeAttribute.
        /// </summary>
        string? GetSchemeAliasForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme alias for C# class type.
        /// Extracts from RedbSchemeAttribute.
        /// </summary>
        string? GetSchemeAliasForType(Type type);
        
        // ===== OBJECT SCHEME (NON-GENERIC) =====
        
        /// <summary>
        /// Get or create scheme for Object type (without Props).
        /// Used for non-generic RedbObject that stores only base _objects fields.
        /// Scheme will have _type = Object (not Class).
        /// </summary>
        /// <param name="name">Scheme name (e.g. "RedbObject" or custom name)</param>
        Task<IRedbScheme> EnsureObjectSchemeAsync(string name);
        
        /// <summary>
        /// Get scheme for Object type by name.
        /// Returns null if not found.
        /// </summary>
        Task<IRedbScheme?> GetObjectSchemeAsync(string name);
        
        // ===== TYPE MIGRATION =====
        
        /// <summary>
        /// Migrate data when changing structure type (e.g. String -> Long).
        /// Moves data from old type column to new type column.
        /// </summary>
        /// <param name="structureId">Structure ID to migrate</param>
        /// <param name="oldTypeName">Old type name (e.g. "String")</param>
        /// <param name="newTypeName">New type name (e.g. "Long")</param>
        /// <param name="dryRun">If true, only returns count without actual migration</param>
        /// <returns>Migration result with affected rows count</returns>
        Task<TypeMigrationResult> MigrateStructureTypeAsync(long structureId, string oldTypeName, string newTypeName, bool dryRun = false);
        
        // ===== STRUCTURE TREE CACHE =====
        
        /// <summary>
        /// Get structure tree for scheme (cached).
        /// </summary>
        Task<List<StructureTreeNode>> GetStructureTreeAsync(long schemeId);
        
        /// <summary>
        /// Get subtree starting from parent structure (cached).
        /// </summary>
        Task<List<StructureTreeNode>> GetSubtreeAsync(long schemeId, long? parentStructureId);
        
        /// <summary>
        /// Invalidate structure tree cache for scheme.
        /// </summary>
        void InvalidateStructureTreeCache(long schemeId);
        
        /// <summary>
        /// Get structure tree cache statistics.
        /// </summary>
        (int TreesCount, int SubtreesCount, long MemoryEstimate) GetStructureTreeCacheStats();
    }
    
    /// <summary>
    /// Result of data type migration.
    /// </summary>
    public class TypeMigrationResult
    {
        public int AffectedRows { get; set; }
        public int SuccessCount { get; set; }
        public int ErrorCount { get; set; }
        public string? Errors { get; set; }
    }
}
