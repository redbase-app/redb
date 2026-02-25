using redb.Core.Query;
using redb.Core.Providers;
using redb.Core.Data;
using System;
using System.Reflection;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Models.Configuration;

namespace redb.Core
{
    /// <summary>
    /// Main REDB service interface - composition of all providers.
    /// </summary>
    public interface IRedbService : 
        ISchemeSyncProvider,
        IObjectStorageProvider,
        ITreeProvider,
        IPermissionProvider,
        IQueryableProvider,
        IValidationProvider
    {
        // === DATABASE CONTEXT ===
        
        /// <summary>
        /// Database context for direct SQL queries.
        /// Replaces EF Core RedbContext.
        /// </summary>
        IRedbContext Context { get; }
        
        // === PROVIDERS ===
        
        /// <summary>
        /// Provider for user management.
        /// </summary>
        IUserProvider UserProvider { get; }
        
        /// <summary>
        /// Provider for role management.
        /// </summary>
        IRoleProvider RoleProvider { get; }
        
        /// <summary>
        /// Provider for list management.
        /// </summary>
        IListProvider ListProvider { get; }
        
        // === CONFIGURATION ===
        
        /// <summary>
        /// Current service configuration.
        /// </summary>
        RedbServiceConfiguration Configuration { get; }
        
        /// <summary>
        /// Cache domain identifier for this service instance.
        /// Used to isolate caches between different database connections.
        /// </summary>
        string CacheDomain { get; }
        
        /// <summary>
        /// Update configuration.
        /// </summary>
        void UpdateConfiguration(Action<RedbServiceConfiguration> configure);
        
        /// <summary>
        /// Update configuration via builder.
        /// </summary>
        void UpdateConfiguration(Action<RedbServiceConfigurationBuilder> configureBuilder);
        
        // === SECURITY CONTEXT ===
        
        /// <summary>
        /// Security context for user and permission management.
        /// </summary>
        IRedbSecurityContext SecurityContext { get; }
        
        /// <summary>
        /// Set current user.
        /// </summary>
        void SetCurrentUser(IRedbUser user);
        
        /// <summary>
        /// Create temporary system context.
        /// </summary>
        IDisposable CreateSystemContext();
        
        /// <summary>
        /// Get effective user ID with fallback logic.
        /// </summary>
        long GetEffectiveUserId();
        
        // === INITIALIZATION ===
        
        /// <summary>
        /// Initialize REDB system at application startup.
        /// Automatically performs:
        /// 1. Synchronization of all schemes with RedbSchemeAttribute
        /// 2. RedbObjectFactory initialization
        /// 3. AutomaticTypeRegistry initialization
        /// 4. GlobalPropsCache initialization (if enabled in configuration)
        /// 5. Setting global scheme provider for RedbObject
        /// </summary>
        /// <param name="assemblies">Assemblies to scan. If not specified - all loaded assemblies are scanned</param>
        Task InitializeAsync(params Assembly[] assemblies);

        // === DATABASE METADATA ===
        
        /// <summary>
        /// Database version.
        /// </summary>

        // === DATABASE SCHEMA MANAGEMENT ===
        
        /// <summary>
        /// Ensures the REDB database schema exists.
        /// If the core tables are not found, executes the full initialization script.
        /// Safe to call on every startup — idempotent.
        /// </summary>
        Task EnsureDatabaseAsync();

        /// <summary>
        /// Returns the full SQL script that creates the REDB schema.
        /// Useful for manual database setup or CI/CD pipelines.
        /// </summary>
        string GetSchemaScript();

        /// <summary>
        /// Initialize REDB system at application startup, optionally creating the database schema first.
        /// </summary>
        /// <param name="ensureCreated">If true, calls <see cref="EnsureDatabaseAsync"/> before initialization.</param>
        /// <param name="assemblies">Assemblies to scan. If not specified — all loaded assemblies are scanned.</param>
        Task InitializeAsync(bool ensureCreated, params Assembly[] assemblies);

        string dbVersion { get; }
        
        /// <summary>
        /// Database type (e.g., "PostgreSQL").
        /// </summary>
        string dbType { get; }
        
        /// <summary>
        /// Database migration version.
        /// </summary>
        string dbMigration { get; }
        
        /// <summary>
        /// Database size in MB (optional).
        /// </summary>
        long? dbSize { get; }
    }
}
