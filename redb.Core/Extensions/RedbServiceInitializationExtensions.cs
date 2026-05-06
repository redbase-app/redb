using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading.Tasks;
using redb.Core.Models;
using redb.Core.Attributes;
using redb.Core.Providers;
using redb.Core.Utils;

namespace redb.Core.Extensions
{
    /// <summary>
    /// Extension methods for initializing REDB system at application startup
    /// </summary>
    public static class RedbServiceInitializationExtensions
    {
        /// <summary>
        /// Unified initialization point for REDB system on application startup.
        /// Executes all necessary steps in correct order:
        /// 1. Automatic synchronization of all schemes with RedbSchemeAttribute
        /// 2. RedbObjectFactory initialization for object creation
        /// 3. AutomaticTypeRegistry initialization for polymorphic operations
        /// 
        /// Usage example:
        /// <code>
        /// Option 1: Automatically finds all loaded assemblies
        /// await redb.InitializeAsync();
        /// 
        /// Option 2: Explicitly specify assemblies to scan
        /// await redb.InitializeAsync(typeof(MyModel).Assembly, typeof(AnotherModel).Assembly);
        /// 
        /// Option 3: Current assembly only
        /// await redb.InitializeAsync(Assembly.GetExecutingAssembly());
        /// </code>
        /// </summary>
        /// <param name="redb">IRedbService instance</param>
        /// <param name="assemblies">Assemblies to scan. If not specified - all loaded assemblies are scanned</param>
        /// <returns>Task for async initialization</returns>
        [Obsolete("Use await redb.InitializeAsync() directly - method is integrated into IRedbService")]
        public static async Task InitializeAsync(
            this IRedbService redb, 
            params Assembly[] assemblies)
        {
            // 1. Synchronize all schemes with RedbSchemeAttribute
            await redb.AutoSyncSchemesAsync(assemblies);

            // 2. Initialize object factory
            RedbObjectFactory.Initialize(redb);

            // 3. Initialize type registry for polymorphic operations
            // Use reflection to avoid dependency on specific implementation (Postgres/MSSql/SQLite)
            var treeProvider = redb.GetType()
                .GetMethod("GetTreeProvider")
                ?.Invoke(redb, null) as ITreeProvider;
            
            if (treeProvider != null)
            {
                await treeProvider.InitializeTypeRegistryAsync();
            }
        }

        /// <summary>
        /// Automatic synchronization of all schemes with RedbSchemeAttribute.
        /// Scans specified assemblies (or all loaded) and synchronizes
        /// all types marked with [RedbScheme] attribute.
        /// Synchronization is performed in parallel for maximum performance.
        /// 
        /// Usage example:
        /// <code>
        /// Automatic synchronization of all schemes
        /// await redb.AutoSyncSchemesAsync();
        /// 
        /// Synchronization only from specified assemblies
        /// await redb.AutoSyncSchemesAsync(typeof(MyModel).Assembly);
        /// </code>
        /// </summary>
        /// <param name="redb">IRedbService instance</param>
        /// <param name="assemblies">Assemblies to scan. If not specified - all loaded are scanned</param>
        /// <returns>Task for async synchronization</returns>
        [Obsolete("Use await redb.InitializeAsync() - method includes scheme synchronization")]
        public static async Task AutoSyncSchemesAsync(
            this IRedbService redb, 
            params Assembly[] assemblies)
        {
            // Determine assemblies to scan
            IEnumerable<Assembly> assembliesToScan = assemblies.Length > 0
                ? assemblies
                : GetAllLoadedAssemblies();

            // Find all types with RedbSchemeAttribute
            var typesToSync = assembliesToScan
                .SelectMany(a => GetTypesWithRedbSchemeAttribute(a))
                .ToList();

            if (typesToSync.Count == 0)
            {
                // No types to synchronize
                return;
            }

            // Parallel synchronization of all schemes for maximum performance
            var tasks = typesToSync.Select(type => SyncSchemeForTypeAsync(redb, type));
            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Get all loaded assemblies (with support for different .NET versions).
        /// Uses modern API for .NET 5+ and legacy API for .NET Framework.
        /// </summary>
        /// <returns>Collection of loaded assemblies</returns>
        private static IEnumerable<Assembly> GetAllLoadedAssemblies()
        {
#if NET5_0_OR_GREATER
            // .NET 5/6/7/8/9 - modern approach via AssemblyLoadContext
            return AssemblyLoadContext.Default.Assemblies;
#else
            // .NET Framework - legacy approach via AppDomain
            return AppDomain.CurrentDomain.GetAssemblies();
#endif
        }

        /// <summary>
        /// Get all types with RedbSchemeAttribute from specified assembly.
        /// Handles ReflectionTypeLoadException for problematic assemblies.
        /// </summary>
        /// <param name="assembly">Assembly to scan</param>
        /// <returns>Collection of types with RedbSchemeAttribute</returns>
        private static IEnumerable<Type> GetTypesWithRedbSchemeAttribute(Assembly assembly)
        {
            try
            {
                return assembly.GetTypes()
                    .Where(t => t.GetCustomAttribute<RedbSchemeAttribute>() != null);
            }
            catch (ReflectionTypeLoadException ex)
            {
                // If couldn't load all types from assembly,
                // return types that were loaded successfully
                return ex.Types
                    .Where(t => t != null && t.GetCustomAttribute<RedbSchemeAttribute>() != null)!;
            }
            catch (Exception)
            {
                // Ignore problematic assemblies (e.g. system or without access)
                return Enumerable.Empty<Type>();
            }
        }

        /// <summary>
        /// Synchronize scheme for specific type.
        /// Uses reflection to call generic method SyncSchemeAsync&lt;T&gt;()
        /// </summary>
        /// <param name="redb">IRedbService instance</param>
        /// <param name="type">Type for scheme synchronization</param>
        /// <returns>Task for async synchronization</returns>
        private static async Task SyncSchemeForTypeAsync(IRedbService redb, Type type)
        {
            try
            {
                // Get generic method SyncSchemeAsync&lt;TProps&gt;()
                var method = typeof(ISchemeSyncProvider)
                    .GetMethod(nameof(ISchemeSyncProvider.SyncSchemeAsync))
                    ?.MakeGenericMethod(type);

                if (method != null)
                {
                    // Call method via reflection
                    var task = method.Invoke(redb, null);
                    if (task is Task asyncTask)
                    {
                        await asyncTask;
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors for specific type,
                // to not interrupt synchronization of other types
                // In production this should be logged
            }
        }
    }
}

