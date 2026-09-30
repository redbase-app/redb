using System;
using System.Reflection;
using System.Threading.Tasks;

namespace redb.Core.Extensions
{
    /// <summary>
    /// Obsolete initialization entry points, kept for callers that invoke them explicitly. Both delegate to
    /// <see cref="IRedbService.InitializeAsync(Assembly[])"/>.
    /// </summary>
    /// <remarks>
    /// INIT-1 (review 2026-09-24): these used to carry a copy of the start-up sequence. The copy synchronized the
    /// schemes in parallel on one service instance (Task.WhenAll) - the one thing a single instance does not
    /// support - skipped the up-front validation of scheme names, and swallowed every synchronization error in a
    /// bare catch. The service's own InitializeAsync does all of it sequentially and loudly, and more.
    /// </remarks>
    public static class RedbServiceInitializationExtensions
    {
        /// <summary>
        /// Initializes REDB at application start-up. Same as <c>await redb.InitializeAsync(assemblies)</c>,
        /// which C# already picks over this extension when called on an instance.
        /// </summary>
        /// <param name="redb">IRedbService instance</param>
        /// <param name="assemblies">Assemblies to scan. If not specified - all loaded assemblies are scanned</param>
        [Obsolete("Use await redb.InitializeAsync() directly - method is integrated into IRedbService")]
        public static Task InitializeAsync(this IRedbService redb, params Assembly[] assemblies)
            => redb.InitializeAsync(assemblies);

        /// <summary>
        /// Synchronizes every scheme with <c>[RedbScheme]</c> - as part of the full initialization, which is the
        /// only supported way to do it.
        /// </summary>
        /// <param name="redb">IRedbService instance</param>
        /// <param name="assemblies">Assemblies to scan. If not specified - all loaded are scanned</param>
        [Obsolete("Use await redb.InitializeAsync() - method includes scheme synchronization")]
        public static Task AutoSyncSchemesAsync(this IRedbService redb, params Assembly[] assemblies)
            => redb.InitializeAsync(assemblies);
    }
}
