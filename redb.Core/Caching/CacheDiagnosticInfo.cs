using System.Collections.Generic;

namespace redb.Core.Caching
{
    /// <summary>
    /// Cache diagnostic information, returned by
    /// <see cref="redb.Core.Models.Contracts.ISchemeCacheProvider.GetCacheDiagnosticInfo"/>.
    /// </summary>
    /// <remarks>
    /// These four types used to live in ICompositeMetadataCache.cs, alongside a layer of cache
    /// interfaces that was never implemented and never consumed. That layer was removed; these
    /// carried on being part of a live public contract, so they moved here unchanged.
    /// </remarks>
    public class CacheDiagnosticInfo
    {
        /// <summary>
        /// Cache health status.
        /// </summary>
        public CacheHealthStatus HealthStatus { get; set; }

        /// <summary>
        /// Potential issues.
        /// </summary>
        public List<string> Issues { get; set; } = new();

        /// <summary>
        /// Optimization recommendations.
        /// </summary>
        public List<string> Recommendations { get; set; } = new();

        /// <summary>
        /// Memory information.
        /// </summary>
        public MemoryUsageInfo MemoryInfo { get; set; } = new();

        /// <summary>
        /// Performance information.
        /// </summary>
        public PerformanceInfo PerformanceInfo { get; set; } = new();
    }

    /// <summary>
    /// Cache health status enumeration.
    /// </summary>
    public enum CacheHealthStatus
    {
        /// <summary>Cache is healthy.</summary>
        Healthy,
        /// <summary>Cache has warnings.</summary>
        Warning,
        /// <summary>Cache is in critical state.</summary>
        Critical,
        /// <summary>Cache status is unknown.</summary>
        Unknown
    }

    /// <summary>
    /// Cache memory usage information.
    /// </summary>
    public class MemoryUsageInfo
    {
        /// <summary>
        /// Used memory in bytes.
        /// </summary>
        public long UsedBytes { get; set; }

        /// <summary>
        /// Maximum memory in bytes (if limited).
        /// </summary>
        public long? MaxBytes { get; set; }

        /// <summary>
        /// Memory usage percentage.
        /// </summary>
        public double UsagePercentage { get; set; }

        /// <summary>
        /// Memory fragmentation percentage (if available).
        /// </summary>
        public double? FragmentationPercentage { get; set; }
    }

    /// <summary>
    /// Cache performance information.
    /// </summary>
    public class PerformanceInfo
    {
        /// <summary>
        /// Average cache access time in milliseconds.
        /// </summary>
        public double AverageAccessTimeMs { get; set; }

        /// <summary>
        /// Average data source load time in milliseconds.
        /// </summary>
        public double AverageLoadTimeMs { get; set; }

        /// <summary>
        /// Operations per second.
        /// </summary>
        public double OperationsPerSecond { get; set; }

        /// <summary>
        /// Peak access time in milliseconds.
        /// </summary>
        public double PeakAccessTimeMs { get; set; }
    }
}
