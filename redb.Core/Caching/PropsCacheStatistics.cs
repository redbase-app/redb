namespace redb.Core.Caching
{
    /// <summary>
    /// Props object cache statistics.
    /// </summary>
    public class PropsCacheStatistics
    {
        /// <summary>
        /// Total cache entries.
        /// </summary>
        public int TotalEntries { get; set; }
        
        /// <summary>
        /// Cache hit count.
        /// </summary>
        public long HitCount { get; set; }
        
        /// <summary>
        /// Cache miss count.
        /// </summary>
        public long MissCount { get; set; }
        
        /// <summary>
        /// Cache hit rate (0.0 - 1.0).
        /// </summary>
        public double HitRate => HitCount + MissCount > 0 
            ? HitCount / (double)(HitCount + MissCount) 
            : 0;
    }
}

