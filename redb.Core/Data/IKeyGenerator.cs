using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Key generator abstraction for generating unique IDs.
    /// Replaces EF Core's automatic ID generation.
    /// </summary>
    public interface IKeyGenerator
    {
        /// <summary>
        /// Generate next unique ID for objects.
        /// Uses database sequence or other mechanism.
        /// </summary>
        /// <returns>Next unique ID.</returns>
        Task<long> NextObjectIdAsync();
        
        /// <summary>
        /// Generate next unique ID for values.
        /// Uses database sequence or other mechanism.
        /// </summary>
        /// <returns>Next unique ID.</returns>
        Task<long> NextValueIdAsync();
        
        /// <summary>
        /// Generate batch of object IDs for bulk operations.
        /// More efficient than calling NextObjectIdAsync multiple times.
        /// </summary>
        /// <param name="count">Number of IDs to generate.</param>
        /// <returns>Array of unique IDs.</returns>
        Task<long[]> NextObjectIdBatchAsync(int count);
        
        /// <summary>
        /// Generate batch of value IDs for bulk operations.
        /// More efficient than calling NextValueIdAsync multiple times.
        /// </summary>
        /// <param name="count">Number of IDs to generate.</param>
        /// <returns>Array of unique IDs.</returns>
        Task<long[]> NextValueIdBatchAsync(int count);
    }
}

