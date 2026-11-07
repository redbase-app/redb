using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB data type interface
    /// Represents data type for fields in schemes
    /// </summary>
    public interface IRedbType
    {
        /// <summary>
        /// Unique type identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Type name (String, Long, DateTimeOffset, Boolean, etc.)
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Type in database (varchar, bigint, timestamp, boolean, etc.)
        /// </summary>
        string? DbType { get; }
        
        /// <summary>
        /// Type in .NET (System.String, System.Int64, System.DateTimeOffset, etc.)
        /// </summary>
        string? Type1 { get; }
    }
}
