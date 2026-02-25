using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB role interface
    /// Represents user role in security system
    /// </summary>
    public interface IRedbRole
    {
        /// <summary>
        /// Unique role identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Role name
        /// </summary>
        string Name { get; }
    }
}
