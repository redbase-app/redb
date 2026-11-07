using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB user-role relationship interface
    /// Represents role assignment to a user
    /// </summary>
    public interface IRedbUserRole
    {
        /// <summary>
        /// Unique identifier of the relationship
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Role identifier
        /// </summary>
        long IdRole { get; }
        
        /// <summary>
        /// User identifier
        /// </summary>
        long IdUser { get; }
    }
}
