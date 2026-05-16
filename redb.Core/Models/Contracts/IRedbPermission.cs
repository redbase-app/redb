using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB permission interface
    /// Represents user or role access rights to object/schema
    /// </summary>
    public interface IRedbPermission
    {
        /// <summary>
        /// Unique permission identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Role identifier (if permission is assigned to role)
        /// </summary>
        long? IdRole { get; }
        
        /// <summary>
        /// User identifier (if permission is assigned to user)
        /// </summary>
        long? IdUser { get; }
        
        /// <summary>
        /// Identifier of object/schema to which permission applies
        /// </summary>
        long IdRef { get; }
        
        /// <summary>
        /// Read permission (SELECT)
        /// </summary>
        bool? Select { get; }
        
        /// <summary>
        /// Create permission (INSERT)
        /// </summary>
        bool? Insert { get; }
        
        /// <summary>
        /// Update permission (UPDATE)
        /// </summary>
        bool? Update { get; }
        
        /// <summary>
        /// Delete permission (DELETE)
        /// </summary>
        bool? Delete { get; }
    }
}
