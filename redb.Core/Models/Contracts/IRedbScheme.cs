using System;
using System.Collections.Generic;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB scheme interface
    /// Represents scheme (type) of objects in system with structure encapsulation
    /// </summary>
    public interface IRedbScheme
    {
        /// <summary>
        /// Unique scheme identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Parent scheme identifier (for scheme hierarchy)
        /// </summary>
        long? IdParent { get; }
        
        /// <summary>
        /// Scheme name
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Scheme alias (short name)
        /// </summary>
        string? Alias { get; }
        
        /// <summary>
        /// Scheme namespace (for C# classes)
        /// </summary>
        string? NameSpace { get; }
        
        /// <summary>
        /// MD5 hash of all scheme structures (aggregated)
        /// Used for automatic change detection and cache invalidation
        /// </summary>
        Guid? StructureHash { get; }
        
        /// <summary>
        /// Collection of structures (fields) of this scheme
        /// Scheme encapsulates its structures for data integrity
        /// </summary>
        IReadOnlyCollection<IRedbStructure> Structures { get; }
        
        /// <summary>
        /// Fast access to structure by name
        /// Avoids the need to search in collection
        /// </summary>
        IRedbStructure? GetStructureByName(string name);
    }
}
