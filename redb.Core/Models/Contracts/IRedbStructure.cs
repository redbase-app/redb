using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB scheme field structure interface
    /// Represents field metadata in object scheme
    /// </summary>
    public interface IRedbStructure
    {
        /// <summary>
        /// Unique structure identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Parent structure identifier (for nested fields)
        /// </summary>
        long? IdParent { get; }
        
        /// <summary>
        /// Identifier of scheme to which structure belongs
        /// </summary>
        long IdScheme { get; }
        
        /// <summary>
        /// Identifier of overridden structure (for inheritance)
        /// </summary>
        long? IdOverride { get; }
        
        /// <summary>
        /// Data type identifier
        /// </summary>
        long IdType { get; }
        
        /// <summary>
        /// List identifier (for list-type fields)
        /// </summary>
        long? IdList { get; }
        
        /// <summary>
        /// Field name
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Field alias (short name)
        /// </summary>
        string? Alias { get; }
        
        /// <summary>
        /// Field order in scheme
        /// </summary>
        long? Order { get; }
        
        /// <summary>
        /// Read-only field
        /// </summary>
        bool? Readonly { get; }
        
        /// <summary>
        /// Field is required for filling
        /// </summary>
        bool? AllowNotNull { get; }
        
        /// <summary>
        /// Collection type ID: Array (-9223372036854775668) or Dictionary (-9223372036854775667)
        /// NULL for non-collection fields
        /// </summary>
        long? CollectionType { get; }
        
        /// <summary>
        /// Key type ID for Dictionary fields. NULL for non-dictionary fields
        /// </summary>
        long? KeyType { get; }
        
        /// <summary>
        /// Compress field values
        /// </summary>
        bool? IsCompress { get; }
        
        /// <summary>
        /// Store null values
        /// </summary>
        bool? StoreNull { get; }
        
        /// <summary>
        /// Default value (in binary form)
        /// </summary>
        byte[]? DefaultValue { get; }
        
        /// <summary>
        /// Default editor for field
        /// </summary>
        string? DefaultEditor { get; }
    }
}
