using redb.Core.Models.Contracts;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB structure (field definition) entity with direct data storage.
    /// Maps to _structures table in PostgreSQL.
    /// </summary>
    public class RedbStructure : IRedbStructure
    {
        /// <summary>
        /// Unique structure identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Parent structure identifier (for nested fields).
        /// </summary>
        [JsonPropertyName("id_parent")]
        public long? IdParent { get; set; }
        
        /// <summary>
        /// Scheme identifier this structure belongs to.
        /// </summary>
        [JsonPropertyName("id_scheme")]
        public long IdScheme { get; set; }
        
        /// <summary>
        /// Override structure identifier (for inheritance).
        /// </summary>
        [JsonPropertyName("id_override")]
        public long? IdOverride { get; set; }
        
        /// <summary>
        /// Type identifier for this field.
        /// </summary>
        [JsonPropertyName("id_type")]
        public long IdType { get; set; }
        
        /// <summary>
        /// List identifier (for list-type fields).
        /// </summary>
        [JsonPropertyName("id_list")]
        public long? IdList { get; set; }
        
        /// <summary>
        /// Field name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// Field alias (short name).
        /// </summary>
        [JsonPropertyName("alias")]
        public string? Alias { get; set; }
        
        /// <summary>
        /// Field order in scheme.
        /// </summary>
        [JsonPropertyName("order")]
        public long? Order { get; set; }
        
        /// <summary>
        /// Is field read-only.
        /// </summary>
        [JsonPropertyName("readonly")]
        public bool? Readonly { get; set; }
        
        /// <summary>
        /// Is field required (not null).
        /// </summary>
        [JsonPropertyName("allow_not_null")]
        public bool? AllowNotNull { get; set; }
        
        /// <summary>
        /// Collection type ID: Array (-9223372036854775668) or Dictionary (-9223372036854775667).
        /// NULL for non-collection fields.
        /// </summary>
        [JsonPropertyName("collection_type")]
        public long? CollectionType { get; set; }
        
        /// <summary>
        /// Key type ID for Dictionary fields. NULL for non-dictionary fields.
        /// </summary>
        [JsonPropertyName("key_type")]
        public long? KeyType { get; set; }
        
        /// <summary>
        /// Compress field values.
        /// </summary>
        [JsonPropertyName("is_compress")]
        public bool? IsCompress { get; set; }
        
        /// <summary>
        /// Store null values.
        /// </summary>
        [JsonPropertyName("store_null")]
        public bool? StoreNull { get; set; }
        
        /// <summary>
        /// Default value (binary).
        /// </summary>
        [JsonPropertyName("default_value")]
        public byte[]? DefaultValue { get; set; }
        
        /// <summary>
        /// Default editor for field.
        /// </summary>
        [JsonPropertyName("default_editor")]
        public string? DefaultEditor { get; set; }

        /// <summary>
        /// Computed property: is this an array field.
        /// </summary>
        [JsonIgnore]
        public bool? IsArray => CollectionType != null;

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbStructure()
        {
        }

        public override string ToString()
        {
            var alias = !string.IsNullOrEmpty(Alias) ? $" ({Alias})" : "";
            var arrayIndicator = IsArray == true ? "[]" : "";
            var requiredIndicator = AllowNotNull == true ? "*" : "";
            return $"Structure {Id}: {Name}{alias}{arrayIndicator}{requiredIndicator} [Type: {IdType}]";
        }
    }
}
