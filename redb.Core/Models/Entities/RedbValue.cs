using System;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB value entity with direct data storage.
    /// Maps to _values table in PostgreSQL.
    /// Stores field values for objects (EAV pattern).
    /// </summary>
    public class RedbValue
    {
        /// <summary>
        /// Unique value identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Structure (field definition) identifier.
        /// </summary>
        [JsonPropertyName("id_structure")]
        public long IdStructure { get; set; }
        
        /// <summary>
        /// Object identifier this value belongs to.
        /// </summary>
        [JsonPropertyName("id_object")]
        public long IdObject { get; set; }
        
        /// <summary>
        /// String value.
        /// </summary>
        [JsonPropertyName("string")]
        public string? String { get; set; }
        
        /// <summary>
        /// Long integer value.
        /// </summary>
        [JsonPropertyName("long")]
        public long? Long { get; set; }
        
        /// <summary>
        /// GUID value.
        /// </summary>
        [JsonPropertyName("guid")]
        public Guid? Guid { get; set; }
        
        /// <summary>
        /// Double value.
        /// </summary>
        [JsonPropertyName("double")]
        public double? Double { get; set; }
        
        /// <summary>
        /// DateTime with timezone value.
        /// </summary>
        [JsonPropertyName("date_time_offset")]
        public DateTimeOffset? DateTimeOffset { get; set; }
        
        /// <summary>
        /// Boolean value.
        /// </summary>
        [JsonPropertyName("boolean")]
        public bool? Boolean { get; set; }
        
        /// <summary>
        /// Binary data value.
        /// </summary>
        [JsonPropertyName("byte_array")]
        public byte[]? ByteArray { get; set; }
        
        /// <summary>
        /// Decimal value (high precision).
        /// </summary>
        [JsonPropertyName("numeric")]
        public decimal? Numeric { get; set; }
        
        /// <summary>
        /// List item reference ID.
        /// </summary>
        [JsonPropertyName("list_item")]
        public long? ListItem { get; set; }
        
        /// <summary>
        /// Object reference ID.
        /// </summary>
        [JsonPropertyName("object")]
        public long? Object { get; set; }
        
        /// <summary>
        /// Parent value ID for nested structures (arrays, dictionaries).
        /// </summary>
        [JsonPropertyName("array_parent_id")]
        public long? ArrayParentId { get; set; }
        
        /// <summary>
        /// Array index or dictionary key.
        /// For arrays: "0", "1", "2", etc.
        /// For dictionaries: string key.
        /// NULL for non-collection fields.
        /// </summary>
        [JsonPropertyName("array_index")]
        public string? ArrayIndex { get; set; }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbValue()
        {
        }

        /// <summary>
        /// Check if this is an array/collection element.
        /// </summary>
        [JsonIgnore]
        public bool IsArrayElement => !string.IsNullOrEmpty(ArrayIndex);

        public override string ToString()
        {
            var valueStr = String ?? Long?.ToString() ?? Guid?.ToString() ?? 
                          Double?.ToString() ?? Boolean?.ToString() ?? "[binary]";
            var indexStr = IsArrayElement ? $"[{ArrayIndex}]" : "";
            return $"Value {Id}: Struct={IdStructure}, Obj={IdObject}{indexStr} = {valueStr}";
        }
    }
}

