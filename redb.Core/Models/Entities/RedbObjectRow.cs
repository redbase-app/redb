using System;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// POCO for _objects table.
    /// Used for direct SQL queries to objects table.
    /// Not to be confused with RedbObject&lt;TProps&gt; which is a domain wrapper.
    /// </summary>
    public class RedbObjectRow
    {
        /// <summary>
        /// Object ID.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Parent object ID.
        /// </summary>
        [JsonPropertyName("id_parent")]
        public long? IdParent { get; set; }
        
        /// <summary>
        /// Scheme ID.
        /// </summary>
        [JsonPropertyName("id_scheme")]
        public long IdScheme { get; set; }
        
        /// <summary>
        /// Owner user ID.
        /// </summary>
        [JsonPropertyName("id_owner")]
        public long IdOwner { get; set; }
        
        /// <summary>
        /// Last modifier user ID.
        /// </summary>
        [JsonPropertyName("id_who_change")]
        public long IdWhoChange { get; set; }
        
        /// <summary>
        /// Creation date.
        /// </summary>
        [JsonPropertyName("date_create")]
        public DateTimeOffset DateCreate { get; set; }
        
        /// <summary>
        /// Last modification date.
        /// </summary>
        [JsonPropertyName("date_modify")]
        public DateTimeOffset DateModify { get; set; }
        
        /// <summary>
        /// Begin date (for temporal objects).
        /// </summary>
        [JsonPropertyName("date_begin")]
        public DateTimeOffset? DateBegin { get; set; }
        
        /// <summary>
        /// Complete date (for temporal objects).
        /// </summary>
        [JsonPropertyName("date_complete")]
        public DateTimeOffset? DateComplete { get; set; }
        
        /// <summary>
        /// External key for integration.
        /// </summary>
        [JsonPropertyName("key")]
        public long? Key { get; set; }
        
        /// <summary>
        /// Object name.
        /// </summary>
        [JsonPropertyName("name")]
        public string? Name { get; set; }
        
        /// <summary>
        /// Object note.
        /// </summary>
        [JsonPropertyName("note")]
        public string? Note { get; set; }
        
        /// <summary>
        /// Hash for change tracking.
        /// </summary>
        [JsonPropertyName("hash")]
        public Guid? Hash { get; set; }

        // Value columns for primitive storage in object itself
        
        /// <summary>
        /// Long value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_long")]
        public long? ValueLong { get; set; }
        
        /// <summary>
        /// String value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_string")]
        public string? ValueString { get; set; }
        
        /// <summary>
        /// Guid value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_guid")]
        public Guid? ValueGuid { get; set; }
        
        /// <summary>
        /// Boolean value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_bool")]
        public bool? ValueBool { get; set; }
        
        /// <summary>
        /// Double value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_double")]
        public double? ValueDouble { get; set; }
        
        /// <summary>
        /// Numeric/decimal value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_numeric")]
        public decimal? ValueNumeric { get; set; }
        
        /// <summary>
        /// DateTime value stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_datetime")]
        public DateTimeOffset? ValueDatetime { get; set; }
        
        /// <summary>
        /// Binary data stored directly in object row.
        /// </summary>
        [JsonPropertyName("_value_bytes")]
        public byte[]? ValueBytes { get; set; }

        public override string ToString()
        {
            return $"ObjectRow {Id} (Scheme:{IdScheme}, Name:{Name ?? "null"})";
        }
    }
}

