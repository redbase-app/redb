using redb.Core.Models.Contracts;
using System;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB type entity with direct data storage.
    /// Maps to _types table in PostgreSQL.
    /// </summary>
    public class RedbType : IRedbType
    {
        /// <summary>
        /// Unique type identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Type name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// Database type name (e.g., "String", "Long", "Boolean").
        /// </summary>
        [JsonPropertyName("db_type")]
        public string? DbType { get; set; }
        
        /// <summary>
        /// .NET type full name (e.g., "System.String", "System.Int64").
        /// </summary>
        [JsonPropertyName("type")]
        public string? Type1 { get; set; }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbType()
        {
        }
        
        /// <summary>
        /// Constructor with name.
        /// </summary>
        public RedbType(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        /// <summary>
        /// Get .NET Type from string representation.
        /// </summary>
        public Type? GetDotNetType()
        {
            if (string.IsNullOrEmpty(Type1))
                return null;

            return Type1 switch
            {
                "System.String" => typeof(string),
                "System.Int64" => typeof(long),
                "System.Int32" => typeof(int),
                "System.Double" => typeof(double),
                "System.DateTimeOffset" => typeof(DateTimeOffset),
                "System.Boolean" => typeof(bool),
                "System.Guid" => typeof(Guid),
                _ => Type.GetType(Type1)
            };
        }

        /// <summary>
        /// Check if type supports arrays.
        /// </summary>
        public bool SupportsArrays()
        {
            return !string.IsNullOrEmpty(Name);
        }

        public override string ToString()
        {
            var dotNetType = !string.IsNullOrEmpty(Type1) ? $" (.NET: {Type1})" : "";
            var dbType = !string.IsNullOrEmpty(DbType) ? $" (DB: {DbType})" : "";
            return $"Type {Id}: {Name}{dotNetType}{dbType}";
        }
    }
}
