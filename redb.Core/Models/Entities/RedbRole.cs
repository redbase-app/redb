using redb.Core.Models.Contracts;
using System;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB role entity with direct data storage.
    /// Maps to _roles table in PostgreSQL.
    /// </summary>
    public class RedbRole : IRedbRole
    {
        /// <summary>
        /// Unique role identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Role name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// Configuration object ID (optional).
        /// </summary>
        [JsonPropertyName("id_configuration")]
        public long? IdConfiguration { get; set; }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbRole()
        {
        }
        
        /// <summary>
        /// Constructor with name.
        /// </summary>
        public RedbRole(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public override string ToString()
        {
            return $"Role {Id}: {Name}";
        }
    }
}
