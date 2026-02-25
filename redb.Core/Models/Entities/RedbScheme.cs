using redb.Core.Models.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB scheme entity with direct data storage.
    /// Maps to _schemes table in PostgreSQL.
    /// Contains collection of structures (fields).
    /// </summary>
    public class RedbScheme : IRedbScheme
    {
        /// <summary>
        /// Unique scheme identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Parent scheme identifier (for scheme hierarchy).
        /// </summary>
        [JsonPropertyName("id_parent")]
        public long? IdParent { get; set; }
        
        /// <summary>
        /// Scheme name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// Scheme alias (short name).
        /// </summary>
        [JsonPropertyName("alias")]
        public string? Alias { get; set; }
        
        /// <summary>
        /// Namespace for C# class generation.
        /// </summary>
        [JsonPropertyName("name_space")]
        public string? NameSpace { get; set; }
        
        /// <summary>
        /// MD5 hash of all structures (aggregated).
        /// Used for change detection and cache invalidation.
        /// </summary>
        [JsonPropertyName("structure_hash")]
        public Guid? StructureHash { get; set; }
        
        /// <summary>
        /// Scheme type ID (Class, Array, Dictionary, JsonDocument, XDocument).
        /// </summary>
        [JsonPropertyName("type")]
        public long Type { get; set; } = -9223372036854775675; // Default: Class

        // === Structures collection ===
        
        private List<RedbStructure> _structures = new();
        private Dictionary<string, IRedbStructure>? _structuresByName;
        
        /// <summary>
        /// Collection of structures (fields) for this scheme.
        /// </summary>
        [JsonPropertyName("structures")]
        public IReadOnlyCollection<IRedbStructure> Structures => _structures.AsReadOnly();
        
        /// <summary>
        /// Internal structures list (for provider).
        /// </summary>
        internal List<RedbStructure> StructuresInternal => _structures;
        
        /// <summary>
        /// Set structures collection (for mapping).
        /// </summary>
        public void SetStructures(IEnumerable<RedbStructure> structures)
        {
            _structures = structures?.ToList() ?? new List<RedbStructure>();
            _structuresByName = null; // Invalidate cache
        }

        /// <summary>
        /// Fast lookup of structure by name.
        /// </summary>
        public IRedbStructure? GetStructureByName(string name)
        {
            if (_structuresByName == null)
            {
                _structuresByName = _structures.ToDictionary(s => s.Name, s => (IRedbStructure)s);
            }
            return _structuresByName.TryGetValue(name, out var structure) ? structure : null;
        }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbScheme()
        {
        }
        
        /// <summary>
        /// Constructor with name.
        /// </summary>
        public RedbScheme(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public override string ToString()
        {
            var nameSpace = !string.IsNullOrEmpty(NameSpace) ? $"{NameSpace}." : "";
            var alias = !string.IsNullOrEmpty(Alias) ? $" ({Alias})" : "";
            return $"Scheme {Id}: {nameSpace}{Name}{alias}";
        }
    }
}
