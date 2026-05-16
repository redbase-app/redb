using redb.Core.Models.Contracts;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB permission entity with direct data storage.
    /// Maps to _permissions table in PostgreSQL.
    /// </summary>
    public class RedbPermission : IRedbPermission
    {
        /// <summary>
        /// Unique permission identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Role identifier (null if user-specific permission).
        /// </summary>
        [JsonPropertyName("id_role")]
        public long? IdRole { get; set; }
        
        /// <summary>
        /// User identifier (null if role-based permission).
        /// </summary>
        [JsonPropertyName("id_user")]
        public long? IdUser { get; set; }
        
        /// <summary>
        /// Reference object identifier (scheme, object, etc.).
        /// </summary>
        [JsonPropertyName("id_ref")]
        public long IdRef { get; set; }
        
        /// <summary>
        /// SELECT (read) permission.
        /// </summary>
        [JsonPropertyName("select")]
        public bool? Select { get; set; }
        
        /// <summary>
        /// INSERT (create) permission.
        /// </summary>
        [JsonPropertyName("insert")]
        public bool? Insert { get; set; }
        
        /// <summary>
        /// UPDATE (edit) permission.
        /// </summary>
        [JsonPropertyName("update")]
        public bool? Update { get; set; }
        
        /// <summary>
        /// DELETE (remove) permission.
        /// </summary>
        [JsonPropertyName("delete")]
        public bool? Delete { get; set; }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbPermission()
        {
        }

        /// <summary>
        /// Check if specific permission is granted.
        /// </summary>
        public bool HasPermission(string action)
        {
            return action.ToLower() switch
            {
                "select" or "read" => Select == true,
                "insert" or "create" => Insert == true,
                "update" or "edit" => Update == true,
                "delete" or "remove" => Delete == true,
                _ => false
            };
        }

        /// <summary>
        /// Get list of active permissions.
        /// </summary>
        public IEnumerable<string> GetActivePermissions()
        {
            var permissions = new List<string>();
            if (Select == true) permissions.Add("Select");
            if (Insert == true) permissions.Add("Insert");
            if (Update == true) permissions.Add("Update");
            if (Delete == true) permissions.Add("Delete");
            return permissions;
        }

        public override string ToString()
        {
            var target = IdRole.HasValue ? $"Role {IdRole}" : $"User {IdUser}";
            var permissions = string.Join(", ", GetActivePermissions());
            return $"Permission {Id}: {target} -> Ref {IdRef} [{permissions}]";
        }
    }
}
