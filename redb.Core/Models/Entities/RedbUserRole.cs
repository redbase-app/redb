using redb.Core.Models.Contracts;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB user-role association entity with direct data storage.
    /// Maps to _users_roles table in PostgreSQL.
    /// </summary>
    public class RedbUserRole : IRedbUserRole
    {
        /// <summary>
        /// Unique association identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// Role identifier.
        /// </summary>
        [JsonPropertyName("id_role")]
        public long IdRole { get; set; }
        
        /// <summary>
        /// User identifier.
        /// </summary>
        [JsonPropertyName("id_user")]
        public long IdUser { get; set; }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbUserRole()
        {
        }
        
        /// <summary>
        /// Constructor with user and role IDs.
        /// </summary>
        public RedbUserRole(long idUser, long idRole)
        {
            IdUser = idUser;
            IdRole = idRole;
        }

        public override string ToString()
        {
            return $"UserRole {Id}: User {IdUser} -> Role {IdRole}";
        }
    }
}
