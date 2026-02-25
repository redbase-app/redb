using redb.Core.Models.Contracts;
using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// REDB user entity with direct data storage.
    /// Maps to _users table in PostgreSQL.
    /// </summary>
    public class RedbUser : IRedbUser
    {
        /// <summary>
        /// Unique user identifier.
        /// </summary>
        [JsonPropertyName("id")]
        public long Id { get; set; }
        
        /// <summary>
        /// User login (unique).
        /// </summary>
        [JsonPropertyName("login")]
        public string Login { get; set; } = string.Empty;
        
        /// <summary>
        /// User display name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// User password (hashed).
        /// </summary>
        [JsonPropertyName("password")]
        public string Password { get; set; } = string.Empty;
        
        /// <summary>
        /// Whether user is active.
        /// </summary>
        [JsonPropertyName("enabled")]
        public bool Enabled { get; set; } = true;
        
        /// <summary>
        /// User registration date.
        /// </summary>
        [JsonPropertyName("date_register")]
        public DateTimeOffset DateRegister { get; set; } = DateTimeOffset.UtcNow;
        
        /// <summary>
        /// User dismissal date (null if active).
        /// </summary>
        [JsonPropertyName("date_dismiss")]
        public DateTimeOffset? DateDismiss { get; set; }
        
        /// <summary>
        /// User phone (optional).
        /// </summary>
        [JsonPropertyName("phone")]
        public string? Phone { get; set; }
        
        /// <summary>
        /// User email (optional).
        /// </summary>
        [JsonPropertyName("email")]
        public string? Email { get; set; }
        
        /// <summary>
        /// Additional user key (optional).
        /// </summary>
        [JsonPropertyName("key")]
        public long? Key { get; set; }
        
        /// <summary>
        /// Integer code for categorization (optional).
        /// </summary>
        [JsonPropertyName("code_int")]
        public long? CodeInt { get; set; }
        
        /// <summary>
        /// String code for departments/branches (optional).
        /// </summary>
        [JsonPropertyName("code_string")]
        public string? CodeString { get; set; }
        
        /// <summary>
        /// GUID code for distributed systems (optional).
        /// </summary>
        [JsonPropertyName("code_guid")]
        public Guid? CodeGuid { get; set; }
        
        /// <summary>
        /// User notes or comments (optional).
        /// </summary>
        [JsonPropertyName("note")]
        public string? Note { get; set; }
        
        /// <summary>
        /// Hash for data integrity verification (optional).
        /// </summary>
        [JsonPropertyName("hash")]
        public Guid? Hash { get; set; }
        
        /// <summary>
        /// Configuration object ID (optional).
        /// </summary>
        [JsonPropertyName("id_configuration")]
        public long? IdConfiguration { get; set; }

        /// <summary>
        /// Default constructor for deserialization and mapping.
        /// </summary>
        public RedbUser()
        {
        }
        
        /// <summary>
        /// Constructor with required fields.
        /// </summary>
        public RedbUser(string login, string name, string password)
        {
            Login = login ?? throw new ArgumentNullException(nameof(login));
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Password = password ?? throw new ArgumentNullException(nameof(password));
        }

        /// <summary>
        /// System user (SYS_USER_ID = 0).
        /// </summary>
        public static RedbUser SystemUser => new RedbUser
        {
            Id = 0,
            Login = "sys",
            Name = "System User",
            Password = "",
            Enabled = true,
            DateRegister = DateTimeOffset.MinValue,
            DateDismiss = null,
            Phone = null,
            Email = null,
            Key = null,
            CodeInt = 0,
            CodeString = "SYS",
            CodeGuid = new Guid("00000000-0000-0000-0000-000000000001"),
            Note = "System user REDB",
            Hash = null
        };

        public override string ToString()
        {
            var status = Enabled ? "Active" : "Disabled";
            var codes = new List<string>();
            
            if (CodeInt.HasValue) codes.Add($"Int={CodeInt}");
            if (!string.IsNullOrEmpty(CodeString)) codes.Add($"Str={CodeString}");
            if (Key.HasValue) codes.Add($"Key={Key}");
            
            var codesStr = codes.Count > 0 ? $" [{string.Join(", ", codes)}]" : "";
            
            return $"User {Id}: {Login} ({Name}) - {status}{codesStr}";
        }
    }
}
