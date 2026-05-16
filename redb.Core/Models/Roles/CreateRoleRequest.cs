using redb.Core.Models.Contracts;

namespace redb.Core.Models.Roles
{
    /// <summary>
    /// Request for creating new role
    /// </summary>
    public class CreateRoleRequest
    {
        /// <summary>
        /// Role name (unique)
        /// </summary>
        public string Name { get; set; } = "";
        
        /// <summary>
        /// Role description (optional)
        /// </summary>
        public string? Description { get; set; }
        
        /// <summary>
        /// Users to assign to role upon creation (objects)
        /// For backward compatibility and programmatic usage
        /// </summary>
        public IRedbUser[]? Users { get; set; }
        
        /// <summary>
        /// User logins to assign to role upon creation
        /// Example: ["admin", "manager", "user123"]
        /// If both Users and UserLogins are specified - UserLogins are used (priority)
        /// </summary>
        public string[]? UserLogins { get; set; }
    }
}
