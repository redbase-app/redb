using System.Threading.Tasks;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;

namespace redb.Core.Configuration
{
    /// <summary>
    /// Service for managing user configurations
    /// Provides merging of configurations from the chain:
    /// RedbServiceConfiguration -> Default User Config -> Role Config -> User Config
    /// </summary>
    public interface IUserConfigurationService
    {
        /// <summary>
        /// Get effective user configuration
        /// (taking into account the entire inheritance chain and overrides)
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <returns>Effective configuration with resolved values</returns>
        Task<EffectiveUserConfiguration> GetEffectiveConfigurationAsync(long userId);
        
        /// <summary>
        /// Get default configuration for regular users
        /// (not for sys, but for new users)
        /// </summary>
        /// <returns>RedbObject with default configuration or null if not created</returns>
        Task<RedbObject<UserConfigurationProps>?> GetDefaultConfigurationAsync();
        
        /// <summary>
        /// Set configuration for user
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <param name="configId">Configuration object ID (RedbObject&lt;UserConfigurationProps&gt;) or null to reset</param>
        Task SetUserConfigurationAsync(long userId, long? configId);
        
        /// <summary>
        /// Set configuration for role
        /// </summary>
        /// <param name="roleId">Role ID</param>
        /// <param name="configId">Configuration object ID (RedbObject&lt;UserConfigurationProps&gt;) or null to reset</param>
        Task SetRoleConfigurationAsync(long roleId, long? configId);
        
        /// <summary>
        /// Create new configuration
        /// </summary>
        /// <param name="name">Configuration name (e.g., "VIP Config")</param>
        /// <param name="props">Configuration properties</param>
        /// <param name="description">Configuration description (optional)</param>
        /// <returns>Created configuration RedbObject</returns>
        Task<RedbObject<UserConfigurationProps>> CreateConfigurationAsync(
            string name, 
            UserConfigurationProps props,
            string? description = null);
        
        /// <summary>
        /// Invalidate user configuration cache
        /// Called after changing user configuration or their roles
        /// </summary>
        /// <param name="userId">User ID</param>
        void InvalidateCache(long userId);
        
        /// <summary>
        /// Clear entire configuration cache
        /// Called after global changes
        /// </summary>
        void ClearCache();
    }
}

