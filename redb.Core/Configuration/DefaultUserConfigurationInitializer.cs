using System;
using System.Threading.Tasks;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;

namespace redb.Core.Configuration
{
    /// <summary>
    /// Default user configuration initializer.
    /// Creates basic configuration with reasonable values on first run.
    /// </summary>
    public class DefaultUserConfigurationInitializer
    {
        /// <summary>
        /// Fixed ID for default user configuration.
        /// </summary>
        public const long DefaultConfigId = -100;
        
        private readonly IRedbService _redbService;
        
        public DefaultUserConfigurationInitializer(IRedbService redbService)
        {
            _redbService = redbService ?? throw new ArgumentNullException(nameof(redbService));
        }
        
        /// <summary>
        /// Initialize default configuration for users.
        /// Creates object with ID=-100 if it doesn't exist.
        /// </summary>
        public async Task InitializeAsync()
        {
            // Check if already exists
            var existing = await _redbService.LoadAsync<UserConfigurationProps>(DefaultConfigId);
            if (existing != null) return;
            
            // Create default configuration with fixed ID
            var defaultConfig = new RedbObject<UserConfigurationProps>
            {
                Id = DefaultConfigId,
                name = "Default User Config",
                Props = new UserConfigurationProps
                {
                    // Cache quotas (smaller than system)
                    PropsCacheSize = 1000,
                    ListCacheSize = 500,
                    PropsCacheTtlMinutes = 30,
                    ListCacheTtlMinutes = 5,
                    
                    // Loading limits (smaller than system)
                    MaxLoadDepth = 10,
                    MaxTreeDepth = 50,
                    
                    // Performance
                    EnableLazyLoadingForProps = true,
                    
                    // Security (more checks for regular users)
                    AlwaysCheckPermissionsOnLoad = false,
                    AlwaysCheckPermissionsOnSave = false,
                    
                    // API quotas
                    MaxRequestsPerMinute = 100,
                    MaxBatchSize = 100,
                    
                    // Strategy (simpler for regular users)
                    EavSaveStrategy = EavSaveStrategy.DeleteInsert,
                    
                    // Metadata
                    Description = "Default configuration for regular users. " +
                                  "Applied if user or their roles don't have personal configuration.",
                    Priority = 0
                }
            };
            
            await _redbService.SaveAsync(defaultConfig);
        }
    }
}

