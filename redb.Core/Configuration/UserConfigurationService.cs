using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;

namespace redb.Core.Configuration;

/// <summary>
/// Database-agnostic implementation of IUserConfigurationService.
/// Uses $1, $2 parameter format (automatically converted for MSSQL).
/// </summary>
public class UserConfigurationService : IUserConfigurationService
{
    private readonly IRedbService _redbService;
    private readonly IRedbContext _context;
    private readonly RedbServiceConfiguration _baseConfiguration;
    
    // Cache for effective configurations (userId -> (config, expireTime))
    private readonly ConcurrentDictionary<long, CachedConfiguration> _configCache = new();
    private readonly TimeSpan _cacheTtl = TimeSpan.FromMinutes(5);
    
    private class CachedConfiguration
    {
        public EffectiveUserConfiguration Configuration { get; set; } = null!;
        public DateTime ExpiresAt { get; set; }
    }
    
    public UserConfigurationService(
        IRedbService redbService,
        IRedbContext context,
        RedbServiceConfiguration baseConfiguration)
    {
        _redbService = redbService ?? throw new ArgumentNullException(nameof(redbService));
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _baseConfiguration = baseConfiguration ?? throw new ArgumentNullException(nameof(baseConfiguration));
    }
    
    /// <summary>
    /// Get effective configuration for user.
    /// </summary>
    public async Task<EffectiveUserConfiguration> GetEffectiveConfigurationAsync(long userId)
    {
        // Check cache
        if (_configCache.TryGetValue(userId, out var cached))
        {
            if (DateTime.UtcNow < cached.ExpiresAt)
            {
                return cached.Configuration;
            }
            // Expired - remove
            _configCache.TryRemove(userId, out _);
        }
        
        // Build configuration
        var effective = await BuildEffectiveConfigurationAsync(userId);
        
        // Cache it
        _configCache[userId] = new CachedConfiguration
        {
            Configuration = effective,
            ExpiresAt = DateTime.UtcNow.Add(_cacheTtl)
        };
        
        return effective;
    }
    
    /// <summary>
    /// Build effective configuration for user.
    /// </summary>
    private async Task<EffectiveUserConfiguration> BuildEffectiveConfigurationAsync(long userId)
    {
        var effective = new EffectiveUserConfiguration
        {
            UserId = userId,
            CreatedAt = DateTime.UtcNow
        };
        
        // Step 1: Base values from RedbServiceConfiguration
        ApplyBaseConfiguration(effective);
        
        // Step 2: For sys (userId=0) return base values (no limits)
        if (userId == 0)
        {
            effective.PropsCacheSize = null; // Unlimited
            effective.ListCacheSize = null;   // Unlimited
            return effective;
        }
        
        // Step 3: Load Default User Configuration
        var defaultConfig = await GetDefaultConfigurationAsync();
        if (defaultConfig != null)
        {
            ApplyOverride(effective, defaultConfig.Props, "Default User Config", 10);
        }
        
        // Step 4: Load user's role configurations
        var user = await _context.QueryFirstOrDefaultAsync<RedbUser>(
            "SELECT _id as \"Id\", _id_configuration as \"IdConfiguration\" FROM _users WHERE _id = $1", userId);
        
        // Load user roles
        var userRoles = await _context.QueryAsync<RedbUserRole>(
            "SELECT ur._id_role as \"IdRole\" FROM _users_roles ur WHERE ur._id_user = $1", userId);
        
        if (user != null && userRoles.Any())
        {
            // Get roles with configurations
            var roleConfigs = new List<(RedbObject<UserConfigurationProps> config, string roleName)>();
            
            foreach (var userRole in userRoles)
            {
                var role = await _context.QueryFirstOrDefaultAsync<RedbRole>(
                    "SELECT _id as \"Id\", _name as \"Name\", _id_configuration as \"IdConfiguration\" FROM _roles WHERE _id = $1", 
                    userRole.IdRole);
                
                if (role?.IdConfiguration != null)
                {
                    var roleConfig = await _redbService.LoadAsync<UserConfigurationProps>(role.IdConfiguration.Value);
                    if (roleConfig != null)
                    {
                        roleConfigs.Add((roleConfig, role.Name));
                    }
                }
            }
            
            // Sort by priority and apply
            foreach (var (config, roleName) in roleConfigs.OrderBy(rc => rc.config.Props.Priority))
            {
                ApplyOverride(effective, config.Props, $"Role:{roleName}", 20 + config.Props.Priority);
            }
        }
        
        // Step 5: Load personal user configuration
        if (user?.IdConfiguration.HasValue == true)
        {
            var userConfig = await _redbService.LoadAsync<UserConfigurationProps>(user.IdConfiguration.Value);
            if (userConfig != null)
            {
                ApplyOverride(effective, userConfig.Props, "User", 100);
            }
        }
        
        return effective;
    }
    
    /// <summary>
    /// Apply base values from RedbServiceConfiguration.
    /// </summary>
    private void ApplyBaseConfiguration(EffectiveUserConfiguration effective)
    {
        effective.PropsCacheSize = _baseConfiguration.PropsCacheMaxSize;
        effective.ListCacheSize = 5000;
        effective.PropsCacheTtl = _baseConfiguration.PropsCacheTtl;
        effective.ListCacheTtl = _baseConfiguration.ListCacheTtl;
        effective.MaxLoadDepth = _baseConfiguration.DefaultLoadDepth;
        effective.MaxTreeDepth = _baseConfiguration.DefaultMaxTreeDepth;
        effective.EnableLazyLoadingForProps = _baseConfiguration.EnableLazyLoadingForProps;
        effective.AlwaysCheckPermissionsOnLoad = _baseConfiguration.DefaultCheckPermissionsOnLoad;
        effective.AlwaysCheckPermissionsOnSave = _baseConfiguration.DefaultCheckPermissionsOnSave;
        effective.MaxRequestsPerMinute = null;
        effective.MaxBatchSize = 1000;
        effective.EavSaveStrategy = _baseConfiguration.EavSaveStrategy;
        
        // Sources
        effective.Sources.Add(new ConfigurationSource
        {
            ParameterName = "All",
            Source = "RedbServiceConfiguration",
            Priority = 0,
            Value = "Base values"
        });
    }
    
    /// <summary>
    /// Apply override from configuration (only non-null values).
    /// </summary>
    private void ApplyOverride(
        EffectiveUserConfiguration target, 
        UserConfigurationProps source,
        string sourceName,
        int priority)
    {
        if (source.PropsCacheSize.HasValue)
        {
            target.PropsCacheSize = source.PropsCacheSize.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.PropsCacheSize),
                Source = sourceName,
                Priority = priority,
                Value = source.PropsCacheSize.Value.ToString()
            });
        }
        
        if (source.ListCacheSize.HasValue)
        {
            target.ListCacheSize = source.ListCacheSize.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.ListCacheSize),
                Source = sourceName,
                Priority = priority,
                Value = source.ListCacheSize.Value.ToString()
            });
        }
        
        if (source.PropsCacheTtlMinutes.HasValue)
        {
            target.PropsCacheTtl = TimeSpan.FromMinutes(source.PropsCacheTtlMinutes.Value);
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.PropsCacheTtlMinutes),
                Source = sourceName,
                Priority = priority,
                Value = source.PropsCacheTtlMinutes.Value.ToString()
            });
        }
        
        if (source.ListCacheTtlMinutes.HasValue)
        {
            target.ListCacheTtl = TimeSpan.FromMinutes(source.ListCacheTtlMinutes.Value);
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.ListCacheTtlMinutes),
                Source = sourceName,
                Priority = priority,
                Value = source.ListCacheTtlMinutes.Value.ToString()
            });
        }
        
        if (source.MaxLoadDepth.HasValue)
        {
            target.MaxLoadDepth = source.MaxLoadDepth.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.MaxLoadDepth),
                Source = sourceName,
                Priority = priority,
                Value = source.MaxLoadDepth.Value.ToString()
            });
        }
        
        if (source.MaxTreeDepth.HasValue)
        {
            target.MaxTreeDepth = source.MaxTreeDepth.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.MaxTreeDepth),
                Source = sourceName,
                Priority = priority,
                Value = source.MaxTreeDepth.Value.ToString()
            });
        }
        
        if (source.EnableLazyLoadingForProps.HasValue)
        {
            target.EnableLazyLoadingForProps = source.EnableLazyLoadingForProps.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.EnableLazyLoadingForProps),
                Source = sourceName,
                Priority = priority,
                Value = source.EnableLazyLoadingForProps.Value.ToString()
            });
        }
        
        if (source.AlwaysCheckPermissionsOnLoad.HasValue)
        {
            target.AlwaysCheckPermissionsOnLoad = source.AlwaysCheckPermissionsOnLoad.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.AlwaysCheckPermissionsOnLoad),
                Source = sourceName,
                Priority = priority,
                Value = source.AlwaysCheckPermissionsOnLoad.Value.ToString()
            });
        }
        
        if (source.AlwaysCheckPermissionsOnSave.HasValue)
        {
            target.AlwaysCheckPermissionsOnSave = source.AlwaysCheckPermissionsOnSave.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.AlwaysCheckPermissionsOnSave),
                Source = sourceName,
                Priority = priority,
                Value = source.AlwaysCheckPermissionsOnSave.Value.ToString()
            });
        }
        
        if (source.MaxRequestsPerMinute.HasValue)
        {
            target.MaxRequestsPerMinute = source.MaxRequestsPerMinute.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.MaxRequestsPerMinute),
                Source = sourceName,
                Priority = priority,
                Value = source.MaxRequestsPerMinute.Value.ToString()
            });
        }
        
        if (source.MaxBatchSize.HasValue)
        {
            target.MaxBatchSize = source.MaxBatchSize.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.MaxBatchSize),
                Source = sourceName,
                Priority = priority,
                Value = source.MaxBatchSize.Value.ToString()
            });
        }
        
        if (source.EavSaveStrategy.HasValue)
        {
            target.EavSaveStrategy = source.EavSaveStrategy.Value;
            target.Sources.Add(new ConfigurationSource
            {
                ParameterName = nameof(source.EavSaveStrategy),
                Source = sourceName,
                Priority = priority,
                Value = source.EavSaveStrategy.Value.ToString()
            });
        }
    }
    
    /// <summary>
    /// Get default user configuration (ID=-100).
    /// </summary>
    public Task<RedbObject<UserConfigurationProps>?> GetDefaultConfigurationAsync()
    {
        return _redbService.LoadAsync<UserConfigurationProps>(DefaultUserConfigurationInitializer.DefaultConfigId);
    }
    
    /// <summary>
    /// Set user configuration.
    /// </summary>
    public async Task SetUserConfigurationAsync(long userId, long? configId)
    {
        var exists = await _context.ExecuteScalarAsync<long?>(
            "SELECT _id FROM _users WHERE _id = $1", userId);
        if (!exists.HasValue)
            throw new ArgumentException($"User with ID {userId} not found");
        
        await _context.ExecuteAsync(
            "UPDATE _users SET _id_configuration = $1 WHERE _id = $2", configId, userId);
        
        // Invalidate cache
        InvalidateCache(userId);
    }
    
    /// <summary>
    /// Set role configuration.
    /// </summary>
    public async Task SetRoleConfigurationAsync(long roleId, long? configId)
    {
        var exists = await _context.ExecuteScalarAsync<long?>(
            "SELECT _id FROM _roles WHERE _id = $1", roleId);
        if (!exists.HasValue)
            throw new ArgumentException($"Role with ID {roleId} not found");
        
        await _context.ExecuteAsync(
            "UPDATE _roles SET _id_configuration = $1 WHERE _id = $2", configId, roleId);
        
        // Invalidate cache for all users with this role
        var userIds = await _context.QueryAsync<long>(
            "SELECT _id_user FROM _users_roles WHERE _id_role = $1", roleId);
            
        foreach (var userId in userIds)
        {
            InvalidateCache(userId);
        }
    }
    
    /// <summary>
    /// Create new configuration.
    /// </summary>
    public async Task<RedbObject<UserConfigurationProps>> CreateConfigurationAsync(
        string name, 
        UserConfigurationProps props,
        string? description = null)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Configuration name cannot be empty", nameof(name));
            
        if (props == null)
            throw new ArgumentNullException(nameof(props));
            
        // Set description
        if (!string.IsNullOrWhiteSpace(description))
        {
            props.Description = description;
        }
        
        // Create RedbObject
        var config = new RedbObject<UserConfigurationProps>
        {
            name = name,
            Props = props
        };
        
        // Save via IRedbService
        await _redbService.SaveAsync(config);
        
        return config;
    }
    
    /// <summary>
    /// Invalidate cache for user configuration.
    /// </summary>
    public void InvalidateCache(long userId)
    {
        _configCache.TryRemove(userId, out _);
    }
    
    /// <summary>
    /// Clear all configuration cache.
    /// </summary>
    public void ClearCache()
    {
        _configCache.Clear();
    }
}

