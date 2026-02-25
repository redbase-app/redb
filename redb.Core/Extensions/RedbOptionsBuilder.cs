using Microsoft.Extensions.DependencyInjection;
using redb.Core.Models.Configuration;

namespace redb.Core.Extensions;

/// <summary>
/// Infrastructure interface for database providers and Pro extensions.
/// Similar to EF Core's IDbContextOptionsBuilderInfrastructure.
/// Used by UsePostgres(), UseMsSql(), AddRedbPro(), etc.
/// </summary>
public interface IRedbOptionsBuilderInfrastructure
{
    /// <summary>
    /// Service collection for registering services.
    /// </summary>
    IServiceCollection Services { get; }
    
    /// <summary>
    /// Configuration instance to be populated by provider.
    /// </summary>
    RedbServiceConfiguration Configuration { get; }
    
    /// <summary>
    /// Whether Pro version is being configured.
    /// Used by AddRedbPro() to mark Pro mode.
    /// </summary>
    bool IsPro { get; set; }
    
    /// <summary>
    /// License key for Pro features (JWT token).
    /// Can also be set via REDB_PRO_LICENSE env var or redb.license file.
    /// </summary>
    string? LicenseKey { get; set; }
    
    /// <summary>
    /// Register provider-specific services.
    /// Called by UsePostgres(), UseMsSql(), etc.
    /// </summary>
    /// <param name="registration">Action that registers provider services.
    /// Parameters: services, configuration, isPro</param>
    void SetProviderRegistration(Action<IServiceCollection, RedbServiceConfiguration, bool> registration);
    
    /// <summary>
    /// Finalize configuration and register all services.
    /// Called after UseXxx() and Configure() methods.
    /// </summary>
    void Build();
}

/// <summary>
/// Builder for configuring REDB services.
/// Database providers (UsePostgres, UseMsSql) register their implementations via IRedbOptionsBuilderInfrastructure.
/// </summary>
public class RedbOptionsBuilder : IRedbOptionsBuilderInfrastructure
{
    private readonly IServiceCollection _services;
    private readonly RedbServiceConfiguration _configuration = new();
    private Action<IServiceCollection, RedbServiceConfiguration, bool>? _providerRegistration;
    
    private bool _isPro;
    private string? _licenseKey;
    
    public RedbOptionsBuilder(IServiceCollection services)
    {
        _services = services;
    }
    
    /// <summary>
    /// Set Pro license key (JWT token).
    /// If not set, license is searched in REDB_PRO_LICENSE env var or redb.license file.
    /// </summary>
    /// <param name="licenseKey">JWT license token</param>
    /// <returns>Builder for chaining</returns>
    public RedbOptionsBuilder WithLicense(string licenseKey)
    {
        _licenseKey = licenseKey;
        return this;
    }
    
    /// <summary>
    /// Configure RedbServiceConfiguration options.
    /// </summary>
    /// <example>
    /// builder.Configure(c => {
    ///     c.EnableLazyLoadingForProps = true;
    ///     c.EavSaveStrategy = EavSaveStrategy.ChangeTracking;
    /// });
    /// </example>
    public RedbOptionsBuilder Configure(Action<RedbServiceConfiguration> configure)
    {
        configure(_configuration);
        return this;
    }
    
    // === IRedbOptionsBuilderInfrastructure explicit implementation ===
    // Available to providers/Pro via cast: ((IRedbOptionsBuilderInfrastructure)builder)
    
    IServiceCollection IRedbOptionsBuilderInfrastructure.Services => _services;
    
    RedbServiceConfiguration IRedbOptionsBuilderInfrastructure.Configuration => _configuration;
    
    bool IRedbOptionsBuilderInfrastructure.IsPro
    {
        get => _isPro;
        set => _isPro = value;
    }
    
    string? IRedbOptionsBuilderInfrastructure.LicenseKey
    {
        get => _licenseKey;
        set => _licenseKey = value;
    }
    
    void IRedbOptionsBuilderInfrastructure.SetProviderRegistration(
        Action<IServiceCollection, RedbServiceConfiguration, bool> registration)
    {
        if (_providerRegistration != null)
            throw new InvalidOperationException(
                "Database provider already configured. Call UsePostgres/UseMsSql only once.");
        _providerRegistration = registration;
    }
    
    void IRedbOptionsBuilderInfrastructure.Build()
    {
        if (_providerRegistration == null)
            throw new InvalidOperationException(
                "No database provider configured. Call UsePostgres() or UseMsSql() in the options builder.");
        
        // Register configuration
        _services.AddSingleton(_configuration);
        
        // Register provider-specific services
        _providerRegistration(_services, _configuration, _isPro);
    }
}
