using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Configuration;

namespace redb.Core.Services;

/// <summary>
/// Hosted service that initializes redb schema on startup.
/// Registered before BackgroundDeletionService to guarantee the schema
/// exists when other services start querying.
/// </summary>
internal sealed class RedbInitHostedService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly RedbServiceConfiguration _configuration;
    private readonly ILogger<RedbInitHostedService>? _logger;

    public RedbInitHostedService(
        IServiceProvider serviceProvider,
        RedbServiceConfiguration configuration,
        ILogger<RedbInitHostedService>? logger = null)
    {
        _serviceProvider = serviceProvider;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var redb = scope.ServiceProvider.GetService<IRedbService>();
        if (redb is null) return;

        await redb.InitializeAsync(ensureCreated: _configuration.EnsureCreated);
        _logger?.LogInformation("Redb initialized (EnsureCreated={EnsureCreated})", _configuration.EnsureCreated);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
