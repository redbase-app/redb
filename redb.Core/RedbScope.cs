using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace redb.Core;

/// <summary>
/// A scope opened from a service (<see cref="IRedbScopeSource.CreateScope"/>) and the service of the same database
/// inside it. The holder owns the scope: disposing it ends the service and returns its connection.
/// </summary>
public sealed class RedbScope : IDisposable, IAsyncDisposable
{
    private readonly IServiceScope? _scope;

    /// <summary>
    /// Wraps <paramref name="service"/> with the scope that owns it. A null scope is for a service owned elsewhere
    /// (a test double); disposing then does nothing.
    /// </summary>
    public RedbScope(IRedbService service, IServiceScope? scope)
    {
        ArgumentNullException.ThrowIfNull(service);
        Service = service;
        _scope = scope;
    }

    /// <summary>The service inside the scope.</summary>
    public IRedbService Service { get; }

    /// <summary>The scope's provider, for other scoped services of the same unit of work; null without a scope.</summary>
    public IServiceProvider? ServiceProvider => _scope?.ServiceProvider;

    /// <inheritdoc />
    public void Dispose() => _scope?.Dispose();

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_scope is IAsyncDisposable asyncScope)
            return asyncScope.DisposeAsync();
        _scope?.Dispose();
        return default;
    }
}
