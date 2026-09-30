using redb.Core;

namespace RedbBlazor.Services;

/// <summary>
/// Runs one unit of work on its own <see cref="IRedbService"/>.
/// </summary>
/// <remarks>
/// In Blazor Server a scoped service lives as long as the user's circuit, and two event handlers of
/// one page can be in flight at the same time. An <see cref="IRedbService"/> is one connection and
/// does not take parallel calls, so components do not inject it: each operation opens a scope, uses
/// the service of that scope and disposes it. This is the same shape as IDbContextFactory in EF Core.
/// </remarks>
public sealed class RedbWork(IServiceScopeFactory scopes)
{
    public async Task<T> RunAsync<T>(Func<IRedbService, Task<T>> work)
    {
        await using var scope = scopes.CreateAsyncScope();
        return await work(scope.ServiceProvider.GetRequiredService<IRedbService>());
    }

    public async Task RunAsync(Func<IRedbService, Task> work)
    {
        await using var scope = scopes.CreateAsyncScope();
        await work(scope.ServiceProvider.GetRequiredService<IRedbService>());
    }
}
