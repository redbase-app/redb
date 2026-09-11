using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Models.Entities;
using redb.Core.Utils;

namespace redb.Core.Providers;

/// <summary>
/// V4 (review, owner decision 2026-09-01): the loader of a reference that lives in the props cache.
///
/// <para>
/// A cached object is one shared instance served to every scope, and the references inside it
/// carried the loader of the scope that loaded it - one connection, disposed with that scope. A
/// later request touching such a reference resurrected a pooled connection nobody returned, or
/// used the connection of another request still in flight. This loader owns no scope: every load
/// opens a fresh DI scope, takes the provider's own <see cref="ILazyPropsLoader"/> from it (Free
/// or Pro alike), loads, and closes the scope. Whatever that inner loader attached to the freshly
/// loaded graph is replaced by this loader again, so the cached graph never carries a scoped one.
/// </para>
///
/// <para>
/// Consequence to know: such a load reads COMMITTED state through its own connection, not the
/// uncommitted data of the caller's transaction - a shared cached object cannot belong to one
/// caller's transaction by definition.
/// </para>
/// </summary>
public sealed class DetachedLazyPropsLoader : ILazyPropsLoader
{
    private readonly IServiceScopeFactory _scopes;

    public DetachedLazyPropsLoader(IServiceScopeFactory scopes)
    {
        _scopes = scopes ?? throw new System.ArgumentNullException(nameof(scopes));
    }

    public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
    {
        // The inner loader owns the access mode (Throw) and the pool-thread hand-off.
        using var scope = _scopes.CreateScope();
        BorrowDiag(scope.ServiceProvider, objectId);
        var props = scope.ServiceProvider.GetRequiredService<ILazyPropsLoader>().LoadProps<TProps>(objectId, schemeId);
        LazyReferenceInstaller.InstallInto(props, this);
        return props;
    }

    public async Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        await using var scope = _scopes.CreateAsyncScope();
        BorrowDiag(scope.ServiceProvider, objectId);
        var props = await scope.ServiceProvider.GetRequiredService<ILazyPropsLoader>().LoadPropsAsync<TProps>(objectId, schemeId);
        LazyReferenceInstaller.InstallInto(props, this);
        return props;
    }

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new()
        => ForManyAsync(objects, (l, o) => l.LoadPropsForManyAsync(o));

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default)
        where TProps : class, new()
        => ForManyAsync(objects, (l, o) => l.LoadPropsForManyAsync(o, projectedStructureIds));

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
        => ForManyAsync(objects, (l, o) => l.LoadPropsForManyAsync(o, propsDepth));

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth, CancellationToken cancellationToken = default)
        where TProps : class, new()
        => ForManyAsync(objects, (l, o) => l.LoadPropsForManyAsync(o, projectedStructureIds, propsDepth));

    private async Task ForManyAsync<TProps>(
        List<RedbObject<TProps>> objects,
        System.Func<ILazyPropsLoader, List<RedbObject<TProps>>, Task> load) where TProps : class, new()
    {
        if (objects.Count == 0) return;
        await using var scope = _scopes.CreateAsyncScope();
        await load(scope.ServiceProvider.GetRequiredService<ILazyPropsLoader>(), objects);
        foreach (var obj in objects)
            LazyReferenceInstaller.Install(obj, this);
    }
    // Diagnostics (tsum pool incident, 2026-09-09): every detached load borrows a pooled
    // connection outside the caller's scope. The counter names a storm while it happens;
    // Debug level adds the stack that tells WHO touches lazy references past their scope.
    private static long _windowStartMs;
    private static int _windowCount;
    private const int BorrowStormThreshold = 200;

    private static void BorrowDiag(System.IServiceProvider scoped, long objectId)
    {
        // Rate detector: the pool dries out from the FREQUENCY of detached borrows, not from
        // their overlap. Count loads per 10s window; past the threshold, warn once per window.
        var now = System.Environment.TickCount64;
        var windowStart = System.Threading.Interlocked.Read(ref _windowStartMs);
        if (now - windowStart > 10_000 &&
            System.Threading.Interlocked.CompareExchange(ref _windowStartMs, now, windowStart) == windowStart)
            System.Threading.Interlocked.Exchange(ref _windowCount, 0);
        var inWindow = System.Threading.Interlocked.Increment(ref _windowCount);

        var logger = (scoped.GetService(typeof(Microsoft.Extensions.Logging.ILoggerFactory))
            as Microsoft.Extensions.Logging.ILoggerFactory)
            ?.CreateLogger(typeof(DetachedLazyPropsLoader).FullName!);
        if (logger == null) return;
        if (inWindow == BorrowStormThreshold)
            Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(logger,
                "Detached reference loads: {Count} within 10s - lazy references are being touched " +
                "on cached objects at a rate that rents the connection pool dry. Enable Debug " +
                "logging on this category for call stacks.", inWindow);
        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Debug))
            Microsoft.Extensions.Logging.LoggerExtensions.LogDebug(logger,
                "Detached reference load of object {ObjectId} at: {Stack}",
                objectId, System.Environment.StackTrace);
    }

}