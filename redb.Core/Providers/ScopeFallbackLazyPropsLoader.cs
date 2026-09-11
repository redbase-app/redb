using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Entities;

namespace redb.Core.Providers;

/// <summary>
/// The loader a graph carries after it was PUT into the props cache (tsum pool incident,
/// 2026-09-09). The writer that cached the object keeps walking this very instance, so its
/// scoped loader must keep working through the writer's own connection - detaching on Set made
/// every reference touch rent a fresh scope and pooled connection, and a busy tick drained the
/// pool. But the instance may also outlive the writer's scope (application-level caches hold
/// objects for hours); a touch then must not die on the disposed-context guard. So: delegate to
/// the writer's scoped loader while its scope lives, and on the first
/// <see cref="ObjectDisposedException"/> switch permanently to the domain's
/// <see cref="DetachedLazyPropsLoader"/> (a fresh scope per load) - the same live-then-fresh
/// policy <c>RedbServiceBase.LoadLinkedObjectAsync</c> applies to list items.
/// <para>
/// A graph SERVED from the cache never carries this wrapper: the hand-out installs the pure
/// detached loader, because another scope must not ride the writer's still-live connection.
/// </para>
/// </summary>
public sealed class ScopeFallbackLazyPropsLoader : ILazyPropsLoader
{
    private readonly ILazyPropsLoader _scoped;
    private readonly DetachedLazyPropsLoader _detached;
    private volatile bool _scopeDead;

    public ScopeFallbackLazyPropsLoader(ILazyPropsLoader scoped, DetachedLazyPropsLoader detached)
    {
        _scoped = scoped ?? throw new ArgumentNullException(nameof(scoped));
        _detached = detached ?? throw new ArgumentNullException(nameof(detached));
    }

    /// <summary>The writer's loader this wrapper guards (diagnostics and tests).</summary>
    public ILazyPropsLoader Scoped => _scoped;

    public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
    {
        if (!_scopeDead)
        {
            try { return _scoped.LoadProps<TProps>(objectId, schemeId); }
            catch (ObjectDisposedException) { _scopeDead = true; }
        }
        return _detached.LoadProps<TProps>(objectId, schemeId);
    }

    public async Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        if (!_scopeDead)
        {
            try { return await _scoped.LoadPropsAsync<TProps>(objectId, schemeId, cancellationToken); }
            catch (ObjectDisposedException) { _scopeDead = true; }
        }
        return await _detached.LoadPropsAsync<TProps>(objectId, schemeId, cancellationToken);
    }

    public async Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        if (!_scopeDead)
        {
            try { await _scoped.LoadPropsForManyAsync(objects, cancellationToken); return; }
            catch (ObjectDisposedException) { _scopeDead = true; }
        }
        await _detached.LoadPropsForManyAsync(objects, cancellationToken);
    }

    public async Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        if (!_scopeDead)
        {
            try { await _scoped.LoadPropsForManyAsync(objects, projectedStructureIds, cancellationToken); return; }
            catch (ObjectDisposedException) { _scopeDead = true; }
        }
        await _detached.LoadPropsForManyAsync(objects, projectedStructureIds, cancellationToken);
    }

    public async Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        if (!_scopeDead)
        {
            try { await _scoped.LoadPropsForManyAsync(objects, propsDepth, cancellationToken); return; }
            catch (ObjectDisposedException) { _scopeDead = true; }
        }
        await _detached.LoadPropsForManyAsync(objects, propsDepth, cancellationToken);
    }

    public async Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
    {
        if (!_scopeDead)
        {
            try { await _scoped.LoadPropsForManyAsync(objects, projectedStructureIds, propsDepth, cancellationToken); return; }
            catch (ObjectDisposedException) { _scopeDead = true; }
        }
        await _detached.LoadPropsForManyAsync(objects, projectedStructureIds, propsDepth, cancellationToken);
    }
}
