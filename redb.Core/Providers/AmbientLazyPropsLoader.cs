using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;

namespace redb.Core.Providers;

/// <summary>
/// The loader every reference stub carries (owner decision 2026-09-15, plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md
/// §4.1). It owns no scope and no connection. A load runs on, in this order:
/// <list type="number">
/// <item>the live redb scope current for the reader (the flow that resolved it, or <c>BeginAccess</c>);</item>
/// <item>the scope that materialized the stub (its origin, held weakly), while that scope lives - never for a shared
/// instance of a cache, whose stubs carry the origin-less loader of their database;</item>
/// <item>otherwise it refuses with <see cref="RedbLazyLoadScopeEndedException"/>, or opens a fresh scope when the
/// configuration asks for it.</item>
/// </list>
/// It replaces the scope-bound loaders on stubs, the detached loader of the props cache and its dead-scope fallback: those
/// rode the connection of the materializing scope whoever read, or rented a fresh scope per load.
/// </summary>
internal sealed class AmbientLazyPropsLoader : ILazyPropsLoader
{
    private static readonly ConcurrentDictionary<string, AmbientLazyPropsLoader> ByDomain = new();
    private static readonly ConditionalWeakTable<RedbServiceBase, AmbientLazyPropsLoader> ByOrigin = new();

    private readonly string _domain;

    private AmbientLazyPropsLoader(string domain, RedbServiceBase? origin)
    {
        _domain = domain;
        Origin = origin?.SelfReference;
    }

    /// <summary>The loader of <paramref name="domain"/> for a shared instance: the reader's scope only.</summary>
    public static AmbientLazyPropsLoader For(string domain) => ByDomain.GetOrAdd(domain, d => new AmbientLazyPropsLoader(d, null));

    /// <summary>
    /// The loader of an instance materialized by <paramref name="origin"/>: the reader's scope, else the origin while it
    /// lives. Without an origin, the shared loader of the database.
    /// </summary>
    public static AmbientLazyPropsLoader For(string domain, RedbServiceBase? origin)
        => origin == null ? For(domain) : ByOrigin.GetValue(origin, o => new AmbientLazyPropsLoader(domain, o));

    /// <summary>The origin-less loader of the same database, for an instance that became shared.</summary>
    public AmbientLazyPropsLoader Shared => Origin == null ? this : For(_domain);

    /// <summary>The service that materialized the instances carrying this loader; null for a shared loader.</summary>
    public WeakReference<RedbServiceBase>? Origin { get; }

    /// <inheritdoc />
    public string? CacheDomain => _domain;

    private RedbServiceBase? Reader() => RedbAmbientScope.Resolve(_domain) ?? RedbAmbientScope.LiveOrigin(Origin);

    /// <summary>
    /// Whether a shared instance keeps what a load on the reader makes now: outside a transaction, or inside one that has
    /// written nothing yet - what it reads is committed state. Once the reader's transaction has written, what it reads
    /// may be its own uncommitted work, and a shared instance keeps nothing of it (owner decisions 2026-09-15, 2026-09-17).
    /// </summary>
    public bool ReaderKeepsLoads => Reader() is not { } reader || reader.LendsScope || Data.TransactionWrites.ReadsCommitted(reader.Context);

    /// <summary>Runs <paramref name="onCompleted"/> when the reader's transaction ends; false when the reader is in none.</summary>
    public bool OnReaderTransactionCompleted(Action<bool> onCompleted)
        => Reader() is { LendsScope: false } reader && Data.TransactionHooks.OnCompleted(reader.Context, onCompleted);

    public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
    {
        var reader = Reader();
        // A captive reader (a service of the root provider, outside a transaction) lends its scope factory, never its one connection.
        if (reader is { LendsScope: true } captive)
            return RedbDomainRegistry.InScopeOf(captive, service => service.LazyPropsLoader.LoadProps<TProps>(objectId, schemeId));
        return reader != null
            ? reader.LazyPropsLoader.LoadProps<TProps>(objectId, schemeId)
            : RedbDomainRegistry.InFreshScope(_domain,
                service => service.LazyPropsLoader.LoadProps<TProps>(objectId, schemeId),
                () => new RedbLazyLoadScopeEndedException(objectId, schemeId));
    }

    public async Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default)
        where TProps : class, new()
    {
        var reader = Reader();
        if (reader is { LendsScope: true } captive)
            return await RedbDomainRegistry.InScopeOfAsync(captive,
                service => service.LazyPropsLoader.LoadPropsAsync<TProps>(objectId, schemeId, cancellationToken)).ConfigureAwait(false);
        return reader != null
            ? await reader.LazyPropsLoader.LoadPropsAsync<TProps>(objectId, schemeId, cancellationToken).ConfigureAwait(false)
            : await RedbDomainRegistry.InFreshScopeAsync(_domain,
                service => service.LazyPropsLoader.LoadPropsAsync<TProps>(objectId, schemeId, cancellationToken),
                () => new RedbLazyLoadScopeEndedException(objectId, schemeId)).ConfigureAwait(false);
    }

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default)
        where TProps : class, new()
        => ForManyAsync(objects, (loader, list) => loader.LoadPropsForManyAsync(list, cancellationToken));

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds,
        CancellationToken cancellationToken = default) where TProps : class, new()
        => ForManyAsync(objects, (loader, list) => loader.LoadPropsForManyAsync(list, projectedStructureIds, cancellationToken));

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth,
        CancellationToken cancellationToken = default) where TProps : class, new()
        => ForManyAsync(objects, (loader, list) => loader.LoadPropsForManyAsync(list, propsDepth, cancellationToken));

    public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth,
        CancellationToken cancellationToken = default) where TProps : class, new()
        => ForManyAsync(objects, (loader, list) => loader.LoadPropsForManyAsync(list, projectedStructureIds, propsDepth, cancellationToken));

    private async Task ForManyAsync<TProps>(List<RedbObject<TProps>> objects,
        Func<ILazyPropsLoader, List<RedbObject<TProps>>, Task> load) where TProps : class, new()
    {
        if (objects.Count == 0) return;
        var reader = Reader();
        if (reader is { LendsScope: true } captive)
        {
            await RedbDomainRegistry.InScopeOfAsync(captive, async service =>
            {
                await load(service.LazyPropsLoader, objects).ConfigureAwait(false);
                return true;
            }).ConfigureAwait(false);
            return;
        }
        if (reader != null)
        {
            await load(reader.LazyPropsLoader, objects).ConfigureAwait(false);
            return;
        }
        await RedbDomainRegistry.InFreshScopeAsync(_domain, async service =>
            {
                await load(service.LazyPropsLoader, objects).ConfigureAwait(false);
                return true;
            },
            () => new RedbLazyLoadScopeEndedException(objects[0].id, objects[0].scheme_id)).ConfigureAwait(false);
    }
}
