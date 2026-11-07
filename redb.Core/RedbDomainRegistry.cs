using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Configuration;

namespace redb.Core;

/// <summary>
/// Per database (cache domain): the root scope factory and the configuration a lazy load needs when no live redb scope
/// reads it. By default such a load is refused; <see cref="RedbServiceConfiguration.LazyLoadWithoutScope"/> =
/// <see cref="LazyLoadWithoutScopeMode.FreshScope"/> opens a scope for that one load, outside any ambient transaction, and
/// warns once the rate gets high (owner decision 2026-09-15). Every service registers its database; the last registration
/// of a database wins.
/// </summary>
internal static class RedbDomainRegistry
{
    private sealed class Entry
    {
        public Entry(IServiceScopeFactory? scopes, RedbServiceConfiguration configuration, ILogger? logger)
        {
            Scopes = scopes;
            Configuration = configuration;
            Logger = logger;
        }

        public IServiceScopeFactory? Scopes { get; }
        public RedbServiceConfiguration Configuration { get; }
        public ILogger? Logger { get; }

        /// <summary>The entry this one replaced, set by the registration that replaced it.</summary>
        public Entry? Replaced { get; set; }
    }

    private const int FreshScopeLoadThreshold = 200;
    private static readonly ConcurrentDictionary<string, Entry> ByDomain = new();
    private static readonly ConcurrentDictionary<string, Utils.RateWindow> Rates = new();
    private static readonly ConcurrentDictionary<string, byte> ConflictReported = new();

    /// <summary>
    /// Registers the database of a service. The last registration wins: a host rebuilt in place (tests, hot reload)
    /// takes over from the disposed one. Two hosts of one database that disagree on
    /// <see cref="RedbServiceConfiguration.LazyLoadWithoutScope"/> flip the policy with every service constructed;
    /// that is reported once per database.
    /// </summary>
    public static void Register(string domain, IServiceScopeFactory? scopes, RedbServiceConfiguration configuration, ILogger? logger)
    {
        var entry = new Entry(scopes, configuration, logger);
        var previous = ByDomain.AddOrUpdate(domain, entry, (_, old) => { entry.Replaced = old; return entry; }).Replaced;
        if (previous == null || previous.Configuration.LazyLoadWithoutScope == configuration.LazyLoadWithoutScope
            || !ConflictReported.TryAdd(domain, 0))
            return;
        logger?.LogWarning(
            "Database '{Domain}' is used by two redb configurations that differ in LazyLoadWithoutScope ({Previous} and " +
            "{Current}). The registration of the most recently constructed service wins, so which policy a lazy load " +
            "with no live scope gets depends on which host constructed a service last. Give the hosts one policy.",
            domain, previous.Configuration.LazyLoadWithoutScope, configuration.LazyLoadWithoutScope);
    }

    /// <summary>
    /// Runs <paramref name="load"/> on a service of a fresh scope of the captive reader's container: a service of the root
    /// provider lends lazy loads its scope factory, never its one connection. No option is asked - the container the
    /// reader came from is known - and no ambient transaction is joined: the load reads committed state.
    /// </summary>
    public static T InScopeOf<T>(RedbServiceBase captive, Func<RedbServiceBase, T> load)
    {
        using var outside = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        using var scope = captive.ScopeFactory!.CreateScope();
        ReportCaptive(captive);
        return load(Lent(scope.ServiceProvider, captive));
    }

    /// <summary>Asynchronous twin of <see cref="InScopeOf{T}"/>.</summary>
    public static async Task<T> InScopeOfAsync<T>(RedbServiceBase captive, Func<RedbServiceBase, Task<T>> load)
    {
        using var outside = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await using var scope = captive.ScopeFactory!.CreateAsyncScope();
        ReportCaptive(captive);
        return await load(Lent(scope.ServiceProvider, captive)).ConfigureAwait(false);
    }

    // What the lent scope materializes belongs to the captive reader: its stubs name the captive service as their origin.
    private static RedbServiceBase Lent(IServiceProvider scope, RedbServiceBase captive)
    {
        var service = (RedbServiceBase)scope.GetRequiredService<IRedbService>();
        service.LentTo = captive;
        return service;
    }

    private static void ReportCaptive(RedbServiceBase captive)
    {
        var inWindow = Rates.GetOrAdd(captive.CacheDomain + "|captive",
            _ => new Utils.RateWindow(TimeSpan.FromSeconds(10), FreshScopeLoadThreshold)).Add();
        if (inWindow > 0)
            captive.Logger?.LogWarning(
                "Lazy loads through a captive service: {Count} within 10s - code reads lazy Object or Props on a service " +
                "resolved from the root provider, and every such load runs in a fresh scope on a pooled connection. " +
                "Resolve a scoped IRedbService per flow, or load explicitly.", inWindow);
    }

    /// <summary>Runs <paramref name="load"/> on a service of a fresh scope, or throws <paramref name="refusal"/>.</summary>
    public static T InFreshScope<T>(string? domain, Func<RedbServiceBase, T> load, Func<Exception> refusal)
    {
        var (entry, key) = FreshScopeEntry(domain, refusal);
        // A fresh-scope load reads committed state on a connection of its own and must not join an ambient transaction of
        // the caller. Declared first: disposed last.
        using var outside = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        using var scope = entry.Scopes!.CreateScope();
        Report(entry, key);
        return load((RedbServiceBase)scope.ServiceProvider.GetRequiredService<IRedbService>());
    }

    /// <summary>Asynchronous twin of <see cref="InFreshScope{T}"/>.</summary>
    public static async Task<T> InFreshScopeAsync<T>(string? domain, Func<RedbServiceBase, Task<T>> load, Func<Exception> refusal)
    {
        var (entry, key) = FreshScopeEntry(domain, refusal);
        using var outside = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await using var scope = entry.Scopes!.CreateAsyncScope();
        Report(entry, key);
        return await load((RedbServiceBase)scope.ServiceProvider.GetRequiredService<IRedbService>()).ConfigureAwait(false);
    }

    private static (Entry Entry, string Domain) FreshScopeEntry(string? domain, Func<Exception> refusal)
    {
        Entry? entry = null;
        var key = domain;
        if (domain != null)
            ByDomain.TryGetValue(domain, out entry);
        else if (ByDomain.Count == 1)
        {
            var only = ByDomain.First();
            (key, entry) = (only.Key, only.Value);
        }

        if (entry is not { Scopes: not null } || key == null
            || entry.Configuration.LazyLoadWithoutScope != LazyLoadWithoutScopeMode.FreshScope)
            throw refusal();
        return (entry, key);
    }

    private static void Report(Entry entry, string domain)
    {
        var inWindow = Rates.GetOrAdd(domain, _ => new Utils.RateWindow(TimeSpan.FromSeconds(10), FreshScopeLoadThreshold)).Add();
        if (inWindow > 0)
            entry.Logger?.LogWarning(
                "Lazy loads in fresh scopes: {Count} within 10s (LazyLoadWithoutScope = FreshScope) - code reads lazy " +
                "Object or Props with no live redb scope, and every such load rents a pooled connection. Load explicitly " +
                "inside a scope, or wrap the reads in redb.BeginAccess().", inWindow);
    }
}
