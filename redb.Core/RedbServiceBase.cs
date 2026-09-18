using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Utils;
using redb.Core.Models.Enums;
using redb.Core.Models.Permissions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Security;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using redb.Core.Attributes;
using redb.Core.Exceptions;
using redb.Core.Models;
using redb.Core.Services;
using System.Reflection;
using System.Threading;
using System.Runtime.Loader;

namespace redb.Core;

/// <summary>
/// Base abstract class for RedbService implementations.
/// Contains all common logic for delegating to providers.
/// Provider-specific implementations (Postgres, MSSQL) inherit from this class.
/// </summary>
public abstract class RedbServiceBase : IRedbService
{
    private readonly IRedbContext _context;
    private readonly ISchemeSyncProvider _schemeSync;
    protected IObjectStorageProvider _objectStorage;
    protected ITreeProvider _treeProvider;
    private readonly IPermissionProvider _permissionProvider;
    protected IQueryableProvider _queryProvider;
    private readonly IValidationProvider _validationProvider;
    private readonly IRedbSecurityContext _securityContext;
    private readonly IUserProvider _userProvider;
    private readonly IRoleProvider _roleProvider;
    private readonly IListProvider _listProvider;
    private readonly IServiceProvider _serviceProvider;
    private RedbServiceConfiguration _configuration;
    private readonly IRedbObjectSerializer _serializer;
    private readonly ILogger? _logger;
    
    /// <summary>
    /// Cache domain identifier for isolating caches between different database connections.
    /// </summary>
    protected readonly string _cacheDomain;

    /// <summary>
    /// Database context for direct data access.
    /// </summary>
    public IRedbContext Context => _context;
    
    public IRedbSecurityContext SecurityContext => _securityContext;
    public IUserProvider UserProvider => _userProvider;
    public IRoleProvider RoleProvider => _roleProvider;
    public IListProvider ListProvider => _listProvider;
    public RedbServiceConfiguration Configuration => _configuration;
    
    /// <summary>
    /// Cache domain identifier for this service instance.
    /// </summary>
    public string CacheDomain => _cacheDomain;

    // === ABSTRACT METHODS FOR PROVIDER-SPECIFIC IMPLEMENTATIONS ===
    
    /// <summary>
    /// Database provider name (e.g., "PostgreSQL", "MSSql").
    /// </summary>
    protected abstract string DatabaseTypeName { get; }
    
    /// <summary>
    /// SQL query to get database version.
    /// </summary>
    protected abstract string GetVersionSql { get; }
    
    /// <summary>
    /// SQL query to get database size in bytes.
    /// </summary>
    protected abstract string GetDatabaseSizeSql { get; }
    
    /// <summary>
    /// Error message when IRedbContext is not registered.
    /// </summary>
    protected abstract string ContextNotRegisteredError { get; }
    
    /// <summary>
    /// Create scheme sync provider.
    /// </summary>
    protected abstract ISchemeSyncProvider CreateSchemeSyncProvider(
        IRedbContext context, RedbServiceConfiguration config, string cacheDomain, ILogger? logger);
    
    /// <summary>
    /// Create permission provider.
    /// </summary>
    protected abstract IPermissionProvider CreatePermissionProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger);
    
    /// <summary>
    /// Create user provider.
    /// </summary>
    protected abstract IUserProvider CreateUserProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger);
    
    /// <summary>
    /// Create role provider.
    /// </summary>
    protected abstract IRoleProvider CreateRoleProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger);
    
    /// <summary>
    /// Create list provider.
    /// </summary>
    protected abstract IListProvider CreateListProvider(
        IRedbContext context, RedbServiceConfiguration config, ISchemeSyncProvider schemeSync, ILogger? logger);
    
    /// <summary>
    /// Save-pipeline interceptors from DI (discussion #12) - resolved once, handed to the
    /// storage provider. Pro services building their own storage call this too.
    /// </summary>
    protected IEnumerable<Interception.IRedbSaveInterceptor>? ResolveSaveInterceptors()
        => _serviceProvider.GetService(typeof(IEnumerable<Interception.IRedbSaveInterceptor>))
            as IEnumerable<Interception.IRedbSaveInterceptor>;

    /// <summary>
    /// Password hasher for the user provider: a DI registration wins, the fallback is bcrypt.
    /// Never SimplePasswordHasher - a bare redb deployment must not default to salted SHA256
    /// (external security report, 2026-09-08). Legacy SHA256+salt hashes keep validating:
    /// BcryptPasswordHasher recognizes both formats.
    /// </summary>
    protected Security.IPasswordHasher ResolvePasswordHasher()
        => _serviceProvider.GetService(typeof(Security.IPasswordHasher)) as Security.IPasswordHasher
            ?? new Security.BcryptPasswordHasher();

    /// <summary>
    /// Create object storage provider.
    /// </summary>
    protected abstract IObjectStorageProvider CreateObjectStorageProvider(
        IRedbContext context, IRedbObjectSerializer serializer, IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext, ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration config, IListProvider listProvider, ILogger? logger,
        IEnumerable<Interception.IRedbSaveInterceptor>? saveInterceptors);
    
    /// <summary>
    /// Create tree provider.
    /// </summary>
    protected abstract ITreeProvider CreateTreeProvider(
        IRedbContext context, IObjectStorageProvider objectStorage, IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer, IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync, RedbServiceConfiguration config, ILogger? logger);
    
    /// <summary>
    /// Create lazy props loader.
    /// </summary>
    protected abstract ILazyPropsLoader CreateLazyPropsLoader(
        IRedbContext context, ISchemeSyncProvider schemeSync, IRedbObjectSerializer serializer,
        RedbServiceConfiguration config, string cacheDomain, IListProvider listProvider, ILogger? logger);
    
    /// <summary>
    /// Create queryable provider.
    /// </summary>
    protected abstract IQueryableProvider CreateQueryableProvider(
        IRedbContext context, IRedbObjectSerializer serializer, ISchemeSyncProvider schemeSync,
        IRedbSecurityContext securityContext, ILazyPropsLoader lazyPropsLoader,
        RedbServiceConfiguration config, string cacheDomain, ILogger? logger);
    
    /// <summary>
    /// Create validation provider.
    /// </summary>
    protected abstract IValidationProvider CreateValidationProvider(
        IRedbContext context, ILogger? logger);

    /// <summary>
    /// Factory for the maintenance provider (planner statistics, index health). Lazy: most
    /// scopes never touch maintenance, so nothing is built until the first access.
    /// </summary>
    protected abstract IMaintenanceProvider CreateMaintenanceProvider(
        IRedbContext context, int analysisLimit, ILogger? logger);

    private IMaintenanceProvider? _maintenance;

    /// <inheritdoc />
    public IMaintenanceProvider Maintenance
        => _maintenance ??= CreateMaintenanceProvider(_context, _configuration.MaintenanceAnalysisLimit, _logger);
    
    /// <summary>
    /// SQL dialect for database-specific queries.
    /// </summary>
    protected abstract ISqlDialect SqlDialect { get; }

    /// <summary>
    /// Initialize RedbServiceBase with dependency injection.
    /// </summary>
    protected RedbServiceBase(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
        _context = serviceProvider.GetService<IRedbContext>() ?? 
            throw new InvalidOperationException(ContextNotRegisteredError);
        _serializer = serviceProvider.GetService<IRedbObjectSerializer>() ?? new SystemTextJsonRedbSerializer();
        _logger = serviceProvider.GetService<ILogger<RedbServiceBase>>() as ILogger;
        _configuration = serviceProvider.GetService<RedbServiceConfiguration>() ?? new RedbServiceConfiguration();
        _securityContext = serviceProvider.GetService<IRedbSecurityContext>() ?? 
                          AmbientSecurityContext.GetOrCreateDefault();

        // Compute cache domain for this service instance
        _cacheDomain = _configuration.GetEffectiveCacheDomain();
        
        // Create providers using abstract factory methods (passing cacheDomain)
        _schemeSync = CreateSchemeSyncProvider(_context, _configuration, _cacheDomain, _logger);
        _permissionProvider = CreatePermissionProvider(_context, _securityContext, _logger);
        _userProvider = CreateUserProvider(_context, _securityContext, _logger);
        _roleProvider = CreateRoleProvider(_context, _securityContext, _logger);
        _listProvider = CreateListProvider(_context, _configuration, _schemeSync, _logger);
        
        _objectStorage = CreateObjectStorageProvider(_context, _serializer, _permissionProvider,
            _securityContext, _schemeSync, _configuration, _listProvider, _logger,
            ResolveSaveInterceptors());
        _treeProvider = CreateTreeProvider(_context, _objectStorage, _permissionProvider, 
            _serializer, _securityContext, _schemeSync, _configuration, _logger);
        
        // A data object owns no connection (owner decision 2026-09-15, plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md
        // §4.1): RedbListItem.Object and the Props of reference stubs load on the live scope of whoever reads them. This
        // service becomes current for the flow that resolved it, and registers its database for the loads that find no
        // live scope (refused by default, a fresh scope by explicit option). The per-item and process-wide loader
        // delegates it installed before opened a fresh scope and pooled connection per read.
        _listProvider.LinkedObjectsBatchLoader = LoadLinkedObjectsBatchAsync;
        ScopeFactory = serviceProvider.GetService<IServiceScopeFactory>();
        RedbDomainRegistry.Register(_cacheDomain, ScopeFactory, _configuration, _logger);
        // A service of the root provider is captive: it lives as long as the process, and the frame it enters below is
        // inherited by every flow. It lends lazy loads its scope factory, never its one connection (RedbDomainRegistry.
        // InScopeOf) - parallel lazy loads of shared instances collided on that connection (review after 4.0.0).
        IsCaptive = ScopeFactory != null && RedbServiceProviders.IsRoot(serviceProvider);

        LazyPropsLoader = CreateLazyPropsLoader(_context, _schemeSync, _serializer,
            _configuration, _cacheDomain, _listProvider, _logger);

        _queryProvider = CreateQueryableProvider(_context, _serializer, _schemeSync,
            _securityContext, LazyPropsLoader, _configuration, _cacheDomain, _logger);
        _validationProvider = CreateValidationProvider(_context, _logger);

        SelfReference = new WeakReference<RedbServiceBase>(this);
        ServicesByContext.AddOrUpdate(_context, this);
        RedbAmbientScope.Enter(this);
    }

    // === Lazy loads on the reader's scope ===

    // Context (one scope, one connection) -> the service of that scope: how a scope-bound loader names the origin of the
    // instances it materializes. Weak both ways: an ended scope leaves nothing behind.
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<IRedbContext, RedbServiceBase> ServicesByContext = new();

    /// <summary>The service whose scope owns <paramref name="context"/>; null for an unknown context.</summary>
    internal static RedbServiceBase? ServiceOf(IRedbContext? context)
        => context != null && ServicesByContext.TryGetValue(context, out var service) ? service : null;

    /// <summary>A weak reference to this service, shared by every instance it is the origin of.</summary>
    internal WeakReference<RedbServiceBase> SelfReference { get; }

    /// <summary>
    /// This service's own lazy Props loader, bound to its context. Reference stubs never carry it: they carry the
    /// scope-free loader of this database, which calls the loader of whichever live service reads them.
    /// </summary>
    protected internal ILazyPropsLoader LazyPropsLoader { get; protected set; } = null!;

    /// <summary>Whether this service's scope has ended (its context is disposed): it no longer answers lazy loads.</summary>
    internal bool IsScopeEnded => _context.IsDisposed;

    /// <summary>The service was resolved from the root provider of its container (see <see cref="RedbServiceProviders"/>).</summary>
    internal bool IsCaptive { get; }

    /// <summary>
    /// Whether a lazy load resolved to this reader runs in a fresh scope of its container instead of on its connection: a
    /// captive service outside a transaction. Inside its transaction the connection is the transaction, and the load is
    /// part of it - it reads what the transaction wrote, as it did before.
    /// </summary>
    internal bool LendsScope => IsCaptive && !_context.IsInTransaction;

    /// <summary>The logger of this service; null when the host registered none.</summary>
    internal ILogger? Logger => _logger;

    /// <summary>The scope factory of the container that resolved this service; null outside DI.</summary>
    internal IServiceScopeFactory? ScopeFactory { get; }

    /// <summary>
    /// The captive service this scope was lent to for one load (<see cref="RedbDomainRegistry.InScopeOf{T}"/>); null for a
    /// scope of its own.
    /// </summary>
    internal RedbServiceBase? LentTo { get; set; }

    /// <summary>
    /// The service that instances materialized by this one name as their origin: itself, or the captive service it was lent
    /// to. A lent scope ends with its load, while what it loaded lives on with the captive reader - the stubs under it
    /// load through a lent scope of their own.
    /// </summary>
    internal RedbServiceBase AsOrigin => LentTo ?? this;

    // === A scope of this service's container (IRedbScopeSource) ===

    /// <inheritdoc />
    public bool CanCreateScope => ScopeFactory != null;

    /// <inheritdoc />
    public RedbScope CreateScope()
    {
        if (ScopeFactory == null)
            throw new InvalidOperationException(
                "This IRedbService was built without a DI container (its provider has no IServiceScopeFactory), so it " +
                "cannot open a scope of its own. Resolve services from a container to get one per unit of work.");
        var scope = ScopeFactory.CreateAsyncScope();
        RedbScope? opened = null;
        try
        {
            var service = scope.ServiceProvider.GetRequiredService<IRedbService>();
            if (!string.Equals(service.CacheDomain, CacheDomain, StringComparison.Ordinal))
                throw new InvalidOperationException(
                    $"A scope of this service's container resolves IRedbService for database '{service.CacheDomain}', not " +
                    $"'{CacheDomain}': the container registers IRedbService differently per resolution. A scope opened from " +
                    "a service must read that service's database; register one IRedbService per container.");
            opened = new RedbScope(service, scope);
            return opened;
        }
        finally
        {
            if (opened == null)
                scope.Dispose();
        }
    }

    /// <inheritdoc />
    public IDisposable BeginAccess() => RedbAmbientScope.Push(this);

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<long, IRedbObject>> LoadLinkedObjectsAsync(
        IEnumerable<RedbListItem> items, CancellationToken cancellationToken = default)
    {
        var pending = items.Where(i => !i.IsObjectLoaded && i.IdObject.HasValue).ToList();
        if (pending.Count == 0)
            return new Dictionary<long, IRedbObject>();

        var byId = await LoadLinkedObjectsBatchAsync(pending.Select(i => i.IdObject!.Value).Distinct().ToList(), cancellationToken);
        foreach (var item in pending)
            if (byId.TryGetValue(item.IdObject!.Value, out var obj))
                item.Publish(obj, _context);
        return byId;
    }

    /// <summary>
    /// Batch resolver behind the list hand-out preload: one polymorphic load for all linked
    /// objects of a list. Depth 10 - the SAME depth the lazy getter path uses (LoadTypedAsync),
    /// so a preloaded extension object is indistinguishable from a lazily loaded one (dictionary
    /// extension objects are used actively and may carry nested references). Missing ids are
    /// simply absent from the result - their items keep the lazy path.
    /// </summary>
    private async Task<IReadOnlyDictionary<long, IRedbObject>> LoadLinkedObjectsBatchAsync(
        IReadOnlyCollection<long> objectIds, CancellationToken cancellationToken)
    {
        var loaded = await _objectStorage.LoadAsync(objectIds, depth: 10, cancellationToken: cancellationToken);
        var byId = new Dictionary<long, IRedbObject>(loaded.Count);
        foreach (var obj in loaded)
            byId[obj.Id] = obj;
        return byId;
    }

    /// <summary>
    /// The object behind a list item, loaded on this service's connection - the scope of whoever reads the item. A scope
    /// that tears down meanwhile refuses the command (ObjectDisposedException); nothing falls back to another scope.
    /// </summary>
    internal Task<IRedbObject?> LoadLinkedObjectAsync(long objectId, CancellationToken cancellationToken)
        => LoadLinkedObjectCoreAsync(this, objectId, cancellationToken);

    private static async Task<IRedbObject?> LoadLinkedObjectCoreAsync(IRedbService service, long objectId, CancellationToken cancellationToken)
    {
        var dialect = (service as RedbServiceBase)?.SqlDialect;
        var schemeId = dialect != null
            ? await service.Context.ExecuteScalarAsync<long>(
                dialect.ObjectStorage_SelectSchemeIdByObjectId(), new object[] { objectId }, cancellationToken)
            : 0;
        if (schemeId == 0)
            return null;

        // A scheme this node has never cached (a fresh process, another cluster node) is resolved by
        // its id; the cache-only lookup fell through to the untyped get_object_json path below.
        var propsType = await service.Cache.ResolveClrTypeAsync(schemeId, service);
        if (propsType != null)
        {
            // The generic method is our own, so its shape is under our control: the previous
            // lookup of IObjectStorageProvider.LoadAsync by parameter list silently returned null
            // (NRE at runtime) the moment that signature grew a CancellationToken.
            var load = typeof(RedbServiceBase)
                .GetMethod(nameof(LoadTypedAsync), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(propsType);
            return await (Task<IRedbObject?>)load.Invoke(null, new object[] { service, objectId, cancellationToken })!;
        }

        if (service is not RedbServiceBase typed)
            return null;

        var json = await service.Context.ExecuteJsonAsync(
            typed.SqlDialect.ObjectStorage_GetObjectJson(), new object[] { objectId, 10 }, cancellationToken);
        return string.IsNullOrEmpty(json) ? null : typed._serializer.DeserializeDynamic(json, typeof(object));
    }

    private static async Task<IRedbObject?> LoadTypedAsync<TProps>(IObjectStorageProvider storage, long objectId, CancellationToken cancellationToken)
        where TProps : class, new()
        => await storage.LoadAsync<TProps>(objectId, 10, cancellationToken);

    // === Synchronous twins (thread-pool-free lazy path) ===
    // The sync getter of RedbListItem.Object prefers these: the whole load - scheme lookup,
    // permission check, get_object_json, deserialization - runs on the calling thread down to
    // ADO.NET, so a saturated thread pool cannot slow or deadlock a touch of Object. Mirrors of
    // the async chain above, same depth, same diagnostics.

    /// <summary>Synchronous twin of <see cref="LoadLinkedObjectAsync"/>, on the calling thread down to ADO.NET.</summary>
    internal IRedbObject? LoadLinkedObjectSync(long objectId) => LoadLinkedObjectCoreSync(this, objectId);

    private static IRedbObject? LoadLinkedObjectCoreSync(IRedbService service, long objectId)
    {
        var dialect = (service as RedbServiceBase)?.SqlDialect;
        var schemeId = dialect != null
            ? service.Context.ExecuteScalar<long>(dialect.ObjectStorage_SelectSchemeIdByObjectId(), objectId)
            : 0;
        if (schemeId == 0)
            return null;

        // Same cold resolution as the async twin, on the calling thread.
        var propsType = service.Cache.ResolveClrType(schemeId, service);
        if (propsType != null)
        {
            // Same reflection shape as the async twin (and the same lesson: our own method, found
            // by name, never by parameter list).
            var load = typeof(RedbServiceBase)
                .GetMethod(nameof(LoadTypedSync), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(propsType);
            return (IRedbObject?)load.Invoke(null, new object[] { service, objectId });
        }

        if (service is not RedbServiceBase typed)
            return null;

        var json = service.Context.ExecuteJson(typed.SqlDialect.ObjectStorage_GetObjectJson(), objectId, 10);
        return string.IsNullOrEmpty(json) ? null : typed._serializer.DeserializeDynamic(json, typeof(object));
    }

    private static IRedbObject? LoadTypedSync<TProps>(IObjectStorageProvider storage, long objectId)
        where TProps : class, new()
        => storage.Load<TProps>(objectId, 10);
    
    /// <summary>
    /// Get tree provider for extended scenarios.
    /// </summary>
    public ITreeProvider GetTreeProvider() => _treeProvider;

    // === DATABASE METADATA ===
    // Sync properties ride the true sync path (calling thread down to ADO.NET) - the previous
    // .Result over the async form parked a thread on pool-scheduled continuations (debug-era
    // leftover; same class as the lazy-getter fix).
    public string dbVersion => _context.ExecuteScalar<string>(GetVersionSql) ?? "unknown";
    public string dbType => DatabaseTypeName;
    public string dbMigration => "ADO.NET";
    public long? dbSize => _context.ExecuteScalar<long>(GetDatabaseSizeSql);

    /// <inheritdoc />
    public Task<string?> GetDbVersionAsync(CancellationToken cancellationToken = default)
        => _context.ExecuteScalarAsync<string>(GetVersionSql);

    // === ISchemeSyncProvider DELEGATION ===

    public Task<IRedbScheme> EnsureSchemeFromTypeAsync<TProps>(CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.EnsureSchemeFromTypeAsync<TProps>(cancellationToken);
    
    public Task<List<IRedbStructure>> SyncStructuresFromTypeAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true, CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.SyncStructuresFromTypeAsync<TProps>(scheme, strictDeleteExtra, cancellationToken);
    
    public Task<IRedbScheme> SyncSchemeAsync<TProps>(CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.SyncSchemeAsync<TProps>(cancellationToken);

    public Task<IRedbScheme?> GetSchemeByTypeAsync<TProps>(CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.GetSchemeByTypeAsync<TProps>(cancellationToken);

    public Task<IRedbScheme?> GetSchemeByTypeAsync(Type type, CancellationToken cancellationToken = default)
        => _schemeSync.GetSchemeByTypeAsync(type, cancellationToken);
    
    public IRedbScheme? GetSchemeFromCache<TProps>() where TProps : class
        => _schemeSync.GetSchemeFromCache<TProps>();
    
    public IRedbScheme? GetSchemeFromCache(string schemeName)
        => _schemeSync.GetSchemeFromCache(schemeName);

    public Task<IRedbScheme> LoadSchemeByTypeAsync<TProps>(CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.LoadSchemeByTypeAsync<TProps>(cancellationToken);

    public Task<IRedbScheme> LoadSchemeByTypeAsync(Type type, CancellationToken cancellationToken = default)
        => _schemeSync.LoadSchemeByTypeAsync(type, cancellationToken);

    public Task<List<IRedbStructure>> GetStructuresByTypeAsync<TProps>(CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.GetStructuresByTypeAsync<TProps>(cancellationToken);

    public Task<List<IRedbStructure>> GetStructuresByTypeAsync(Type type, CancellationToken cancellationToken = default)
        => _schemeSync.GetStructuresByTypeAsync(type, cancellationToken);

    public Task<bool> SchemeExistsForTypeAsync<TProps>(CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.SchemeExistsForTypeAsync<TProps>(cancellationToken);

    public Task<bool> SchemeExistsForTypeAsync(Type type, CancellationToken cancellationToken = default)
        => _schemeSync.SchemeExistsForTypeAsync(type, cancellationToken);

    public Task<bool> SchemeExistsByNameAsync(string schemeName, CancellationToken cancellationToken = default)
        => _schemeSync.SchemeExistsByNameAsync(schemeName, cancellationToken);

    public string GetSchemeNameForType<TProps>() where TProps : class
        => _schemeSync.GetSchemeNameForType<TProps>();

    public string GetSchemeNameForType(Type type)
        => _schemeSync.GetSchemeNameForType(type);

    public string? GetSchemeAliasForType<TProps>() where TProps : class
        => _schemeSync.GetSchemeAliasForType<TProps>();

    public string? GetSchemeAliasForType(Type type)
        => _schemeSync.GetSchemeAliasForType(type);
    
    public Task<IRedbScheme?> GetSchemeByIdAsync(long schemeId, CancellationToken cancellationToken = default)
        => _schemeSync.GetSchemeByIdAsync(schemeId, cancellationToken);

    public IRedbScheme? GetSchemeById(long schemeId)
        => _schemeSync.GetSchemeById(schemeId);
    
    public Task<IRedbScheme?> GetSchemeByNameAsync(string schemeName, CancellationToken cancellationToken = default)
        => _schemeSync.GetSchemeByNameAsync(schemeName, cancellationToken);
    
    public Task<IRedbScheme> EnsureObjectSchemeAsync(string name, CancellationToken cancellationToken = default)
        => _schemeSync.EnsureObjectSchemeAsync(name, cancellationToken);
    
    public Task<IRedbScheme?> GetObjectSchemeAsync(string name, CancellationToken cancellationToken = default)
        => _schemeSync.GetObjectSchemeAsync(name, cancellationToken);
    
    public Task<List<IRedbScheme>> GetSchemesAsync(CancellationToken cancellationToken = default)
        => _schemeSync.GetSchemesAsync(cancellationToken);
    
    public GlobalMetadataCache Cache => _schemeSync.Cache;
    public GlobalListCache ListCache => _schemeSync.ListCache;
    public GlobalPropsCache PropsCache => _schemeSync.PropsCache;
    
    public Task<List<IRedbStructure>> GetStructuresAsync(IRedbScheme scheme, CancellationToken cancellationToken = default)
        => _schemeSync.GetStructuresAsync(scheme, cancellationToken);
    
    public Task<TypeMigrationResult> MigrateStructureTypeAsync(long structureId, string oldTypeName, string newTypeName, bool dryRun = false, CancellationToken cancellationToken = default)
        => _schemeSync.MigrateStructureTypeAsync(structureId, oldTypeName, newTypeName, dryRun, cancellationToken);

    public Task<Models.UniqueRecomputeReport> RecomputeUniqueAsync<TProps>(string propertyName, CancellationToken cancellationToken = default) where TProps : class
        => _schemeSync.RecomputeUniqueAsync<TProps>(propertyName, cancellationToken);
    
    public Task<List<StructureTreeNode>> GetStructureTreeAsync(long schemeId, CancellationToken cancellationToken = default)
        => _schemeSync.GetStructureTreeAsync(schemeId, cancellationToken);
    
    public Task<List<StructureTreeNode>> GetSubtreeAsync(long schemeId, long? parentStructureId, CancellationToken cancellationToken = default)
        => _schemeSync.GetSubtreeAsync(schemeId, parentStructureId, cancellationToken);
    
    public void InvalidateStructureTreeCache(long schemeId)
        => _schemeSync.InvalidateStructureTreeCache(schemeId);
    
    public (int TreesCount, int SubtreesCount, long MemoryEstimate) GetStructureTreeCacheStats()
        => _schemeSync.GetStructureTreeCacheStats();

    // === IObjectStorageProvider DELEGATION ===

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(objectId, depth, cancellationToken);

    // Without this delegation the facade would satisfy IObjectStorageProvider.Load with the
    // interface DEFAULT - the blocking-over-async fallback - and the sync lazy path would quietly
    // lose its thread-pool freedom (caught by the starvation stand: 4 capped threads, total
    // freeze inside ConfiguredTaskAwaiter.GetResult under IObjectStorageProvider.Load).
    public RedbObject<TProps>? Load<TProps>(long objectId, int depth = 10) where TProps : class, new()
        => _objectStorage.Load<TProps>(objectId, depth);

    public Task<RedbObject<TProps>?> GetByUniqueAsync<TProps>(System.Linq.Expressions.Expression<Func<TProps, object?>> keyProperty, object? value, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.GetByUniqueAsync(keyProperty, value, depth, cancellationToken);

    public Task<RedbObject<TProps>?> GetByUniqueAsync<TProps>(string propertyName, object? value, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.GetByUniqueAsync<TProps>(propertyName, value, depth, cancellationToken);

    public Task<long> SaveByUniqueAsync<TProps>(RedbObject<TProps> obj, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.SaveByUniqueAsync(obj, cancellationToken);

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(obj, depth, cancellationToken);

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(obj, user, depth, cancellationToken);

    public Task<long> SaveAsync(IRedbObject obj, CancellationToken cancellationToken = default)
        => _objectStorage.SaveAsync(obj, cancellationToken);

    public Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.SaveAsync(obj, cancellationToken);

    public Task<bool> DeleteAsync(IRedbObject obj, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(obj, cancellationToken);

    public Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.LoadAsync<TProps>(objectId, user, depth, cancellationToken);

    public Task<string?> LoadJsonAsync(long objectId, int depth = 10, CancellationToken cancellationToken = default)
        => _objectStorage.LoadJsonAsync(objectId, depth, cancellationToken);

    public Task<string?> LoadJsonAsync(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default)
        => _objectStorage.LoadJsonAsync(objectId, user, depth, cancellationToken);

    public Task<long> SaveAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _objectStorage.SaveAsync(obj, user, cancellationToken);

    public Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.SaveAsync(obj, user, cancellationToken);

    public Task<bool> DeleteAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(obj, user, cancellationToken);

    public Task<bool> DeleteAsync(long objectId, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(objectId, cancellationToken);

    public Task<bool> DeleteAsync(long objectId, IRedbUser user, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(objectId, user, cancellationToken);

    public Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.AddNewObjectsAsync(objects, cancellationToken);

    public Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => _objectStorage.AddNewObjectsAsync(objects, user, cancellationToken);

    public Task<int> DeleteAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(objectIds, cancellationToken);

    public Task<int> DeleteAsync(IEnumerable<long> objectIds, IRedbUser user, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(objectIds, user, cancellationToken);

    public Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(objects, cancellationToken);

    public Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default)
        => _objectStorage.DeleteAsync(objects, user, cancellationToken);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, long? trashParentId = null, CancellationToken cancellationToken = default)
        => _objectStorage.SoftDeleteAsync(objectIds, trashParentId, cancellationToken);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, IRedbUser user, long? trashParentId = null, CancellationToken cancellationToken = default)
        => _objectStorage.SoftDeleteAsync(objectIds, user, trashParentId, cancellationToken);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, long? trashParentId = null, CancellationToken cancellationToken = default)
        => _objectStorage.SoftDeleteAsync(objects, trashParentId, cancellationToken);
    
    public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, long? trashParentId = null, CancellationToken cancellationToken = default)
        => _objectStorage.SoftDeleteAsync(objects, user, trashParentId, cancellationToken);
    
    public Task DeleteWithPurgeAsync(
        IEnumerable<long> objectIds, 
        int batchSize = 10,
        IProgress<PurgeProgress>? progress = null,
        CancellationToken cancellationToken = default,
        long? trashParentId = null)
        => _objectStorage.DeleteWithPurgeAsync(objectIds, batchSize, progress, cancellationToken, trashParentId);
    
    public Task PurgeTrashAsync(
        long trashId,
        int totalCount,
        int batchSize = 10,
        IProgress<PurgeProgress>? progress = null,
        CancellationToken cancellationToken = default)
        => _objectStorage.PurgeTrashAsync(trashId, totalCount, batchSize, progress, cancellationToken);
    
    public Task<PurgeProgress?> GetDeletionProgressAsync(long trashId, CancellationToken cancellationToken = default)
        => _objectStorage.GetDeletionProgressAsync(trashId, cancellationToken);
    
    public Task<List<PurgeProgress>> GetUserActiveDeletionsAsync(long userId, CancellationToken cancellationToken = default)
        => _objectStorage.GetUserActiveDeletionsAsync(userId, cancellationToken);
    
    public Task<List<OrphanedTask>> GetOrphanedDeletionTasksAsync(int timeoutMinutes = 30, CancellationToken cancellationToken = default)
        => _objectStorage.GetOrphanedDeletionTasksAsync(timeoutMinutes, cancellationToken);
    
    public Task<bool> TryClaimOrphanedTaskAsync(long trashId, int timeoutMinutes = 30, CancellationToken cancellationToken = default)
        => _objectStorage.TryClaimOrphanedTaskAsync(trashId, timeoutMinutes, cancellationToken);

    public Task<int> LockForUpdateAsync(params long[] objectIds)
        => _objectStorage.LockForUpdateAsync(objectIds);

    public Task<int> LockForUpdateAsync(long[] objectIds, CancellationToken cancellationToken)
        => _objectStorage.LockForUpdateAsync(objectIds, cancellationToken);

    public Task LockForUpdateRequiredAsync(params long[] objectIds)
        => _objectStorage.LockForUpdateRequiredAsync(objectIds);

    public Task LockForUpdateRequiredAsync(long[] objectIds, CancellationToken cancellationToken)
        => _objectStorage.LockForUpdateRequiredAsync(objectIds, cancellationToken);

    public Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default)
        => _objectStorage.LoadAsync(objectIds, depth, cancellationToken);

    public Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default)
        => _objectStorage.LoadAsync(objectIds, user, depth, cancellationToken);

    public Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, CancellationToken cancellationToken = default)
        => _objectStorage.SaveAsync(objects, cancellationToken);

    public Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default)
        => _objectStorage.SaveAsync(objects, user, cancellationToken);

    // V4 (LAZY Л2 §4.6): batch reload of reference stubs
    public Task LoadReferencesAsync<TProps, TRef>(RedbObject<TProps> parent, Func<TProps, IEnumerable<RedbObject<TRef>?>?> references, CancellationToken cancellationToken = default)
        where TProps : class, new() where TRef : class, new()
        => _objectStorage.LoadReferencesAsync(parent, references, cancellationToken);

    public Task LoadReferencesAsync<TProps, TRef>(RedbObject<TProps> parent, Func<TProps, RedbObject<TRef>?> reference, CancellationToken cancellationToken = default)
        where TProps : class, new() where TRef : class, new()
        => _objectStorage.LoadReferencesAsync(parent, reference, cancellationToken);

    // LoadWithParentsAsync - Load with parent chain to root
    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(long objectId, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectId, depth, cancellationToken);

    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IRedbObject obj, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync<TProps>(obj, depth, cancellationToken);

    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(long objectId, IRedbUser user, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectId, user, depth, cancellationToken);

    Task<TreeRedbObject<TProps>?> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IRedbObject obj, IRedbUser user, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync<TProps>(obj, user, depth, cancellationToken);

    Task<List<TreeRedbObject<TProps>>> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectIds, depth, cancellationToken);

    Task<List<TreeRedbObject<TProps>>> IObjectStorageProvider.LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, IRedbUser user, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync<TProps>(objectIds, user, depth, cancellationToken);

    Task<List<ITreeRedbObject>> IObjectStorageProvider.LoadWithParentsAsync(IEnumerable<long> objectIds, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync(objectIds, depth, cancellationToken);

    Task<List<ITreeRedbObject>> IObjectStorageProvider.LoadWithParentsAsync(IEnumerable<long> objectIds, IRedbUser user, int depth, CancellationToken cancellationToken)
        => _objectStorage.LoadWithParentsAsync(objectIds, user, depth, cancellationToken);

    // === ITreeProvider DELEGATION ===

    public Task<int> DeleteSubtreeAsync(IRedbObject parentObj, CancellationToken cancellationToken = default)
        => _treeProvider.DeleteSubtreeAsync(parentObj, cancellationToken);

    public Task<int> DeleteSubtreeAsync(IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => _treeProvider.DeleteSubtreeAsync(parentObj, user, cancellationToken);

    public Task<int> DeleteSubtreeAsync(RedbObject parentObj, CancellationToken cancellationToken = default)
        => _treeProvider.DeleteSubtreeAsync(parentObj, cancellationToken);

    public Task<int> DeleteSubtreeAsync(RedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => _treeProvider.DeleteSubtreeAsync(parentObj, user, cancellationToken);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObjectId, maxDepth, cancellationToken);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObj, maxDepth, cancellationToken);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.GetChildrenAsync<TProps>(parentObj, cancellationToken);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.GetPathToRootAsync<TProps>(obj, cancellationToken);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.GetDescendantsAsync<TProps>(parentObj, maxDepth, cancellationToken);

    public Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, CancellationToken cancellationToken = default)
        => _treeProvider.MoveObjectAsync(obj, newParentObj, cancellationToken);

    public Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.CreateChildAsync<TProps>(obj, parentObj, cancellationToken);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObjectId, user, maxDepth, cancellationToken);

    public Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.LoadTreeAsync<TProps>(rootObj, user, maxDepth, cancellationToken);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.GetChildrenAsync<TProps>(parentObj, user, cancellationToken);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.GetPathToRootAsync<TProps>(obj, user, cancellationToken);

    public Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.GetDescendantsAsync<TProps>(parentObj, user, maxDepth, cancellationToken);

    public Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => _treeProvider.MoveObjectAsync(obj, newParentObj, user, cancellationToken);

    public Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        => _treeProvider.CreateChildAsync<TProps>(obj, parentObj, user, cancellationToken);
    
    public Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, int? maxDepth = null, CancellationToken cancellationToken = default)
        => _treeProvider.LoadPolymorphicTreeAsync(rootObj, maxDepth, cancellationToken);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, CancellationToken cancellationToken = default)
        => _treeProvider.GetPolymorphicChildrenAsync(parentObj, cancellationToken);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, CancellationToken cancellationToken = default)
        => _treeProvider.GetPolymorphicPathToRootAsync(obj, cancellationToken);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, int? maxDepth = null, CancellationToken cancellationToken = default)
        => _treeProvider.GetPolymorphicDescendantsAsync(parentObj, maxDepth, cancellationToken);
    
    public Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default)
        => _treeProvider.LoadPolymorphicTreeAsync(rootObj, user, maxDepth, cancellationToken);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, IRedbUser user, CancellationToken cancellationToken = default)
        => _treeProvider.GetPolymorphicChildrenAsync(parentObj, user, cancellationToken);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _treeProvider.GetPolymorphicPathToRootAsync(obj, user, cancellationToken);
        
    public Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, IRedbUser user, int? maxDepth = null, CancellationToken cancellationToken = default)
        => _treeProvider.GetPolymorphicDescendantsAsync(parentObj, user, maxDepth, cancellationToken);
        
    public Task InitializeTypeRegistryAsync(CancellationToken cancellationToken = default)
        => _treeProvider.InitializeTypeRegistryAsync(cancellationToken);

    // === IPermissionProvider DELEGATION ===
    
    public IQueryable<long> GetReadableObjectIds()
        => _permissionProvider.GetReadableObjectIds();
    
    public Task<bool> CanUserEditObject(IRedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserEditObject(obj, cancellationToken);
    
    public Task<bool> CanUserSelectObject(IRedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserSelectObject(obj, cancellationToken);

    public Task<bool> CanUserInsertScheme(IRedbScheme scheme, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserInsertScheme(scheme, cancellationToken);

    public Task<bool> CanUserInsertScheme(IRedbScheme scheme, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserInsertScheme(scheme, user, cancellationToken);

    public Task<bool> CanUserDeleteObject(IRedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserDeleteObject(obj, cancellationToken);
    
    public IQueryable<long> GetReadableObjectIds(IRedbUser user)
        => _permissionProvider.GetReadableObjectIds(user);
    
    public Task<bool> CanUserEditObject(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserEditObject(obj, user, cancellationToken);
    
    public Task<bool> CanUserSelectObject(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserSelectObject(obj, user, cancellationToken);
    
    public Task<bool> CanUserDeleteObject(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserDeleteObject(obj, user, cancellationToken);
    
    public Task<bool> CanUserEditObject(RedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserEditObject(obj, cancellationToken);
    
    public Task<bool> CanUserSelectObject(RedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserSelectObject(obj, cancellationToken);
    
    public Task<bool> CanUserDeleteObject(RedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserDeleteObject(obj, cancellationToken);
    
    public Task<bool> CanUserEditObject(RedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserEditObject(obj, user, cancellationToken);
    
    public Task<bool> CanUserSelectObject(RedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserSelectObject(obj, user, cancellationToken);
    
    public Task<bool> CanUserDeleteObject(RedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserDeleteObject(obj, user, cancellationToken);

    public Task<bool> CanUserInsertScheme(RedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserInsertScheme(obj, user, cancellationToken);

    public Task<IRedbPermission> CreatePermissionAsync(PermissionRequest request, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.CreatePermissionAsync(request, currentUser, cancellationToken);

    public Task<IRedbPermission> UpdatePermissionAsync(IRedbPermission permission, PermissionRequest request, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.UpdatePermissionAsync(permission, request, currentUser, cancellationToken);

    public Task<bool> DeletePermissionAsync(IRedbPermission permission, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.DeletePermissionAsync(permission, currentUser, cancellationToken);

    public Task<List<IRedbPermission>> GetPermissionsByUserAsync(IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.GetPermissionsByUserAsync(user, cancellationToken);

    public Task<List<IRedbPermission>> GetPermissionsByRoleAsync(IRedbRole role, CancellationToken cancellationToken = default)
        => _permissionProvider.GetPermissionsByRoleAsync(role, cancellationToken);

    public Task<List<IRedbPermission>> GetPermissionsByObjectAsync(IRedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.GetPermissionsByObjectAsync(obj, cancellationToken);

    public Task<IRedbPermission?> GetPermissionByIdAsync(long permissionId, CancellationToken cancellationToken = default)
        => _permissionProvider.GetPermissionByIdAsync(permissionId, cancellationToken);

    public Task<bool> CanUserEditObject(long objectId, long userId, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserEditObject(objectId, userId, cancellationToken);

    public Task<bool> CanUserSelectObject(long objectId, long userId, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserSelectObject(objectId, userId, cancellationToken);

    public Task<bool> CanUserInsertScheme(long schemeId, long userId, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserInsertScheme(schemeId, userId, cancellationToken);

    public Task<bool> CanUserDeleteObject(long objectId, long userId, CancellationToken cancellationToken = default)
        => _permissionProvider.CanUserDeleteObject(objectId, userId, cancellationToken);

    public Task<bool> GrantPermissionAsync(IRedbUser user, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.GrantPermissionAsync(user, obj, actions, currentUser, cancellationToken);

    public Task<bool> GrantPermissionAsync(IRedbRole role, IRedbObject obj, PermissionAction actions, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.GrantPermissionAsync(role, obj, actions, currentUser, cancellationToken);

    public Task<bool> RevokePermissionAsync(IRedbUser user, IRedbObject obj, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.RevokePermissionAsync(user, obj, currentUser, cancellationToken);

    public Task<bool> RevokePermissionAsync(IRedbRole role, IRedbObject obj, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.RevokePermissionAsync(role, obj, currentUser, cancellationToken);

    public Task<int> RevokeAllUserPermissionsAsync(IRedbUser user, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.RevokeAllUserPermissionsAsync(user, currentUser, cancellationToken);

    public Task<int> RevokeAllRolePermissionsAsync(IRedbRole role, IRedbUser? currentUser = null, CancellationToken cancellationToken = default)
        => _permissionProvider.RevokeAllRolePermissionsAsync(role, currentUser, cancellationToken);

    public Task<EffectivePermissionResult> GetEffectivePermissionsAsync(IRedbUser user, IRedbObject obj, CancellationToken cancellationToken = default)
        => _permissionProvider.GetEffectivePermissionsAsync(user, obj, cancellationToken);

    public Task<Dictionary<IRedbObject, EffectivePermissionResult>> GetEffectivePermissionsBatchAsync(IRedbUser user, IRedbObject[] objects, CancellationToken cancellationToken = default)
        => _permissionProvider.GetEffectivePermissionsBatchAsync(user, objects, cancellationToken);

    public Task<List<EffectivePermissionResult>> GetAllEffectivePermissionsAsync(IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.GetAllEffectivePermissionsAsync(user, cancellationToken);

    public Task<int> GetPermissionCountAsync(CancellationToken cancellationToken = default)
        => _permissionProvider.GetPermissionCountAsync(cancellationToken);

    public Task<int> GetUserPermissionCountAsync(IRedbUser user, CancellationToken cancellationToken = default)
        => _permissionProvider.GetUserPermissionCountAsync(user, cancellationToken);

    public Task<int> GetRolePermissionCountAsync(IRedbRole role, CancellationToken cancellationToken = default)
        => _permissionProvider.GetRolePermissionCountAsync(role, cancellationToken);

    // === IQueryableProvider DELEGATION ===

    public IRedbQueryable<TProps> Query<TProps>() where TProps : class, new()
        => _queryProvider.Query<TProps>();
    
    public IRedbQueryable<TProps> Query<TProps>(IRedbUser user) where TProps : class, new()
        => _queryProvider.Query<TProps>(user);

    public IRedbQueryable<TProps> TreeQuery<TProps>() where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>();
    
    public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbUser user) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(user);

    public IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectId, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObject, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjects, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectIds, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectId, user, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObject, user, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjects, user, maxDepth);

    public IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, IRedbUser user, int? maxDepth = null) where TProps : class, new()
        => _queryProvider.TreeQuery<TProps>(rootObjectIds, user, maxDepth);

    // === IValidationProvider DELEGATION ===

    public Task<List<SupportedType>> GetSupportedTypesAsync(CancellationToken cancellationToken = default)
        => _validationProvider.GetSupportedTypesAsync(cancellationToken);

    public Task<ValidationIssue?> ValidateTypeAsync(Type csharpType, string propertyName, CancellationToken cancellationToken = default)
        => _validationProvider.ValidateTypeAsync(csharpType, propertyName, cancellationToken);

    public Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(string schemeName, bool strictDeleteExtra = true, CancellationToken cancellationToken = default) where TProps : class
        => _validationProvider.ValidateSchemaAsync<TProps>(schemeName, strictDeleteExtra, cancellationToken);

    public ValidationIssue? ValidatePropertyConstraints(Type propertyType, string propertyName, bool isRequired, bool isArray)
        => _validationProvider.ValidatePropertyConstraints(propertyType, propertyName, isRequired, isArray);
    
    public Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true, CancellationToken cancellationToken = default) where TProps : class
        => _validationProvider.ValidateSchemaAsync<TProps>(scheme, strictDeleteExtra, cancellationToken);
    
    public Task<SchemaChangeReport> AnalyzeSchemaChangesAsync<TProps>(IRedbScheme scheme, CancellationToken cancellationToken = default) where TProps : class
        => _validationProvider.AnalyzeSchemaChangesAsync<TProps>(scheme, cancellationToken);

    // === SECURITY CONTEXT MANAGEMENT ===

    public void SetCurrentUser(IRedbUser user)
        => _securityContext.SetCurrentUser(user);

    public IDisposable CreateSystemContext()
        => _securityContext.CreateSystemContext();

    public long GetEffectiveUserId()
        => _securityContext.GetEffectiveUserId();

    // === CONFIGURATION MANAGEMENT ===

    public void UpdateConfiguration(Action<RedbServiceConfiguration> configure)
    {
        configure(_configuration);
    }

    public void UpdateConfiguration(Action<RedbServiceConfigurationBuilder> configureBuilder)
    {
        var builder = new RedbServiceConfigurationBuilder(_configuration);
        configureBuilder(builder);
        _configuration = builder.Build();
    }

    // === INITIALIZATION ===
    
    /// <summary>
    /// Initialize REDB system at application startup.
    /// </summary>
    public async Task InitializeAsync(params Assembly[] assemblies)
    {
        // An empty database is named as such before any start-up query can fail on it: schema
        // creation is opt-in (ensureCreated), and without this check the module probe or the
        // scheme sync below surfaced a raw driver error ("relation _structures does not exist")
        // that said nothing about how to proceed.
        if (!await TableExistsAsync("_schemes"))
            throw new Exceptions.RedbSchemaMissingException(DatabaseTypeName);

        // 0. Verify v2-pvt SQL module is deployed (only for dialects that ship one).
        await EnsurePvtModuleDeployedAsync();

        // 1. Set type resolver for serializer (for polymorphic deserialization)
        SystemTextJsonRedbSerializer.SetTypeResolver(schemeId => _schemeSync.Cache.GetClrType(schemeId));
        
        // 2. Sync all schemes with RedbSchemeAttribute
        await AutoSyncSchemesAsync(assemblies);

        // 3. Sync UserConfigurationProps scheme
        await SyncSchemeAsync<Models.Configuration.UserConfigurationProps>();

        // 4. Initialize default user configuration
        // Disabled: config ID=-100 is for cache quotas, not needed for every DB
        // var configInitializer = new Configuration.DefaultUserConfigurationInitializer(this);
        // await configInitializer.InitializeAsync();

        // 5. Initialize object factory
        RedbObjectFactory.Initialize(this);
        
        // 6. Set global provider for RedbObject
        RedbObject.SetSchemeSyncProvider(_schemeSync);

        // 7. Warmup metadata cache
        await WarmupMetadataCacheAsync();
        
        // 8. Initialize GlobalPropsCache
        InitializePropsCache();

        // 9. Initialize type registry for polymorphic operations
        await _treeProvider.InitializeTypeRegistryAsync();
    }

    // === v2-pvt MODULE GUARD ===

    /// <summary>
    /// Verifies that the v2-pvt SQL module is deployed at the exact
    /// version this dialect's bundle ships. If the deployed version is
    /// missing or differs (in either direction) and the provider ships
    /// an embedded bundle, the bundle is applied automatically — so a
    /// simple `git pull` + restart upgrades the in-database UDFs without
    /// any manual `psql` / `sqlcmd` step. Dialects that do not ship
    /// v2-pvt opt out by returning null from
    /// <see cref="ISqlDialect.Query_PvtModuleVersionFunction"/> and this
    /// check becomes a no-op.
    /// </summary>
    protected virtual async Task EnsurePvtModuleDeployedAsync()
    {
        var versionFn = SqlDialect.Query_PvtModuleVersionFunction();
        if (string.IsNullOrEmpty(versionFn))
            return;

        var required = SqlDialect.Query_PvtRequiredVersion();
        if (string.IsNullOrWhiteSpace(required))
            return;

        string? deployed = null;
        try
        {
            deployed = await _context.ExecuteScalarAsync<string>($"SELECT {versionFn}()");
        }
        catch (Exception ex) when (Data.DbErrorClassifier.IsUndefinedFunction(ex))
        {
            // Module missing entirely — fall through to deploy.
        }

        if (!string.IsNullOrWhiteSpace(deployed)
            && string.Equals(deployed, required, StringComparison.Ordinal))
        {
            return;
        }

        // The operator asked that the schema be changed by the DBA only. Say so, with the script to
        // hand over, and touch nothing.
        if (!_configuration.AutoApplyDatabaseUpgrades)
            throw new Exceptions.RedbSchemaOutdatedException(DatabaseTypeName, deployed, required, cause: null);

        var bundleSql = ReadEmbeddedPvtBundleSql();
        if (string.IsNullOrEmpty(bundleSql))
        {
            throw new InvalidOperationException(
                $"v2-pvt module version mismatch: deployed='{deployed ?? "<none>"}', required='{required}'. " +
                "The provider does not ship an embedded bundle; redeploy the SQL files manually " +
                "(redb.Postgres/sql/v2-pvt/*.sql for PostgreSQL or redb.MSSql/sql/v2-pvt/pvt_bundle.sql for SQL Server).");
        }

        _logger?.LogInformation(
            "v2-pvt module {State} (deployed='{Deployed}', required='{Required}'); applying embedded bundle...",
            string.IsNullOrEmpty(deployed) ? "missing" : "out of date",
            deployed ?? "<none>", required);

        try
        {
            await ExecuteSchemaScriptAsync(bundleSql);
        }
        catch (Exception ex) when (Data.DbErrorClassifier.IsInsufficientPrivilege(ex))
        {
            // The connected role may run the application but not change the schema: the DBA granted
            // owner rights for the initial installation and took them back. That is a supported
            // deployment, not a fault, and it deserves an instruction rather than a driver error.
            // Any other failure of the bundle propagates as is — it is a defect, not a policy.
            throw new Exceptions.RedbSchemaOutdatedException(DatabaseTypeName, deployed, required, cause: ex);
        }

        _logger?.LogInformation("v2-pvt module bundle applied.");
    }

    // === DATABASE SCHEMA MANAGEMENT ===

    /// <summary>
    /// Checks whether the specified table exists in the database.
    /// </summary>
    protected abstract Task<bool> TableExistsAsync(string tableName);

    /// <summary>
    /// Reads the embedded combined SQL initialization script (redb_init.sql).
    /// </summary>
    protected abstract string ReadEmbeddedSql();

    /// <summary>
    /// Executes the full schema initialization SQL script against the database.
    /// Provider-specific: e.g. MSSQL splits by GO batch separators.
    /// </summary>
    protected abstract Task ExecuteSchemaScriptAsync(string sql);

    /// <summary>
    /// Returns the embedded v2-pvt module bundle (pvt_bundle.sql) or
    /// <c>null</c> when the provider doesn't ship a standalone bundle.
    /// Used by <see cref="EnsureDatabaseAsync"/> to auto-upgrade databases
    /// that were created before the v2-pvt module was added to
    /// redb_init.sql (so the full init script is skipped, but the
    /// module is still missing or outdated).
    /// </summary>
    protected virtual string? ReadEmbeddedPvtBundleSql() => null;

    /// <inheritdoc />
    public virtual async Task EnsureDatabaseAsync()
    {
        // Check if the core table '_schemes' already exists
        if (await TableExistsAsync("_schemes"))
        {
            _logger?.LogInformation("REDB schema already exists, skipping creation.");
            await EnsurePvtModuleDeployedAsync();
            return;
        }

        _logger?.LogInformation("REDB schema not found. Creating database schema...");

        var sql = ReadEmbeddedSql();
        await ExecuteSchemaScriptAsync(sql);

        _logger?.LogInformation("REDB database schema created successfully.");
    }

    /// <inheritdoc />
    public string GetSchemaScript() => ReadEmbeddedSql();

    /// <inheritdoc />
    public string? GetUpgradeScript() => ReadEmbeddedPvtBundleSql();

    /// <inheritdoc />
    public async Task InitializeAsync(bool ensureCreated, params Assembly[] assemblies)
    {
        if (ensureCreated)
            await EnsureDatabaseAsync();

        await InitializeAsync(assemblies);
    }
    
    /// <summary>
    /// Warmup metadata cache. Override in derived classes for DB-specific SQL.
    /// </summary>
    protected virtual async Task WarmupMetadataCacheAsync()
    {
        if (!_configuration.WarmupMetadataCacheOnInit) return;
        
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var warmupSql = SqlDialect.Warmup_AllMetadataCaches();
            var result = await _context.QueryAsync<WarmupCacheResult>(warmupSql);
            sw.Stop();
            
            if (result.Any())
            {
                _logger?.LogInformation(
                    "Metadata cache warmed up: {SchemeCount} schemes, {StructureCount} structures in {ElapsedMs} ms",
                    result.Count, result.Sum(r => r.structures_count), sw.ElapsedMilliseconds);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to warmup metadata cache during initialization");
        }
    }
    
    /// <summary>
    /// Initialize GlobalPropsCache.
    /// </summary>
    private void InitializePropsCache()
    {
        if (!_configuration.EnablePropsCache) return;

        // The cache lives for the process: nothing scoped (a user, a configuration service) is captured into it.
        // Its logger is how a production host learns that the cache is too small or its hits are slow.
        var cache = new Caching.MemoryRedbObjectCache(
            maxSize: _configuration.PropsCacheMaxSize,
            ttl: _configuration.PropsCacheTtl,
            logger: _serviceProvider.GetService<ILoggerFactory>()?.CreateLogger(typeof(Caching.MemoryRedbObjectCache).FullName!));
        
        // A cached graph carries nothing scope-bound: its stubs load on the reader's scope (owner decision 2026-09-15).
        _schemeSync.PropsCache.Initialize(cache);
    }
    
    /// <summary>
    /// Auto-sync all schemes with RedbSchemeAttribute.
    /// </summary>
    private async Task AutoSyncSchemesAsync(params Assembly[] assemblies)
    {
        IEnumerable<Assembly> assembliesToScan = assemblies.Length > 0
            ? assemblies
            : GetAllLoadedAssemblies();

        var typesToSync = assembliesToScan
            .SelectMany(GetTypesWithRedbSchemeAttribute)
            .ToList();

        if (typesToSync.Count == 0) return;

        // Pre-flight: validate ALL explicit scheme names up front. A bad name is fatal by design
        // (a scheme with an invalid name must not boot), but failing on the first offender hides the
        // rest — the developer fixes one, reruns, hits the next. Reporting the full list in one
        // AggregateException lets them fix everything in a single pass (H2).
        var nameErrors = new List<Exception>();
        foreach (var type in typesToSync)
        {
            var explicitName = type.GetCustomAttribute<RedbSchemeAttribute>()?.Name;
            if (!string.IsNullOrWhiteSpace(explicitName) && !SchemeNameValidator.IsValid(explicitName, out var reason))
                nameErrors.Add(new RedbSchemeNameException(type, explicitName, reason!));
        }
        if (nameErrors.Count == 1)
            throw nameErrors[0];
        if (nameErrors.Count > 1)
            throw new AggregateException(
                $"{nameErrors.Count} types declare invalid explicit scheme names — fix all of them:", nameErrors);

        foreach (var type in typesToSync)
        {
            await SyncSchemeForTypeAsync(type);
        }
    }
    
    private static IEnumerable<Assembly> GetAllLoadedAssemblies()
    {
#if NET5_0_OR_GREATER
        return AssemblyLoadContext.Default.Assemblies;
#else
        return AppDomain.CurrentDomain.GetAssemblies();
#endif
    }
    
    private static IEnumerable<Type> GetTypesWithRedbSchemeAttribute(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes()
                .Where(t => t.GetCustomAttribute<RedbSchemeAttribute>() != null);
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types
                .Where(t => t != null && t.GetCustomAttribute<RedbSchemeAttribute>() != null)!;
        }
        catch
        {
            return Enumerable.Empty<Type>();
        }
    }
    
    private async Task SyncSchemeForTypeAsync(Type type)
    {
        try
        {
            var method = typeof(ISchemeSyncProvider)
                .GetMethod(nameof(ISchemeSyncProvider.SyncSchemeAsync))
                ?.MakeGenericMethod(type);

            if (method != null)
            {
                var task = method.Invoke(this, new object[] { CancellationToken.None }); // the generic overload takes a token now
                if (task is Task asyncTask)
                {
                    await asyncTask;
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to sync scheme for type '{TypeName}'", type.FullName);
            throw;
        }
    }
}

/// <summary>
/// Result from warmup_all_metadata_caches() SQL function.
/// </summary>
internal class WarmupCacheResult
{
    public long scheme_id { get; set; }
    public long structures_count { get; set; }
    public string? scheme_name { get; set; }
}

