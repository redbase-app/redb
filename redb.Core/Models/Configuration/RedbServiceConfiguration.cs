using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;
using redb.Core.Configuration;
using redb.Core.Caching;

namespace redb.Core.Models.Configuration
{
    /// <summary>
    /// RedbService behavior configuration.
    /// </summary>
    public class RedbServiceConfiguration
    {
        // === CONNECTION SETTINGS ===
        
        /// <summary>
        /// PostgreSQL connection string. Required for automatic IRedbContext registration.
        /// If not set, user must register IRedbContext manually.
        /// </summary>
        public string? ConnectionString { get; set; }
        
        /// <summary>
        /// Cache domain name for isolating scheme caches between different databases.
        /// If not set, computed from connection string hash.
        /// Use explicit name when connecting to same DB from multiple services.
        /// </summary>
        public string? CacheDomain { get; set; }
        
        // === OBJECT DELETION SETTINGS ===

        /// <summary>
        /// Strategy for handling ID after object deletion.
        /// </summary>
        [JsonConverter(typeof(ObjectIdResetStrategyJsonConverter))]
        public ObjectIdResetStrategy IdResetStrategy { get; set; } = ObjectIdResetStrategy.Manual;

        /// <summary>
        /// Strategy for handling non-existent objects on UPDATE.
        /// </summary>
        [JsonConverter(typeof(MissingObjectStrategyJsonConverter))]
        public MissingObjectStrategy MissingObjectStrategy { get; set; } = MissingObjectStrategy.AutoSwitchToInsert;

        // === DEFAULT SECURITY SETTINGS ===

        /// <summary>
        /// Check permissions by default when loading objects.
        /// </summary>
        public bool DefaultCheckPermissionsOnLoad { get; set; } = false;

        /// <summary>
        /// Check permissions by default when saving objects.
        /// </summary>
        public bool DefaultCheckPermissionsOnSave { get; set; } = false;

        /// <summary>
        /// Check permissions by default when deleting objects.
        /// </summary>
        public bool DefaultCheckPermissionsOnDelete { get; set; } = true;

        /// <summary>
        /// Check permissions by default when executing queries.
        /// </summary>
        public bool DefaultCheckPermissionsOnQuery { get; set; } = false;

        // === SCHEME SETTINGS ===

        /// <summary>
        /// Strictly delete extra fields when synchronizing schemes by default.
        /// </summary>
        public bool DefaultStrictDeleteExtra { get; set; } = true;

        /// <summary>
        /// Automatically synchronize schemes when saving objects.
        /// </summary>
        public bool AutoSyncSchemesOnSave { get; set; } = true;

        // === OBJECT LOADING SETTINGS ===

        /// <summary>
        /// Default depth for loading nested objects.
        /// </summary>
        private int _defaultLoadDepth = 10;
        public int DefaultLoadDepth 
        { 
            get => _defaultLoadDepth;
            set 
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(DefaultLoadDepth), "Minimum 1");
                if (value > 100)
                    throw new ArgumentOutOfRangeException(nameof(DefaultLoadDepth), "Maximum 100");
                _defaultLoadDepth = value;
            }
        }

        /// <summary>
        /// Maximum depth for tree structures.
        /// </summary>
        private int _defaultMaxTreeDepth = 50;
        public int DefaultMaxTreeDepth 
        { 
            get => _defaultMaxTreeDepth;
            set 
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(DefaultMaxTreeDepth), "Minimum 1");
                if (value > 1000)
                    throw new ArgumentOutOfRangeException(nameof(DefaultMaxTreeDepth), "Maximum 1000");
                _defaultMaxTreeDepth = value;
            }
        }

        /// <summary>
        /// Throw exception if object not found in LoadAsync.
        /// true (default) - throws InvalidOperationException
        /// false - returns null for single object, skips in batch load
        /// </summary>
        public bool ThrowOnObjectNotFound { get; set; } = false;

        // === PERFORMANCE SETTINGS ===

        /// <summary>
        /// Enable the PVT prefilter: a cutting step that narrows the object set BEFORE the
        /// pivot aggregate runs, so a selective filter stops costing a full scheme scan.
        /// The prefilter is a superset and never changes results; when the planner cannot
        /// analyse a filter it emits nothing and behaviour is identical to disabled.
        /// Pro only; implemented by all three Pro providers - PostgreSQL, SQL Server and SQLite. How
        /// much it saves depends on the engine and the predicate (see docs/PVT_PREFILTER_PLAN.md).
        /// Default is false (opt-in while the feature is being validated).
        /// </summary>
        public bool EnablePvtPrefilter { get; set; } = false;

        private string? _stringCollation;

        /// <summary>
        /// Unicode-aware case folding for case-insensitive text operations
        /// (<c>ContainsIgnoreCase</c>, <c>StartsWithIgnoreCase</c>, <c>EndsWithIgnoreCase</c>,
        /// <c>ToLower</c>, <c>ToUpper</c>).
        ///
        /// <para>
        /// <b>Why it exists.</b> Case folding is driven by the database's own rules, and those cover
        /// only ASCII in two of the three providers. On PostgreSQL created with <c>LC_CTYPE=C</c>,
        /// <c>'Привет' ILIKE '%привет%'</c> is false and <c>lower('Привет')</c> returns the string
        /// unchanged. On SQLite this is unconditional: <c>LIKE</c>, <c>lower()</c>, <c>upper()</c> and
        /// <c>COLLATE NOCASE</c> are ASCII-only in every database. MSSQL is unaffected because its
        /// default collation is case-insensitive for all scripts.
        /// </para>
        ///
        /// <para>
        /// <b>What it fixes.</b> Every script whose case mapping is one character to one character:
        /// Cyrillic, Greek, Hungarian, Polish, Czech, French, most of German. There is no per-language
        /// work; one setting covers all of them.
        /// </para>
        ///
        /// <para>
        /// <b>What it does NOT fix</b>, and cannot, because no collation can:
        /// foldings that change length (German <c>ß</c> against <c>SS</c>: <c>upper()</c> expands it,
        /// pattern matching does not, so the two disagree); language-dependent foldings (Turkish
        /// <c>İ</c> does not fold to <c>i</c>, and Turkish wants <c>I</c>→<c>ı</c> where every other
        /// locale wants <c>I</c>→<c>i</c>); and insensitivity to diacritics (<c>Müller</c> against
        /// <c>muller</c>), which is a different feature and is rejected outright by PostgreSQL for
        /// pattern matching. See COLLATION.md.
        /// </para>
        ///
        /// <para>
        /// <b>Cost on PostgreSQL.</b> A collated operand cannot use an index built with the database's
        /// own collation, so a trigram index on the text column stops being usable and the search
        /// degrades to a full scan, silently. Add a matching expression index yourself:
        /// <c>CREATE INDEX ... USING gin ((_String COLLATE "und-x-icu") gin_trgm_ops)</c>.
        /// </para>
        ///
        /// Default is null: behaviour is exactly what it was, on every provider.
        /// </summary>
        public string? StringCollation
        {
            get => _stringCollation;
            set
            {
                var normalised = string.IsNullOrWhiteSpace(value) ? null : value.Trim();
                if (normalised != null)
                    Core.Query.CollationNameValidator.Validate(normalised);
                _stringCollation = normalised;
            }
        }

        /// <summary>
        /// V4 (LAZY Л2): lazy references. With the option on, the JSON builders emit a STUB for
        /// every reference whose structure carries the `virtual` marker (`_structures._lazy`),
        /// regardless of depth; first access to the stub's Props loads exactly that object.
        /// The option travels to the builders as a session flag of the context's connection
        /// (PostgreSQL GUC `redb.lazy_refs`, MSSQL SESSION_CONTEXT, a connection flag of the
        /// SQLite native extension) - no builder signature changes (plan decision 5).
        /// Per-query override: `WithLazyReferences(bool)`. Default is false: behaviour is
        /// exactly what it was.
        /// </summary>
        public bool EnableLazyReferences { get; set; } = false;

        /// <summary>
        /// V4 (review): what the <c>Props</c> getter of an UNLOADED reference does. <c>Blocking</c>
        /// (default) loads the object synchronously - a hidden database round trip that waits for
        /// the query on the calling thread (the load itself runs on the thread pool, so a host with a
        /// SynchronizationContext does not deadlock). <c>Throw</c> refuses with
        /// <see cref="Exceptions.RedbSynchronousLazyLoadException"/> and names the explicit way
        /// (<c>LoadPropsAsync</c>, <c>LoadReferencesAsync</c>, a larger depth): for Blazor WebAssembly,
        /// which cannot block at all, and for UI hosts where a synchronous query on property access
        /// is a defect. The async APIs work in both modes.
        /// </summary>
        public LazyReferenceAccessMode LazyReferenceAccess { get; set; } = LazyReferenceAccessMode.Blocking;

        /// <summary>
        /// What a lazy load - the Props of a reference stub, <c>RedbListItem.Object</c> - does when no live redb scope reads
        /// it. A data object owns no connection: the load runs on the scope of whoever reads it (owner decision 2026-09-15).
        /// <c>Refuse</c> (default) throws <see cref="Exceptions.RedbLazyLoadScopeEndedException"/>; <c>FreshScope</c> opens a
        /// scope and a pooled connection per such load.
        /// </summary>
        public LazyLoadWithoutScopeMode LazyLoadWithoutScope { get; set; } = LazyLoadWithoutScopeMode.Refuse;

        /// <summary>
        /// Apply the versioned SQL module (functions and, from V4, schema upgrades) automatically at
        /// start-up when the database reports a different module version than this build requires.
        ///
        /// <para>
        /// <c>true</c> (default) keeps the long-standing behaviour: pull, restart, the database
        /// follows. When the connected role is not allowed to — the DBA revoked owner rights after the
        /// initial installation — start-up stops with <see cref="Exceptions.RedbSchemaOutdatedException"/>
        /// naming the script for the DBA, rather than a raw driver error.
        /// </para>
        ///
        /// <para>
        /// <c>false</c> is for installations where the policy is "only the DBA changes the schema":
        /// start-up then checks the version and stops with the same exception without trying.
        /// SQLite ignores this setting — the database file belongs to the process and there is no
        /// DBA to defer to.
        /// </para>
        /// </summary>
        public bool AutoApplyDatabaseUpgrades { get; set; } = true;

        /// <summary>
        /// Enable transparent whole-object (Props) caching, validated by object hash.
        /// Default is false.
        /// </summary>
        public bool EnablePropsCache { get; set; } = false;

        /// <summary>
        /// When list items are handed out (list reads, item lookups), load their linked objects
        /// (<see cref="Models.Entities.RedbListItem.Object"/>) in ONE batch up front, so touching
        /// the property later is a field read: no database call, no blocked thread. Lists are
        /// dictionaries by design (dozens of rows), so the batch is one cheap SELECT; disable only
        /// when a list is unusually large AND its objects are rarely needed. Default: enabled -
        /// the lazy sync getter blocks a thread per touch, and a hot loop over fresh items froze
        /// a production process (2026-09-09).
        /// </summary>
        public bool PreloadListItemLinkedObjects { get; set; } = true;

        /// <summary>
        /// Sample bound for SQLite's ANALYZE in <c>IMaintenanceProvider.AnalyzeAsync</c>
        /// (PRAGMA analysis_limit; 0 = unbounded full scan). PostgreSQL and MSSQL ignore it.
        /// The bounded form turns a minutes-long full-file ANALYZE into seconds while producing
        /// estimates just as good for redb's query shapes. Default: 1000 (SQLite's own
        /// recommended ballpark).
        /// </summary>
        public int MaintenanceAnalysisLimit { get; set; } = 1000;

        /// <summary>
        /// Maximum number of objects in Props cache.
        /// On overflow the least recently used tenth is evicted at once; a working set above the limit
        /// is logged as a warning
        /// </summary>
        private int _propsCacheMaxSize = 10000;
        public int PropsCacheMaxSize 
        { 
            get => _propsCacheMaxSize;
            set 
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheMaxSize), "Must be greater than 0");
                if (value > 10_000_000)
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheMaxSize), "Maximum 10,000,000");
                _propsCacheMaxSize = value;
            }
        }

        /// <summary>
        /// Lifetime of Props cache entry.
        /// After expiration - entry considered stale
        /// </summary>
        private TimeSpan _propsCacheTtl = TimeSpan.FromMinutes(60);
        public TimeSpan PropsCacheTtl 
        { 
            get => _propsCacheTtl;
            set 
            {
                if (value <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheTtl), "TTL must be greater than 0");
                if (value > TimeSpan.FromDays(7))
                    throw new ArgumentOutOfRangeException(nameof(PropsCacheTtl), "Maximum 7 days");
                _propsCacheTtl = value;
            }
        }

        /// <summary>
        /// Skip hash validation in DB before cache access.
        /// true - for monolithic apps (faster, trust cache)
        /// false - for distributed systems (safer, check freshness)
        /// Default false (safe)
        /// </summary>
        /// <remarks>
        /// The zero-DB shortcut applies OUTSIDE transactions only: inside an active transaction
        /// (explicit or ambient) a load always consults the database hash, so the canonical
        /// ExecuteAtomicAsync + LockForUpdateAsync + re-read pattern stays correct even with this
        /// flag on (bug report п.1, 2026-09-02). The flag remains a single-writer trade-off: outside
        /// transactions, changes committed by ANOTHER process stay invisible until the TTL expires.
        /// </remarks>
        public bool SkipHashValidationOnCacheCheck { get; set; } = false;

        /// <summary>
        /// Controls what <c>LoadAsync&lt;TProps&gt;</c> does when the loaded object belongs to a different
        /// scheme than <c>TProps</c> maps to. The check itself always runs (garbage never reaches the
        /// cache); this flag only chooses the reaction:
        /// <list type="bullet">
        /// <item><c>false</c> (default) — return <c>null</c>. A soft-deleted object (scheme <c>-10</c>)
        /// therefore reads as <c>null</c>, which is what callers that soft-delete expect.</item>
        /// <item><c>true</c> — throw <c>RedbSchemeMismatchException</c>, to surface a genuine type mistake
        /// loudly.</item>
        /// </list>
        /// The untyped <c>LoadAsync(objectId)</c> is never affected.
        /// </summary>
        public bool ThrowOnSchemeMismatch { get; set; } = false;

        // === LIST CACHE SETTINGS ===

        /// <summary>
        /// Enable caching of lists and their items.
        /// </summary>
        public bool EnableListCache { get; set; } = true;

        /// <summary>
        /// Lifetime of list cache entry (TTL).
        /// Eventual consistency: other clients see changes after TTL
        /// </summary>
        private TimeSpan _listCacheTtl = TimeSpan.FromMinutes(5);
        public TimeSpan ListCacheTtl 
        { 
            get => _listCacheTtl;
            set 
            {
                if (value <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(ListCacheTtl), "TTL must be greater than 0");
                if (value > TimeSpan.FromDays(7))
                    throw new ArgumentOutOfRangeException(nameof(ListCacheTtl), "Maximum 7 days");
                _listCacheTtl = value;
            }
        }

        /// <summary>
        /// Enable scheme metadata caching (OBSOLETE - use MetadataCache).
        /// </summary>
        //[Obsolete("Use MetadataCache.EnableMetadataCache instead")]
        public bool EnableMetadataCache { get; set; } = true;

        /// <summary>
        /// Metadata cache lifetime in minutes (OBSOLETE - use MetadataCache).
        /// </summary>
        //[Obsolete("Use MetadataCache.Schemes.LifetimeMinutes instead")]
        private int _metadataCacheLifetimeMinutes = 30;
        public int MetadataCacheLifetimeMinutes 
        { 
            get => _metadataCacheLifetimeMinutes;
            set 
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(MetadataCacheLifetimeMinutes), "Minimum 1 minute");
                if (value > 10080)
                    throw new ArgumentOutOfRangeException(nameof(MetadataCacheLifetimeMinutes), "Maximum 10080 minutes (7 days)");
                _metadataCacheLifetimeMinutes = value;
            }
        }

        /// <summary>
        /// Warm up scheme metadata cache during RedbService initialization.
        /// Calls warmup_all_metadata_caches() in InitializeAsync()
        /// Recommended for production (predictable performance)
        /// Default: true
        /// </summary>
        public bool WarmupMetadataCacheOnInit { get; set; } = true;

        // === STARTUP SETTINGS ===

        /// <summary>
        /// Automatically create database schema on startup via IHostedService.
        /// When true, RedbInitHostedService calls InitializeAsync(ensureCreated: true)
        /// before other hosted services that depend on the schema.
        /// Default: false (explicit opt-in).
        /// </summary>
        public bool EnsureCreated { get; set; } = false;

        // === METADATA CACHING SETTINGS (NEW) ===

        /// <summary>
        /// Extended metadata caching configuration.
        /// </summary>
        //public MetadataCacheConfiguration MetadataCache { get; set; } = new();

        // === VALIDATION SETTINGS ===

        /// <summary>
        /// Enable scheme validation before synchronization.
        /// </summary>
        public bool EnableSchemaValidation { get; set; } = true;

        /// <summary>
        /// Enable data validation when saving.
        /// </summary>
        public bool EnableDataValidation { get; set; } = true;

        // === AUDIT SETTINGS ===

        /// <summary>
        /// Automatically set modification date when saving.
        /// </summary>
        public bool AutoSetModifyDate { get; set; } = true;

        /// <summary>
        /// Automatically recompute hash when saving.
        /// </summary>
        public bool AutoRecomputeHash { get; set; } = true;

        // === SECURITY CONTEXT SETTINGS ===

        // Security context priority removed - simple GetEffectiveUser() logic used

        /// <summary>
        /// System user ID for operations without permission checks.
        /// </summary>
        public long SystemUserId { get; set; } = 0;

        // === EAV SAVE SETTINGS ===

        /// <summary>
        /// EAV properties save strategy.
        /// </summary>
        [JsonConverter(typeof(PropsSaveStrategyJsonConverter))]
        public PropsSaveStrategy PropsSaveStrategy { get; set; } = PropsSaveStrategy.DeleteInsert;





        // === SERIALIZATION SETTINGS ===

        /// <summary>
        /// JSON serialization settings for arrays.
        /// </summary>
        public JsonSerializationOptions JsonOptions { get; set; } = new JsonSerializationOptions();

        // === METHODS ===

        /// <summary>
        /// Copy every setting of <paramref name="source"/> into this instance, the connection identity
        /// (<see cref="ConnectionString"/>, <see cref="CacheDomain"/>) included.
        /// </summary>
        public void CopyFrom(RedbServiceConfiguration source)
        {
            ArgumentNullException.ThrowIfNull(source);
            ConnectionString = source.ConnectionString;
            CacheDomain = source.CacheDomain;
            CopyBehaviourFrom(source);
        }

        /// <summary>
        /// Copy every setting of <paramref name="source"/> into this instance except the connection identity
        /// (<see cref="ConnectionString"/>, <see cref="CacheDomain"/>): what a temporary scope may change on a
        /// running service. This is the one list of the behaviour settings - Clone, CopyFrom and ApplyTemporary
        /// all go through it; a new setting is added here (RedbServiceConfigurationCopyTests fails otherwise).
        /// </summary>
        public void CopyBehaviourFrom(RedbServiceConfiguration source)
        {
            ArgumentNullException.ThrowIfNull(source);
            IdResetStrategy = source.IdResetStrategy;
            MissingObjectStrategy = source.MissingObjectStrategy;
            DefaultCheckPermissionsOnLoad = source.DefaultCheckPermissionsOnLoad;
            DefaultCheckPermissionsOnSave = source.DefaultCheckPermissionsOnSave;
            DefaultCheckPermissionsOnDelete = source.DefaultCheckPermissionsOnDelete;
            DefaultCheckPermissionsOnQuery = source.DefaultCheckPermissionsOnQuery;
            DefaultStrictDeleteExtra = source.DefaultStrictDeleteExtra;
            AutoSyncSchemesOnSave = source.AutoSyncSchemesOnSave;
            DefaultLoadDepth = source.DefaultLoadDepth;
            DefaultMaxTreeDepth = source.DefaultMaxTreeDepth;
            ThrowOnObjectNotFound = source.ThrowOnObjectNotFound;
            EnablePvtPrefilter = source.EnablePvtPrefilter;
            StringCollation = source.StringCollation;
            EnableLazyReferences = source.EnableLazyReferences;
            LazyReferenceAccess = source.LazyReferenceAccess;
            LazyLoadWithoutScope = source.LazyLoadWithoutScope;
            AutoApplyDatabaseUpgrades = source.AutoApplyDatabaseUpgrades;
            EnablePropsCache = source.EnablePropsCache;
            PreloadListItemLinkedObjects = source.PreloadListItemLinkedObjects;
            MaintenanceAnalysisLimit = source.MaintenanceAnalysisLimit;
            PropsCacheMaxSize = source.PropsCacheMaxSize;
            PropsCacheTtl = source.PropsCacheTtl;
            SkipHashValidationOnCacheCheck = source.SkipHashValidationOnCacheCheck;
            ThrowOnSchemeMismatch = source.ThrowOnSchemeMismatch;
            EnableListCache = source.EnableListCache;
            ListCacheTtl = source.ListCacheTtl;
            EnableMetadataCache = source.EnableMetadataCache;
            MetadataCacheLifetimeMinutes = source.MetadataCacheLifetimeMinutes;
            WarmupMetadataCacheOnInit = source.WarmupMetadataCacheOnInit;
            EnsureCreated = source.EnsureCreated;
            EnableSchemaValidation = source.EnableSchemaValidation;
            EnableDataValidation = source.EnableDataValidation;
            AutoSetModifyDate = source.AutoSetModifyDate;
            AutoRecomputeHash = source.AutoRecomputeHash;
            SystemUserId = source.SystemUserId;
            PropsSaveStrategy = source.PropsSaveStrategy;
            JsonOptions = new JsonSerializationOptions
            {
                WriteIndented = source.JsonOptions.WriteIndented,
                UseUnsafeRelaxedJsonEscaping = source.JsonOptions.UseUnsafeRelaxedJsonEscaping
            };
        }

        /// <summary>
        /// Create configuration copy.
        /// </summary>
        public RedbServiceConfiguration Clone()
        {
            // CFG-1: one list of settings (CopyFrom); a hand-written copy here had fallen five settings behind.
            var copy = new RedbServiceConfiguration();
            copy.CopyFrom(this);
            return copy;
        }

        /// <summary>
        /// Get configuration description.
        /// </summary>
        public string GetDescription()
        {
            return $"RedbService Configuration: " +
                   $"LoadDepth={DefaultLoadDepth}, " +
                   $"TreeDepth={DefaultMaxTreeDepth}, " +
                   $"Cache={EnableMetadataCache}, " +
                   // $"Security={DefaultSecurityPriority}, " +
                   $"IdReset={IdResetStrategy}, " +
                   $"MissingObj={MissingObjectStrategy}";
        }

        /// <summary>
        /// Check if configuration is safe for production
        /// </summary>
        public bool IsProductionSafe()
        {
            return DefaultCheckPermissionsOnLoad &&
                   DefaultCheckPermissionsOnSave &&
                   DefaultCheckPermissionsOnDelete &&
                   EnableSchemaValidation &&
                   EnableDataValidation &&
                   DefaultLoadDepth <= 10 &&
                   !JsonOptions.WriteIndented;
        }

        /// <summary>
        /// Check if configuration is optimized for performance
        /// </summary>
        public bool IsPerformanceOptimized()
        {
            return !DefaultCheckPermissionsOnLoad &&
                   !DefaultCheckPermissionsOnSave &&
                   !DefaultCheckPermissionsOnDelete &&
                   (EnableMetadataCache/* || MetadataCache.EnableMetadataCache*/) && // Support for new and old settings
                   DefaultLoadDepth <= 5 &&
                   DefaultMaxTreeDepth <= 10 &&
                   !JsonOptions.WriteIndented;
                   //&& MetadataCache.Warmup.EnableWarmup; // Additional performance check
        }
        
        /// <summary>
        /// Gets effective cache domain name.
        /// Returns explicit CacheDomain if set, otherwise computes from connection string.
        /// </summary>
        public string GetEffectiveCacheDomain()
        {
            if (!string.IsNullOrEmpty(CacheDomain))
                return CacheDomain;
                
            if (string.IsNullOrEmpty(ConnectionString))
                return "default";
                
            // Compute short hash from connection string (sanitized - without password)
            var sanitized = SanitizeConnectionString(ConnectionString);
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sanitized));
            return Convert.ToHexString(hash)[..16].ToLowerInvariant();
        }
        
        /// <summary>
        /// Computes cache domain from a connection string (SHA256 hash, password-stripped).
        /// Used by contexts and key generators that don't have access to full configuration.
        /// </summary>
        public static string ComputeCacheDomain(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return "default";
            var sanitized = SanitizeConnectionString(connectionString);
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sanitized));
            return Convert.ToHexString(hash)[..16].ToLowerInvariant();
        }

        /// <summary>
        /// Removes sensitive info (password) from connection string for hashing.
        /// </summary>
        private static string SanitizeConnectionString(string connectionString)
        {
            // Remove password= or pwd= from connection string
            var result = System.Text.RegularExpressions.Regex.Replace(
                connectionString, 
                @"(password|pwd)\s*=\s*[^;]*;?", 
                "", 
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return result.ToLowerInvariant().Trim();
        }
    }

    /// <summary>
    /// Object ID handling strategy after deletion
    /// </summary>
    public enum ObjectIdResetStrategy
    {
        /// <summary>
        /// Manual ID reset (current behavior)
        /// </summary>
        Manual,

        /// <summary>
        /// Automatic ID reset when deleting via DeleteAsync(RedbObject)
        /// </summary>
        AutoResetOnDelete,

        /// <summary>
        /// Automatic creation of new object when trying to save deleted object
        /// </summary>
        AutoCreateNewOnSave
    }

    /// <summary>
    /// Strategy for handling non-existent objects on UPDATE
    /// </summary>
    public enum MissingObjectStrategy
    {
        /// <summary>
        /// Throw exception (current behavior)
        /// </summary>
        ThrowException,

        /// <summary>
        /// Automatically switch to INSERT
        /// </summary>
        AutoSwitchToInsert,

        /// <summary>
        /// Return null/false without error
        /// </summary>
        ReturnNull
    }

    /// <summary>
    /// EAV properties save strategy
    /// </summary>
    public enum PropsSaveStrategy
    {
        /// <summary>
        /// Simple strategy - always DELETE + INSERT all properties
        /// Reliable, but inefficient for large objects
        /// </summary>
        DeleteInsert,
        
        /// <summary>
        /// Efficient strategy - compare with DB and update only changed properties
        /// Recommended by default
        /// </summary>
        ChangeTracking
    }

    /// <summary>
    /// JSON serialization settings
    /// </summary>
    public class JsonSerializationOptions
    {
        /// <summary>
        /// Format JSON with indentation
        /// </summary>
        public bool WriteIndented { get; set; } = false;

        /// <summary>
        /// Use unsafe relaxed JavaScript encoding
        /// </summary>
        public bool UseUnsafeRelaxedJsonEscaping { get; set; } = true;
    }
}
