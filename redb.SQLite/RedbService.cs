using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Utils;
using redb.Core.Security;
using redb.Core.Models.Contracts;
using redb.Core.Models.Configuration;
using redb.SQLite.Data;
using redb.SQLite.Providers;
using redb.SQLite.Sql;

namespace redb.SQLite;

/// <summary>
/// SQLite implementation of IRedbService.
/// Inherits all common logic from RedbServiceBase.
/// Only provides SQLite-specific provider factories.
/// </summary>
public class RedbService : RedbServiceBase
{
    private static readonly SqliteDialect _dialect = new();
    
    /// <summary>
    /// Creates a new SQLite RedbService instance.
    /// </summary>
    public RedbService(IServiceProvider serviceProvider) : base(serviceProvider)
    {
    }

    // === POSTGRESQL-SPECIFIC IMPLEMENTATIONS ===
    
    protected override string DatabaseTypeName => "SQLite";
    
    protected override ISqlDialect SqlDialect => _dialect;
    
    protected override string GetVersionSql => "SELECT version()";
    
    protected override string GetDatabaseSizeSql => "SELECT pg_database_size(current_database())";
    
    protected override string ContextNotRegisteredError => 
        "IRedbContext is not registered in DI container. Add SqliteRedbContext to configuration.";
    
    protected override string GetObjectJsonSql() => "SELECT get_object_json($1, $2)::text";
    
    // === PROVIDER FACTORIES ===
    
    protected override ISchemeSyncProvider CreateSchemeSyncProvider(
        IRedbContext context, RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new SqliteSchemeSyncProvider(context, config, cacheDomain, logger);
    
    protected override IPermissionProvider CreatePermissionProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new SqlitePermissionProvider(context, securityContext, logger);
    
    protected override IUserProvider CreateUserProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new SqliteUserProvider(context, securityContext, ResolvePasswordHasher(), logger);
    
    protected override IRoleProvider CreateRoleProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new SqliteRoleProvider(context, securityContext, logger);
    
    protected override IListProvider CreateListProvider(
        IRedbContext context, RedbServiceConfiguration config, ISchemeSyncProvider schemeSync, ILogger? logger)
        => new SqliteListProvider(context, config, schemeSync, logger);
    
    protected override IObjectStorageProvider CreateObjectStorageProvider(
        IRedbContext context, IRedbObjectSerializer serializer, IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext, ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration config, IListProvider listProvider, ILogger? logger,
        IEnumerable<redb.Core.Interception.IRedbSaveInterceptor>? saveInterceptors)
        => new SqliteObjectStorageProvider(context, serializer, permissionProvider, 
            securityContext, schemeSync, config, listProvider, logger, saveInterceptors);
    
    protected override ITreeProvider CreateTreeProvider(
        IRedbContext context, IObjectStorageProvider objectStorage, IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer, IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync, RedbServiceConfiguration config, ILogger? logger)
        => new SqliteTreeProvider(context, objectStorage, permissionProvider, 
            serializer, securityContext, schemeSync, config, logger);
    
    protected override ILazyPropsLoader CreateLazyPropsLoader(
        IRedbContext context, ISchemeSyncProvider schemeSync, IRedbObjectSerializer serializer,
        RedbServiceConfiguration config, string cacheDomain, IListProvider listProvider, ILogger? logger)
        => new LazyPropsLoader(context, schemeSync, serializer, config, listProvider, logger);
    
    protected override IQueryableProvider CreateQueryableProvider(
        IRedbContext context, IRedbObjectSerializer serializer, ISchemeSyncProvider schemeSync,
        IRedbSecurityContext securityContext, ILazyPropsLoader lazyPropsLoader,
        RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new SqliteQueryableProvider(context, serializer, schemeSync, securityContext, 
            lazyPropsLoader, config, cacheDomain, logger);
    
    protected override IValidationProvider CreateValidationProvider(
        IRedbContext context, ILogger? logger)
        => new SqliteValidationProvider(context, logger);

    protected override Core.Providers.IMaintenanceProvider CreateMaintenanceProvider(
        IRedbContext context, int analysisLimit, ILogger? logger)
        => new Providers.SqliteMaintenanceProvider(context, analysisLimit, logger);

    // === DATABASE SCHEMA MANAGEMENT ===

    /// <inheritdoc />
    protected override async Task<bool> TableExistsAsync(string tableName)
    {
        var sql = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @p0)";
        return await Context.ExecuteScalarAsync<bool>(sql, tableName);
    }

    /// <inheritdoc />
    protected override string ReadEmbeddedSql()
    {
        var assembly = typeof(RedbService).Assembly;
        var resourceName = "redb.SQLite.sql.redbSqlite.sql";

        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{resourceName}' not found. Ensure the project was built correctly.");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <inheritdoc />
    protected override string? ReadEmbeddedPvtBundleSql()
    {
        // SQLite has no server-side v2-pvt module (no stored functions). The Pro
        // tier generates query SQL in C# (ProSqlBuilder); the Free tier will host
        // equivalents in the native extension. Either way, nothing to deploy here.
        return null;
    }

    /// <inheritdoc />
    protected override async Task ExecuteSchemaScriptAsync(string sql)
    {
        await Context.ExecuteAsync(sql);
    }

    /// <summary>
    /// The schema version this build writes into <c>PRAGMA user_version</c> — the SQLite counterpart of
    /// <c>pvt_module_version()</c>: the one number that says which upgrades a database file has had.
    /// Bump it when <see cref="ApplySchemaUpgradesAsync"/> gains a step; a database stamped with an
    /// older number gets every step (they are idempotent), a current one skips the pass entirely.
    /// </summary>
    /// <remarks>
    /// 1 — V4: hashes as BLOB(16) (<see cref="SqliteHash"/>).
    /// 2 — V4 (UNIQUE Э2): _structures._unique, _structures._unique_version, _values._unique BLOB(16),
    ///     the partial unique index, and the same two columns on _scheme_metadata_cache.
    /// 3 — V4 (UNIQUE Э1): _objects._value_unique and its partial unique index.
    /// 4 — V4 (LAZY Л2): _structures._lazy and _scheme_metadata_cache._lazy (the virtual marker).
    /// 5 — V4 (LAZY Л2): resync _scheme_metadata_cache._lazy from _structures (repairs caches
    ///     synced between the column upgrade and the marker write).
    /// 6 — V4 (LAZY Л2): stamp bump only. The SQLite metadata-cache WARMUP (`Warmup_AllMetadataCaches`)
    ///     carried a stale column list and re-nulled the marker after step 5 on files stamped 5;
    ///     the list is fixed and the resync of step 5 runs again for them.
    /// 7 — FK-column indexes IX__values__ListItem_not_null / IX__values__Object_not_null (partial):
    ///     without them every DELETE of a referenced object or list item scans the whole _values
    ///     for the FK check (perf finding, 2026-09-10).
    /// </remarks>
    private const int SchemaVersion = 9;

    /// <summary>
    /// Schema upgrades for a database created by an older build — the SQLite counterpart of the
    /// "0. Schema upgrades" step of the PostgreSQL/MSSQL module bundle. Every statement is idempotent,
    /// and the pass as a whole is gated by <c>PRAGMA user_version</c>: without the gate the
    /// <c>typeof()</c> checks below would scan <c>_objects</c> on every start. Runs before
    /// <see cref="ApplySeedCorrectionsAsync"/>.
    /// </summary>
    private async Task ApplySchemaUpgradesAsync()
    {
        var deployed = await Context.ExecuteScalarAsync<long>("PRAGMA user_version");
        if (deployed >= SchemaVersion)
            return;

        // 1 — V4: hashes are BLOB(16) in RFC 4122 byte order (SqliteHash). Databases created before V4
        // hold them as 36-character TEXT. SQLite keeps the storage class a value was written with,
        // whatever the column declares, so a declared-TEXT column takes the BLOB and no ALTER is
        // needed: convert the values in place; typeof() tells the two apart.
        foreach (var (table, column) in new[] { ("_objects", "_hash"), ("_schemes", "_structure_hash"), ("_users", "_hash") })
        {
            await Context.ExecuteAsync(
                $"UPDATE {table} SET {column} = {SqliteHash.FromText(column)} WHERE typeof({column}) = 'text'");
        }

        // 2 - V4 (UNIQUE stage 2): [RedbUnique] key metadata and the key hash column - the SQLite
        // delivery of the module block "0. Schema upgrades" (PG/MSSQL 00_module_init.sql). SQLite
        // ALTER TABLE ADD COLUMN has no IF NOT EXISTS, so each is guarded by pragma_table_info.
        foreach (var (table, column, type) in new[]
                 {
                     ("_structures", "_unique", "INTEGER"),
                     ("_structures", "_unique_version", "INTEGER"),
                     ("_values", "_unique", "BLOB"),
                     ("_scheme_metadata_cache", "_unique", "INTEGER"),
                     ("_scheme_metadata_cache", "_unique_version", "INTEGER"),
                 })
        {
            if (!await ColumnExistsAsync(table, column))
                await Context.ExecuteAsync($"ALTER TABLE {table} ADD COLUMN {column} {type} NULL");
        }

        await Context.ExecuteAsync(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"UIX__values__structure_unique\" " +
            "ON _values (_id_structure, _unique) " +
            "WHERE _unique IS NOT NULL");

        // 3 - V4 (UNIQUE stage 1): the object key column and its unique index (length 440 is
        // enforced in C# - SQLite does not check VARCHAR lengths).
        if (!await ColumnExistsAsync("_objects", "_value_unique"))
            await Context.ExecuteAsync("ALTER TABLE _objects ADD COLUMN _value_unique TEXT NULL");
        await Context.ExecuteAsync(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"UIX__objects__scheme_unique\" " +
            "ON _objects (_id_scheme, _value_unique) WHERE _value_unique IS NOT NULL");

        // 4 - V4 (LAZY L2): the lazy-reference marker, read from `virtual` at synchronisation.
        foreach (var (table, column) in new[] { ("_structures", "_lazy"), ("_scheme_metadata_cache", "_lazy") })
        {
            if (!await ColumnExistsAsync(table, column))
                await Context.ExecuteAsync($"ALTER TABLE {table} ADD COLUMN {column} INTEGER NULL");
        }

        // 5 - V4 (LAZY L2): repair caches that were synced before the marker landed.
        await Context.ExecuteAsync(
            "UPDATE _scheme_metadata_cache SET _lazy = (SELECT s._lazy FROM _structures s WHERE s._id = _structure_id)");

        // 7 - FK-column indexes of _values (perf, 2026-09-10): _Object and _ListItem carry
        // foreign keys, and without a leading index every DELETE of a referenced object or
        // list item scans the whole table for the FK check (measured: seconds per check on
        // ~8.4M rows, milliseconds with the index). Partial form keeps them nearly empty -
        // reference fields are a small fraction of rows - so writes barely pay.
        await Context.ExecuteAsync(
            "CREATE INDEX IF NOT EXISTS \"IX__values__ListItem_not_null\" " +
            "ON _values (_ListItem) WHERE _ListItem IS NOT NULL");
        await Context.ExecuteAsync(
            "CREATE INDEX IF NOT EXISTS \"IX__values__Object_not_null\" " +
            "ON _values (_Object) WHERE _Object IS NOT NULL");

        // 8 - S2 (subtree-unique plan): the key index covers keyed rows at ANY position now -
        // nested scalar keys carry _array_parent_id, and the old positional filter kept them
        // OUT of the index, so their "uniqueness" silently never fired. Recreate on the old
        // predicate; fresh databases get the final form from redbSqlite.sql.
        var oldKeyIndexForm = await Context.ExecuteScalarAsync<long?>(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' " +
            "AND name = 'UIX__values__structure_unique' AND sql LIKE '%_array_parent_id%'");
        if (oldKeyIndexForm.HasValue)
        {
            await Context.ExecuteAsync("DROP INDEX \"UIX__values__structure_unique\"");
            await Context.ExecuteAsync(
                "CREATE UNIQUE INDEX \"UIX__values__structure_unique\" " +
                "ON _values (_id_structure, _unique) WHERE _unique IS NOT NULL");
        }

        // 9 - S3 (subtree-unique plan): element-key scope of collection keys, and the
        // free-form _tags marker on schemes, structures and the metadata cache.
        foreach (var (table, column, type) in new[]
        {
            ("_structures", "_unique_scope", "INTEGER"),
            ("_structures", "_tags", "TEXT"),
            ("_schemes", "_tags", "TEXT"),
            ("_scheme_metadata_cache", "_unique_scope", "INTEGER"),
            ("_scheme_metadata_cache", "_tags", "TEXT"),
        })
        {
            if (!await ColumnExistsAsync(table, column))
                await Context.ExecuteAsync($"ALTER TABLE {table} ADD COLUMN {column} {type} NULL");
        }
        await Context.ExecuteAsync(
            "CREATE INDEX IF NOT EXISTS \"IX__structures__tags\" " +
            "ON _structures (_tags) WHERE _tags IS NOT NULL");
        await Context.ExecuteAsync(
            "UPDATE _scheme_metadata_cache SET " +
            "_unique_scope = (SELECT s._unique_scope FROM _structures s WHERE s._id = _structure_id), " +
            "_tags = (SELECT s._tags FROM _structures s WHERE s._id = _structure_id)");

        await StampSchemaVersionAsync();
    }

    private Task StampSchemaVersionAsync() => Context.ExecuteAsync($"PRAGMA user_version = {SchemaVersion}");

    private async Task<bool> ColumnExistsAsync(string table, string column)
        => (await Context.ExecuteScalarAsync<long?>(
            $"SELECT 1 FROM pragma_table_info('{table}') WHERE name = '{column}' LIMIT 1")).HasValue;

    /// <summary>
    /// Applies corrections to seeded metadata on a database that already exists.
    ///
    /// <para>
    /// PostgreSQL and MSSQL get this for free: a corrected seed rides
    /// <c>sql/v2-pvt/*.sql</c>, and <c>EnsurePvtModuleDeployedAsync</c> reapplies the bundle whenever
    /// <c>pvt_module_version()</c> disagrees with the dialect. SQLite has no such channel — it has no
    /// stored functions, so it has no module and no version to compare, and
    /// <see cref="ReadEmbeddedPvtBundleSql"/> returns null on purpose. <c>redbSqlite.sql</c> is applied
    /// only when the tables are absent. Without the step below, a fix to seeded data would reach new
    /// databases and no existing one, however many times the package is upgraded.
    /// </para>
    ///
    /// <para>
    /// Every statement here must be idempotent: this runs on every start, not once.
    /// </para>
    /// </summary>
    private async Task ApplySeedCorrectionsAsync()
    {
        // DateOnly was seeded with _db_type = 'DateTime', a value no JSON projection branches on, so
        // every DateOnly property materialised as 0001-01-01. Retyping it to 'DateTimeOffset' routes it
        // through the branch that already exists; the stored value is untouched, it has always been
        // midnight of that date in _values._DateTimeOffset.
        var fixedRows = await Context.ExecuteAsync(
            "UPDATE _types SET _db_type = 'DateTimeOffset' " +
            "WHERE _id = " + RedbTypeIds.DateOnly + " AND _db_type <> 'DateTimeOffset'");

        if (fixedRows <= 0)
            return;

        // The scheme metadata cache copies _db_type, so readers would keep answering from the stale
        // copy. Dropping the affected rows is enough — the cache is rebuilt on demand.
        await Context.ExecuteAsync(
            "DELETE FROM _scheme_metadata_cache WHERE _scheme_id IN " +
            "(SELECT DISTINCT _id_scheme FROM _structures WHERE _id_type = " + RedbTypeIds.DateOnly + ")");

    }

    /// <summary>
    /// SQLite ships no v2-pvt bundle, so the base version check is a no-op; the schema-upgrade pass
    /// is the SQLite delivery of the same contract and must reach an existing file through
    /// <c>InitializeAsync()</c> exactly as the module block "0. Schema upgrades" reaches PostgreSQL
    /// and MSSQL, not only through <see cref="EnsureDatabaseAsync"/>. A file without the schema is
    /// left to <see cref="EnsureDatabaseAsync"/> (or the first synchronisation) to report;
    /// <see cref="EnsureDatabaseAsync"/> itself runs the pass after creation, so the base call made
    /// from there is skipped. <c>AutoApplyDatabaseUpgrades</c> is ignored on SQLite by contract.
    /// </summary>
    protected override async Task EnsurePvtModuleDeployedAsync()
    {
        if (_ensuringDatabase) return;
        if (!await TableExistsAsync("_schemes")) return;
        await ApplySchemaUpgradesAsync();
    }

    private bool _ensuringDatabase;

    /// <inheritdoc />
    public override async Task EnsureDatabaseAsync()
    {
        // Asked before the base call, which creates the tables when they are missing: a freshly created
        // database is already correct and needs no correction pass.
        var existedBefore = await TableExistsAsync("_schemes");

        _ensuringDatabase = true;
        try { await base.EnsureDatabaseAsync(); }
        finally { _ensuringDatabase = false; }

        if (!existedBefore)
        {
            // Created by this build: every upgrade is already in the schema script.
            await StampSchemaVersionAsync();
            return;
        }

        await ApplySchemaUpgradesAsync();
        await ApplySeedCorrectionsAsync();
    }
}
