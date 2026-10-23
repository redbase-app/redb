using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Security;
using redb.Core.Models.Contracts;
using redb.Core.Models.Configuration;
using redb.Postgres.Data;
using redb.Postgres.Providers;
using redb.Postgres.Sql;

namespace redb.Postgres;

/// <summary>
/// PostgreSQL implementation of IRedbService.
/// Inherits all common logic from RedbServiceBase.
/// Only provides PostgreSQL-specific provider factories.
/// </summary>
public class RedbService : RedbServiceBase
{
    private static readonly PostgreSqlDialect _dialect = new();
    
    /// <summary>
    /// Creates a new PostgreSQL RedbService instance.
    /// </summary>
    public RedbService(IServiceProvider serviceProvider) : base(serviceProvider)
    {
    }

    // === POSTGRESQL-SPECIFIC IMPLEMENTATIONS ===
    
    protected override string DatabaseTypeName => "PostgreSQL";
    
    protected override ISqlDialect SqlDialect => _dialect;
    
    protected override string GetVersionSql => "SELECT version()";
    
    protected override string GetDatabaseSizeSql => "SELECT pg_database_size(current_database())";
    
    protected override string ContextNotRegisteredError => 
        "IRedbContext is not registered in DI container. Add NpgsqlRedbContext to configuration.";
    
    // === PROVIDER FACTORIES ===
    
    protected override ISchemeSyncProvider CreateSchemeSyncProvider(
        IRedbContext context, RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new PostgresSchemeSyncProvider(context, config, cacheDomain, logger);
    
    protected override IPermissionProvider CreatePermissionProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new PostgresPermissionProvider(context, securityContext, logger);
    
    protected override IUserProvider CreateUserProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new PostgresUserProvider(context, securityContext, ResolvePasswordHasher(), logger);
    
    protected override IRoleProvider CreateRoleProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new PostgresRoleProvider(context, securityContext, logger);
    
    protected override IListProvider CreateListProvider(
        IRedbContext context, RedbServiceConfiguration config, ISchemeSyncProvider schemeSync, ILogger? logger)
        => new PostgresListProvider(context, config, schemeSync, logger);
    
    protected override IObjectStorageProvider CreateObjectStorageProvider(
        IRedbContext context, IRedbObjectSerializer serializer, IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext, ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration config, IListProvider listProvider, ILogger? logger,
        redb.Core.Interception.RedbSaveInterceptorPipeline? saveInterceptors)
        => new PostgresObjectStorageProvider(context, serializer, permissionProvider, 
            securityContext, schemeSync, config, listProvider, logger, saveInterceptors);
    
    protected override ITreeProvider CreateTreeProvider(
        IRedbContext context, IObjectStorageProvider objectStorage, IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer, IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync, RedbServiceConfiguration config, ILogger? logger)
        => new PostgresTreeProvider(context, objectStorage, permissionProvider, 
            serializer, securityContext, schemeSync, config, logger);
    
    protected override ILazyPropsLoader CreateLazyPropsLoader(
        IRedbContext context, ISchemeSyncProvider schemeSync, IRedbObjectSerializer serializer,
        RedbServiceConfiguration config, string cacheDomain, IListProvider listProvider, ILogger? logger)
        => new LazyPropsLoader(context, schemeSync, serializer, config, listProvider, logger);
    
    protected override IQueryableProvider CreateQueryableProvider(
        IRedbContext context, IRedbObjectSerializer serializer, ISchemeSyncProvider schemeSync,
        IRedbSecurityContext securityContext, ILazyPropsLoader lazyPropsLoader,
        RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new PostgresQueryableProvider(context, serializer, schemeSync, securityContext, 
            lazyPropsLoader, config, cacheDomain, logger);
    
    protected override IValidationProvider CreateValidationProvider(
        IRedbContext context, ILogger? logger)
        => new PostgresValidationProvider(context, logger);

    protected override Core.Providers.IMaintenanceProvider CreateMaintenanceProvider(
        IRedbContext context, int analysisLimit, ILogger? logger)
        => new Providers.PostgresMaintenanceProvider(context, analysisLimit, logger);

    // === DATABASE SCHEMA MANAGEMENT ===

    /// <inheritdoc />
    protected override async Task<bool> TableExistsAsync(string tableName)
    {
        var sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = @p0)";
        return await Context.ExecuteScalarAsync<bool>(sql, tableName);
    }

    /// <inheritdoc />
    protected override string ReadEmbeddedSql()
    {
        var assembly = typeof(RedbService).Assembly;
        var resourceName = "redb.Postgres.sql.redb_init.sql";

        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{resourceName}' not found. Ensure the project was built correctly.");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <inheritdoc />
    protected override string? ReadEmbeddedPvtBundleSql()
    {
        var assembly = typeof(RedbService).Assembly;
        var resourceName = "redb.Postgres.sql.v2-pvt.pvt_bundle.sql";

        using var stream = assembly.GetManifestResourceStream(resourceName);
        if (stream is null)
            return null;

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <inheritdoc />
    protected override async Task ExecuteSchemaScriptAsync(string sql)
    {
        await Context.ExecuteAsync(sql);
    }

    /// <inheritdoc />
    protected override async Task<SchemaLockHandle> OpenSchemaLockAsync(CancellationToken cancellationToken)
    {
        if (Context is not NpgsqlRedbContext npgsql)
            return SchemaLockHandle.None;
        return new AdvisoryLock(await npgsql.DataSource.OpenConnectionAsync(cancellationToken));
    }

    /// <summary>
    /// The schema lock of PostgreSQL: a session advisory lock on a connection of its own, outside any transaction.
    /// Taken with pg_try_advisory_lock, never pg_advisory_lock: a backend blocked in pg_advisory_lock holds the
    /// snapshot of its statement, and CREATE INDEX CONCURRENTLY of the node that holds the lock waits for every such
    /// snapshot, so the two would wait for each other. Advisory locks are per database, so one key serves all of them.
    /// </summary>
    private sealed class AdvisoryLock(Npgsql.NpgsqlConnection connection) : SchemaLockHandle
    {
        // "redb_sch" in ASCII.
        private const long Key = 0x726564625F736368;
        private bool _held;

        public override string Name => $"pg_advisory_lock({Key})";

        public override async Task<bool> TryAcquireAsync(CancellationToken cancellationToken)
        {
            await using var command = new Npgsql.NpgsqlCommand(_dialect.SchemaLock_TryAcquire(), connection);
            command.Parameters.Add(new Npgsql.NpgsqlParameter { Value = Key });
            _held = (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
            return _held;
        }

        public override async ValueTask DisposeAsync()
        {
            try
            {
                if (_held)
                {
                    await using var command = new Npgsql.NpgsqlCommand(_dialect.SchemaLock_Release(), connection);
                    command.Parameters.Add(new Npgsql.NpgsqlParameter { Value = Key });
                    await command.ExecuteScalarAsync();
                }
            }
            finally
            {
                // Closing ends the session too, and the session takes its advisory locks with it.
                await connection.DisposeAsync();
            }
        }
    }
}
