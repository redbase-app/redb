using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using redb.Core.Models.Contracts;
using redb.Core.Models.Configuration;
using redb.MSSql.Data;
using redb.MSSql.Providers;
using redb.MSSql.Query;
using redb.MSSql.Sql;

namespace redb.MSSql;

/// <summary>
/// MSSQL implementation of IRedbService.
/// Inherits all common logic from RedbServiceBase.
/// Only provides MSSQL-specific provider factories.
/// </summary>
public class RedbService : RedbServiceBase
{
    private static readonly MsSqlDialect _dialect = new();
    
    /// <summary>
    /// Creates a new MSSQL RedbService instance.
    /// </summary>
    public RedbService(IServiceProvider serviceProvider) : base(serviceProvider)
    {
    }

    // === MSSQL-SPECIFIC IMPLEMENTATIONS ===
    
    protected override string DatabaseTypeName => "MSSql";
    
    protected override ISqlDialect SqlDialect => _dialect;
    
    protected override string GetVersionSql => "SELECT @@VERSION";
    
    protected override string GetDatabaseSizeSql => 
        "SELECT SUM(CAST(size AS BIGINT)) * 8192 FROM sys.database_files"; // size counts 8 KB pages; dbSize is bytes
    
    protected override string ContextNotRegisteredError => 
        "IRedbContext is not registered in DI container. Add SqlRedbContext to configuration.";
    
    // === PROVIDER FACTORIES ===
    
    protected override ISchemeSyncProvider CreateSchemeSyncProvider(
        IRedbContext context, RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new MssqlSchemeSyncProvider(context, config, cacheDomain, logger);
    
    protected override IPermissionProvider CreatePermissionProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new MssqlPermissionProvider(context, securityContext, logger);
    
    protected override IUserProvider CreateUserProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new MssqlUserProvider(context, securityContext, ResolvePasswordHasher(), logger);
    
    protected override IRoleProvider CreateRoleProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new MssqlRoleProvider(context, securityContext, logger);
    
    protected override IListProvider CreateListProvider(
        IRedbContext context, RedbServiceConfiguration config, ISchemeSyncProvider schemeSync, ILogger? logger)
        => new MssqlListProvider(context, config, schemeSync, logger);
    
    protected override IObjectStorageProvider CreateObjectStorageProvider(
        IRedbContext context, IRedbObjectSerializer serializer, IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext, ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration config, IListProvider listProvider, ILogger? logger,
        redb.Core.Interception.RedbSaveInterceptorPipeline? saveInterceptors)
        => new MssqlObjectStorageProvider(context, serializer, permissionProvider, 
            securityContext, schemeSync, config, listProvider, logger, saveInterceptors);
    
    protected override ITreeProvider CreateTreeProvider(
        IRedbContext context, IObjectStorageProvider objectStorage, IPermissionProvider permissionProvider,
        IRedbObjectSerializer serializer, IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync, RedbServiceConfiguration config, ILogger? logger)
        => new MssqlTreeProvider(context, objectStorage, permissionProvider, 
            serializer, securityContext, schemeSync, config, logger);
    
    protected override ILazyPropsLoader CreateLazyPropsLoader(
        IRedbContext context, ISchemeSyncProvider schemeSync, IRedbObjectSerializer serializer,
        RedbServiceConfiguration config, string cacheDomain, IListProvider listProvider, ILogger? logger)
        => new LazyPropsLoader(context, schemeSync, serializer, config, listProvider, logger);
    
    protected override IQueryableProvider CreateQueryableProvider(
        IRedbContext context, IRedbObjectSerializer serializer, ISchemeSyncProvider schemeSync,
        IRedbSecurityContext securityContext, ILazyPropsLoader lazyPropsLoader,
        RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new MssqlQueryableProvider(context, serializer, schemeSync, securityContext, 
            lazyPropsLoader, config, cacheDomain, logger);
    
    protected override IValidationProvider CreateValidationProvider(
        IRedbContext context, ILogger? logger)
        => new MssqlValidationProvider(context, logger);

    protected override Core.Providers.IMaintenanceProvider CreateMaintenanceProvider(
        IRedbContext context, int analysisLimit, ILogger? logger)
        => new Providers.MssqlMaintenanceProvider(context, analysisLimit, logger);

    // === DATABASE SCHEMA MANAGEMENT ===

    /// <inheritdoc />
    protected override async Task<bool> TableExistsAsync(string tableName)
    {
        // Params: table name, schema (null = dbo).
        if (Context is not SqlRedbContext context)
            return await Context.ExecuteScalarAsync<int>(_dialect.Maintenance_SelectTableExists(), tableName, DBNull.Value) > 0;

        // On a connection of its own, returned to the pool at once: the context keeps its connection open for its
        // lifetime, and a node that looked for the tables and then waits for the schema lock would hold a session in
        // the database the whole time, which the creator's ALTER DATABASE (READ_COMMITTED_SNAPSHOT) waits out. The
        // pool is cleared before the lock is awaited (OpenSchemaLockAsync).
        await using var connection = new Microsoft.Data.SqlClient.SqlConnection(context.ConnectionString);
        await connection.OpenAsync();
        return Convert.ToInt32(await ScalarAsync(connection, _dialect.Maintenance_SelectTableExists(), CancellationToken.None,
            tableName, DBNull.Value)) > 0;
    }

    /// <inheritdoc />
    protected override string ReadEmbeddedSql()
    {
        var assembly = typeof(RedbService).Assembly;
        var resourceName = "redb.MSSql.sql.redb_init.sql";

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
        var resourceName = "redb.MSSql.sql.v2-pvt.pvt_bundle.sql";

        using var stream = assembly.GetManifestResourceStream(resourceName);
        if (stream is null)
            return null;

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    /// <inheritdoc />
    protected override async Task ExecuteSchemaScriptAsync(string sql)
    {
        // MSSQL uses GO as a batch separator — split and execute each batch
        var batches = System.Text.RegularExpressions.Regex.Split(sql, @"^\s*GO\s*$",
            System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (!string.IsNullOrEmpty(trimmed))
                await Context.ExecuteAsync(trimmed);
        }
    }

    /// <inheritdoc />
    protected override async Task<SchemaLockHandle> OpenSchemaLockAsync(CancellationToken cancellationToken)
    {
        if (Context is not SqlRedbContext sql)
            return SchemaLockHandle.None;

        // Where the lock lives follows a property every node of one database reads the same, never an error: a node
        // that took it elsewhere after failing to reach master would not see the lock of the others.
        //   Azure SQL Database (EngineEdition 5): in the database itself. READ_COMMITTED_SNAPSHOT is on there from the
        //   start, the creation script does not alter the database, and a session in it keeps nobody waiting.
        //   Any other SQL Server: in master. The creation script turns on READ_COMMITTED_SNAPSHOT, and ALTER DATABASE
        //   waits until it is the only session in the database: a lock held there, or a waiting node logging in to
        //   look, would keep the creator waiting.
        Microsoft.Data.SqlClient.SqlConnection? master = new Microsoft.Data.SqlClient.SqlConnection(
            new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(sql.ConnectionString) { InitialCatalog = "master" }.ConnectionString);
        Microsoft.Data.SqlClient.SqlException? masterError = null;
        try
        {
            await master.OpenAsync(cancellationToken);
        }
        catch (Microsoft.Data.SqlClient.SqlException ex)
        {
            // Asked in the database instead below: on Azure SQL Database a login without a user in master is normal.
            masterError = ex;
        }

        try
        {
            var edition = masterError == null
                ? Convert.ToInt32(await ScalarAsync(master, _dialect.Server_EngineEdition(), cancellationToken))
                : await Context.ExecuteScalarAsync<int>(_dialect.Server_EngineEdition());

            if (edition == AzureSqlDatabaseEdition)
            {
                var own = new Microsoft.Data.SqlClient.SqlConnection(sql.ConnectionString);
                try
                {
                    await own.OpenAsync(cancellationToken);
                }
                catch
                {
                    await own.DisposeAsync();
                    throw;
                }
                return new ApplicationLock(own, "redb.schema");
            }

            // Reached only when there is schema work to do: a database already deployed starts without the lock.
            if (masterError != null)
                throw new InvalidOperationException(
                    "The database needs schema work (creating the schema, upgrading the SQL module or synchronizing a " +
                    "scheme), which REDB does under a schema lock held in the master database, and the login of this " +
                    $"connection could not open master: {masterError.Message} Either give the login access to master, " +
                    "or deploy the schema with the DBA scripts (GetSchemaScript, GetUpgradeScript) and synchronize the " +
                    "schemes with a login that has it.", masterError);

            // An application lock is per database: in master the name of the database tells them apart.
            var database = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(sql.ConnectionString).InitialCatalog;
            if (string.IsNullOrEmpty(database))
                database = await Context.ExecuteScalarAsync<string>(_dialect.Server_CurrentDatabase());

            // Idle connections of this process's pool are sessions in the database too: closed before the lock is
            // handed out, so the creation script of this process does not wait for its own pool.
            await using (var pooled = new Microsoft.Data.SqlClient.SqlConnection(sql.ConnectionString))
                Microsoft.Data.SqlClient.SqlConnection.ClearPool(pooled);

            var handle = new ApplicationLock(master, $"redb.schema:{database}");
            master = null;
            return handle;
        }
        finally
        {
            if (master != null)
                await master.DisposeAsync();
        }
    }

    /// <inheritdoc />
    /// <remarks>18456 login failed, 4060 the database of the login cannot be opened: both seen while a node creating
    /// the schema turns on READ_COMMITTED_SNAPSHOT.</remarks>
    protected override bool IsDatabaseClosedForSchemaWork(Exception exception)
        => exception is Microsoft.Data.SqlClient.SqlException { Number: 18456 or 4060 };

    // SERVERPROPERTY('EngineEdition') of Azure SQL Database (Managed Instance is 8, a server of one's own 2 to 4).
    private const int AzureSqlDatabaseEdition = 5;

    /// <summary>A dialect statement on a connection of its own; the values bind to @p0, @p1, ... as in the context.</summary>
    private static async Task<object?> ScalarAsync(
        Microsoft.Data.SqlClient.SqlConnection connection, string commandText, CancellationToken cancellationToken,
        params object[] parameters)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        for (var i = 0; i < parameters.Length; i++)
            command.Parameters.AddWithValue($"@p{i}", parameters[i]);
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    /// <summary>
    /// The schema lock of SQL Server: an exclusive application lock owned by the session of a connection of its own.
    /// Every attempt asks with no wait (@LockTimeout = 0), so the wait and its log stay in the service. sp_getapplock
    /// is granted to public: any user of the database may take it.
    /// </summary>
    private sealed class ApplicationLock(Microsoft.Data.SqlClient.SqlConnection connection, string resource) : SchemaLockHandle
    {
        private bool _held;

        public override string Name => $"sp_getapplock '{resource}'";

        public override async Task<bool> TryAcquireAsync(CancellationToken cancellationToken)
        {
            var result = Convert.ToInt32(await ScalarAsync(connection, _dialect.SchemaLock_TryAcquire(), cancellationToken, resource));
            // 0 and 1: granted; -1: held by another session. Anything else (-2 cancelled, -3 deadlock, -999 error) is
            // not a wait to sit out.
            if (result is < -1)
                throw new InvalidOperationException($"sp_getapplock on '{resource}' returned {result}.");
            _held = result >= 0;
            return _held;
        }

        public override async ValueTask DisposeAsync()
        {
            try
            {
                if (_held)
                    await ScalarAsync(connection, _dialect.SchemaLock_Release(), CancellationToken.None, resource);
            }
            finally
            {
                // Closing ends the session, and the session takes its application locks with it.
                await connection.DisposeAsync();
            }
        }
    }
}
