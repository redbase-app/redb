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
        "SELECT SUM(CAST(size AS BIGINT) * 8) FROM sys.database_files";
    
    protected override string ContextNotRegisteredError => 
        "IRedbContext is not registered in DI container. Add SqlRedbContext to configuration.";
    
    protected override string GetObjectJsonSql() => "SELECT dbo.get_object_json(@p0, @p1)";
    
    // === PROVIDER FACTORIES ===
    
    protected override ISchemeSyncProvider CreateSchemeSyncProvider(
        IRedbContext context, RedbServiceConfiguration config, string cacheDomain, ILogger? logger)
        => new MssqlSchemeSyncProvider(context, config, cacheDomain, logger);
    
    protected override IPermissionProvider CreatePermissionProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new MssqlPermissionProvider(context, securityContext, logger);
    
    protected override IUserProvider CreateUserProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new MssqlUserProvider(context, securityContext, logger);
    
    protected override IRoleProvider CreateRoleProvider(
        IRedbContext context, IRedbSecurityContext securityContext, ILogger? logger)
        => new MssqlRoleProvider(context, securityContext, logger);
    
    protected override IListProvider CreateListProvider(
        IRedbContext context, RedbServiceConfiguration config, ISchemeSyncProvider schemeSync, ILogger? logger)
        => new MssqlListProvider(context, config, schemeSync, logger);
    
    protected override IObjectStorageProvider CreateObjectStorageProvider(
        IRedbContext context, IRedbObjectSerializer serializer, IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext, ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration config, IListProvider listProvider, ILogger? logger)
        => new MssqlObjectStorageProvider(context, serializer, permissionProvider, 
            securityContext, schemeSync, config, listProvider, logger);
    
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

    // === DATABASE SCHEMA MANAGEMENT ===

    /// <inheritdoc />
    protected override async Task<bool> TableExistsAsync(string tableName)
    {
        var sql = "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.tables WHERE name = @p0) THEN 1 ELSE 0 END";
        var result = await Context.ExecuteScalarAsync<int>(sql, tableName);
        return result == 1;
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
}
