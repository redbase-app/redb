using Microsoft.Extensions.Configuration;
using Npgsql;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// Owns database <c>redb_upgrade</c> and role <c>redb_noddl</c> on the test server. Not in any
/// collection on purpose: nothing else touches that database, so it can run in parallel with the
/// shared-database suites.
/// </summary>
public class PostgresSchemaUpgradeTests : SchemaUpgradeTestsBase
{
    // Virtual so the Pro subclass owns its own database and role: the two classes run in
    // parallel and must not share the database they deliberately damage.
    protected virtual string DatabaseName => "redb_upgrade";
    protected virtual string FreshDatabaseName => "redb_upgrade_fresh";
    protected virtual string RestrictedRole => "redb_noddl";
    private const string RestrictedPassword = "noddl";

    private static readonly string AdminBase =
        new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;

    protected override string OwnerConnectionString =>
        new NpgsqlConnectionStringBuilder(AdminBase) { Database = DatabaseName }.ConnectionString;

    protected override string FreshDatabaseConnectionString =>
        new NpgsqlConnectionStringBuilder(AdminBase) { Database = FreshDatabaseName }.ConnectionString;

    protected override async Task RecreateFreshDatabaseAsync()
    {
        await ExecAdminAsync("postgres", $"DROP DATABASE IF EXISTS \"{FreshDatabaseName}\" WITH (FORCE)");
        await ExecAdminAsync("postgres", $"CREATE DATABASE \"{FreshDatabaseName}\"");
    }

    protected override string RestrictedConnectionString =>
        new NpgsqlConnectionStringBuilder(AdminBase)
        {
            Database = DatabaseName, Username = RestrictedRole, Password = RestrictedPassword
        }.ConnectionString;

    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UsePostgres(connectionString);

    protected static async Task ExecAdminAsync(string database, string sql)
    {
        var cs = new NpgsqlConnectionStringBuilder(AdminBase) { Database = database, Pooling = false }.ConnectionString;
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    protected static async Task<T?> ScalarAdminAsync<T>(string database, string sql)
    {
        var cs = new NpgsqlConnectionStringBuilder(AdminBase) { Database = database, Pooling = false }.ConnectionString;
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var v = await cmd.ExecuteScalarAsync();
        return v is null or DBNull ? default : (T)v;
    }

    protected override async Task PrepareDatabaseAndLoginAsync()
    {
        if (await ScalarAdminAsync<object>("postgres", $"SELECT 1 FROM pg_database WHERE datname = '{DatabaseName}'") is null)
            await ExecAdminAsync("postgres", $"CREATE DATABASE \"{DatabaseName}\"");

        if (await ScalarAdminAsync<object>("postgres", $"SELECT 1 FROM pg_roles WHERE rolname = '{RestrictedRole}'") is null)
            await ExecAdminAsync("postgres", $"CREATE ROLE {RestrictedRole} LOGIN PASSWORD '{RestrictedPassword}'");

        await ExecAdminAsync("postgres", $"GRANT CONNECT ON DATABASE \"{DatabaseName}\" TO {RestrictedRole}");
    }

    protected override async Task GrantRestrictedAsync()
    {
        // Read and write everything, execute everything, own nothing. Exactly the shape a DBA leaves
        // an application in after installation.
        await ExecAdminAsync(DatabaseName, $"""
            GRANT USAGE ON SCHEMA public TO {RestrictedRole};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {RestrictedRole};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {RestrictedRole};
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {RestrictedRole};
            """);
    }

    protected override Task SetDeployedModuleVersionAsync(string version)
        => ExecAdminAsync(DatabaseName, $"""
            CREATE OR REPLACE FUNCTION pvt_module_version() RETURNS text LANGUAGE plpgsql IMMUTABLE
            AS $$ BEGIN RETURN '{version}'; END $$;
            """);

    protected override async Task<string> ReadDeployedModuleVersionAsync()
        => (await ScalarAdminAsync<string>(DatabaseName, "SELECT pvt_module_version()"))!;

    protected override Task ApplyScriptAsOwnerAsync(IRedbContext ownerContext, string script)
        => ownerContext.ExecuteAsync(script);

    protected override async Task DropV4UpgradeDdlAsync()
    {
        await ExecAdminAsync(DatabaseName, """
            DROP INDEX IF EXISTS "UIX__values__structure_unique";
            DROP INDEX IF EXISTS "UIX__objects__scheme_unique";
            ALTER TABLE _values DROP COLUMN IF EXISTS _unique;
            ALTER TABLE _structures DROP COLUMN IF EXISTS _unique;
            ALTER TABLE _structures DROP COLUMN IF EXISTS _unique_version;
            ALTER TABLE _structures DROP COLUMN IF EXISTS _lazy;
            ALTER TABLE _scheme_metadata_cache DROP COLUMN IF EXISTS _unique;
            ALTER TABLE _scheme_metadata_cache DROP COLUMN IF EXISTS _unique_version;
            ALTER TABLE _scheme_metadata_cache DROP COLUMN IF EXISTS _lazy;
            ALTER TABLE _objects DROP COLUMN IF EXISTS _value_unique;
            """);
    }

    protected override async Task<bool> V4UpgradeDdlExistsAsync()
    {
        var present = await ScalarAdminAsync<object>(DatabaseName, """
            SELECT (SELECT COUNT(*) FROM information_schema.columns
                    WHERE (table_name, column_name) IN (('_values','_unique'), ('_structures','_unique'),
                          ('_structures','_unique_version'), ('_structures','_lazy'),
                          ('_scheme_metadata_cache','_unique'), ('_scheme_metadata_cache','_unique_version'),
                          ('_scheme_metadata_cache','_lazy'), ('_objects','_value_unique')))
                 + (SELECT COUNT(*) FROM pg_indexes
                    WHERE indexname IN ('UIX__values__structure_unique', 'UIX__objects__scheme_unique'))
            """);
        return Convert.ToInt64(present) == 10;
    }
}
