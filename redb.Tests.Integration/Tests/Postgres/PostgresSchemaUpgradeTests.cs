using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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

    protected override Task SetDeployedModuleCompatAsync(string? compat)
        => ExecAdminAsync(DatabaseName, compat is null
            ? "DROP FUNCTION IF EXISTS pvt_module_compat()"
            : $"""
              CREATE OR REPLACE FUNCTION pvt_module_compat() RETURNS text LANGUAGE sql IMMUTABLE
              AS $$ SELECT '{compat}'::text $$;
              """);

    protected override Task ApplyScriptAsOwnerAsync(IRedbContext ownerContext, string script)
        => ownerContext.ExecuteAsync(script);

    protected override async Task<string> ReadSchemaFingerprintAsync()
        => (await ScalarAdminAsync<string>(DatabaseName, """
            SELECT concat_ws('|',
                (SELECT count(*) FROM pg_constraint WHERE contype = 'f'),
                (SELECT last_value::text FROM global_identity),
                (SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal),
                (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'),
                (SELECT count(*) FROM _objects), (SELECT count(*) FROM _values),
                (SELECT count(*) FROM _schemes), (SELECT count(*) FROM _users))
            """))!;

    /// <summary>psql with -v ON_ERROR_STOP=1, as the header of the script asks: the run stops at the first error.</summary>
    protected override async Task<List<string>> RunScriptByHandAsync(string script)
    {
        try
        {
            await ExecAdminAsync(DatabaseName, script);
            return [];
        }
        catch (PostgresException ex)
        {
            return [ex.MessageText];
        }
    }

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

    [Fact]
    public async Task RootDuplicates_EmptyOnesRemoved_OneWithValuesWaitsForTheHandFix()
    {
        // A database written before the schema lock holds root fields twice. The module upgrade removes the empty
        // copies (a nested class with its child among them), keeps the copy with values and leaves the unique key of
        // root structures unbuilt with a warning; once the copy is resolved by hand, the next upgrade builds it.
        await using var owner = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"root-dup-{Guid.NewGuid():N}");
        var redb = owner.GetRequiredService<redb.Core.IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        var context = owner.GetRequiredService<IRedbContext>();
        var scenario = await RootDuplicatesScenario.PlantAsync(redb, context);
        try
        {
            var logs = new CollectingLoggerProvider();
            await SetDeployedModuleVersionAsync("0.0.0-test");
            await using (var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"root-dup-{Guid.NewGuid():N}", logs: logs))
            {
                var started = sp.GetRequiredService<redb.Core.IRedbService>();
                await started.InitializeAsync(ensureCreated: true);
                scenario.ShouldReportCountOnly(started, logs);
            }

            (await RootDuplicatesScenario.StructureExistsAsync(context, scenario.TitleDuplicate)).Should().BeFalse();
            (await RootDuplicatesScenario.StructureExistsAsync(context, scenario.PartDuplicate)).Should().BeFalse();
            (await RootDuplicatesScenario.StructureExistsAsync(context, scenario.PartDuplicateChild)).Should().BeFalse();
            (await RootDuplicatesScenario.StructureExistsAsync(context, scenario.CountDuplicate)).Should().BeTrue("it holds a value");
            (await RootKeyExistsAsync()).Should().BeFalse();

            // A plain start, no module upgrade: the error again, the same list.
            var again = new CollectingLoggerProvider();
            await using (var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"root-dup-{Guid.NewGuid():N}", logs: again))
            {
                var started = sp.GetRequiredService<redb.Core.IRedbService>();
                await started.InitializeAsync(ensureCreated: true);
                scenario.ShouldReportCountOnly(started, again);
            }

            await scenario.ResolveByHandAsync(context);
            await SetDeployedModuleVersionAsync("0.0.0-test");
            await using (var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"root-dup-{Guid.NewGuid():N}"))
            {
                var started = sp.GetRequiredService<redb.Core.IRedbService>();
                await started.InitializeAsync(ensureCreated: true);
                started.SchemaProblems.Should().BeEmpty();
            }
            (await RootKeyExistsAsync()).Should().BeTrue();
        }
        finally
        {
            await context.ExecuteAsync($"DELETE FROM _objects WHERE _id = {scenario.ObjectId}");
            await scenario.ResolveByHandAsync(context);
            await context.ExecuteAsync(
                $"CREATE UNIQUE INDEX IF NOT EXISTS \"{RootStructureKey.IndexName}\" ON _structures (_id_scheme, _name) WHERE _id_parent IS NULL");
        }
    }

    /// <summary>
    /// A database of 4.7.5 (module 0.7.23.1, field names checked against the C# keywords in lower case) gets the
    /// field-name check of this build at the next owner start: the module goes to the required version and block 0 of the
    /// bundle replaces validate_structure_name(), so a property named Ref syncs.
    /// </summary>
    [Fact]
    public async Task Owner_475Module_GetsTheCaseSensitiveKeywordCheck()
    {
        var current = await ScalarAdminAsync<string>(DatabaseName, "SELECT pg_get_functiondef('validate_structure_name()'::regprocedure)");
        current.Should().Contain("IF NEW._name = ANY(ARRAY[", "this build compares the keywords as written");
        await ExecAdminAsync(DatabaseName, current!.Replace("IF NEW._name = ANY(ARRAY[", "IF LOWER(NEW._name) = ANY(ARRAY["));
        await SetDeployedModuleVersionAsync("0.7.23.1");

        await using var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"keyword-case-{Guid.NewGuid():N}");
        var redb = sp.GetRequiredService<redb.Core.IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        (await ReadDeployedModuleVersionAsync()).Should().NotBe("0.7.23.1", "the start upgrades the 4.7.5 module");
        var upgraded = await ScalarAdminAsync<string>(DatabaseName, "SELECT pg_get_functiondef('validate_structure_name()'::regprocedure)");
        upgraded.Should().Contain("IF NEW._name = ANY(ARRAY[").And.NotContain("LOWER(NEW._name) = ANY(ARRAY[");
        await redb.SyncSchemeAsync<redb.Tests.Integration.Models.KeywordNamesProps>();
    }

    private async Task<bool> RootKeyExistsAsync()
        => await ScalarAdminAsync<object>(DatabaseName,
            $"SELECT 1 FROM pg_indexes WHERE indexname = '{RootStructureKey.IndexName}'") != null;
}
