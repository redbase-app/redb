using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// Owns database <c>redb_upgrade</c> and login <c>redb_noddl</c> on the test server. See the
/// PostgreSQL twin for why it is not in a collection.
/// </summary>
public class MsSqlSchemaUpgradeTests : SchemaUpgradeTestsBase
{
    // Virtual so the Pro subclass owns its own database and login: both classes damage the
    // schema on purpose and must not share it.
    protected virtual string DatabaseName => "redb_upgrade";
    protected virtual string FreshDatabaseName => "redb_upgrade_fresh";
    protected virtual string RestrictedLogin => "redb_noddl";
    private const string RestrictedPassword = "NoDdl!12345";

    private static readonly string AdminBase =
        new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;

    protected override string OwnerConnectionString =>
        new SqlConnectionStringBuilder(AdminBase) { InitialCatalog = DatabaseName }.ConnectionString;

    protected override string FreshDatabaseConnectionString =>
        new SqlConnectionStringBuilder(AdminBase) { InitialCatalog = FreshDatabaseName }.ConnectionString;

    protected override async Task RecreateFreshDatabaseAsync()
    {
        await ExecAdminAsync("master",
            $"IF DB_ID(N'{FreshDatabaseName}') IS NOT NULL BEGIN " +
            $"ALTER DATABASE [{FreshDatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " +
            $"DROP DATABASE [{FreshDatabaseName}]; END");
        await ExecAdminAsync("master", $"CREATE DATABASE [{FreshDatabaseName}]");
    }

    protected override string RestrictedConnectionString =>
        new SqlConnectionStringBuilder(AdminBase)
        {
            InitialCatalog = DatabaseName, UserID = RestrictedLogin, Password = RestrictedPassword
        }.ConnectionString;

    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UseMsSql(connectionString);

    private static async Task ExecAdminAsync(string database, string sql)
    {
        var cs = new SqlConnectionStringBuilder(AdminBase) { InitialCatalog = database, Pooling = false }.ConnectionString;
        await using var conn = new SqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<T?> ScalarAdminAsync<T>(string database, string sql)
    {
        var cs = new SqlConnectionStringBuilder(AdminBase) { InitialCatalog = database, Pooling = false }.ConnectionString;
        await using var conn = new SqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var v = await cmd.ExecuteScalarAsync();
        return v is null or DBNull ? default : (T)v;
    }

    protected override async Task PrepareDatabaseAndLoginAsync()
    {
        if (await ScalarAdminAsync<object>("master", $"SELECT 1 FROM sys.databases WHERE name = '{DatabaseName}'") is null)
            await ExecAdminAsync("master", $"CREATE DATABASE [{DatabaseName}]");

        if (await ScalarAdminAsync<object>("master", $"SELECT 1 FROM sys.server_principals WHERE name = '{RestrictedLogin}'") is null)
            await ExecAdminAsync("master",
                $"CREATE LOGIN [{RestrictedLogin}] WITH PASSWORD = '{RestrictedPassword}', CHECK_POLICY = OFF");

        if (await ScalarAdminAsync<object>(DatabaseName, $"SELECT 1 FROM sys.database_principals WHERE name = '{RestrictedLogin}'") is null)
            await ExecAdminAsync(DatabaseName, $"CREATE USER [{RestrictedLogin}] FOR LOGIN [{RestrictedLogin}]");
    }

    protected override async Task GrantRestrictedAsync()
    {
        // db_datareader + db_datawriter + EXECUTE: reads, writes, calls functions. No CREATE, no
        // ALTER, no DROP — the shape a DBA leaves an application in after installation.
        await ExecAdminAsync(DatabaseName, $"""
            ALTER ROLE db_datareader ADD MEMBER [{RestrictedLogin}];
            ALTER ROLE db_datawriter ADD MEMBER [{RestrictedLogin}];
            GRANT EXECUTE TO [{RestrictedLogin}];
            """);
    }

    protected override Task SetDeployedModuleVersionAsync(string version)
        => ExecAdminAsync(DatabaseName, $"""
            ALTER FUNCTION dbo.pvt_module_version() RETURNS nvarchar(50) WITH SCHEMABINDING
            AS BEGIN RETURN N'{version}'; END
            """);

    protected override async Task<string> ReadDeployedModuleVersionAsync()
        => (await ScalarAdminAsync<string>(DatabaseName, "SELECT dbo.pvt_module_version()"))!;


    protected override async Task DropV4UpgradeDdlAsync()
    {
        await ExecAdminAsync(DatabaseName, """
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'UIX__values__structure_unique')
                DROP INDEX [UIX__values__structure_unique] ON [dbo].[_values];
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'UIX__objects__scheme_unique')
                DROP INDEX [UIX__objects__scheme_unique] ON [dbo].[_objects];
            IF COL_LENGTH('dbo._values', '_unique') IS NOT NULL ALTER TABLE [dbo].[_values] DROP COLUMN [_unique];
            IF COL_LENGTH('dbo._structures', '_unique') IS NOT NULL ALTER TABLE [dbo].[_structures] DROP COLUMN [_unique];
            IF COL_LENGTH('dbo._structures', '_unique_version') IS NOT NULL ALTER TABLE [dbo].[_structures] DROP COLUMN [_unique_version];
            IF COL_LENGTH('dbo._structures', '_lazy') IS NOT NULL ALTER TABLE [dbo].[_structures] DROP COLUMN [_lazy];
            IF COL_LENGTH('dbo._scheme_metadata_cache', '_unique') IS NOT NULL ALTER TABLE [dbo].[_scheme_metadata_cache] DROP COLUMN [_unique];
            IF COL_LENGTH('dbo._scheme_metadata_cache', '_unique_version') IS NOT NULL ALTER TABLE [dbo].[_scheme_metadata_cache] DROP COLUMN [_unique_version];
            IF COL_LENGTH('dbo._scheme_metadata_cache', '_lazy') IS NOT NULL ALTER TABLE [dbo].[_scheme_metadata_cache] DROP COLUMN [_lazy];
            IF COL_LENGTH('dbo._objects', '_value_unique') IS NOT NULL ALTER TABLE [dbo].[_objects] DROP COLUMN [_value_unique];
            IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'IX__objects__value_string')
                DROP INDEX [IX__objects__value_string] ON [dbo].[_objects];
            ALTER TABLE [dbo].[_objects] ALTER COLUMN [_value_string] NVARCHAR(MAX) NULL;
            """);
    }

    protected override async Task<bool> V4UpgradeDdlExistsAsync()
    {
        var present = await ScalarAdminAsync<object>(DatabaseName, """
            SELECT (SELECT COUNT(*) FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id
                    WHERE (t.name = '_values' AND c.name = '_unique')
                       OR (t.name = '_structures' AND c.name IN ('_unique', '_unique_version', '_lazy'))
                       OR (t.name = '_scheme_metadata_cache' AND c.name IN ('_unique', '_unique_version', '_lazy'))
                       OR (t.name = '_objects' AND c.name = '_value_unique'))
                 + (SELECT COUNT(*) FROM sys.indexes
                    WHERE name IN ('UIX__values__structure_unique', 'UIX__objects__scheme_unique', 'IX__objects__value_string'))
                 + (SELECT COUNT(*) FROM sys.columns
                    WHERE object_id = OBJECT_ID('dbo._objects') AND name = '_value_string' AND max_length = 900)
                 -- owner decision 2026-09-02: the unread full-text index must be GONE (counts as 0)
                 + (SELECT COUNT(*) FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID('dbo._values'))
            """);
        return Convert.ToInt64(present) == 12;
    }

    protected override async Task ApplyScriptAsOwnerAsync(IRedbContext ownerContext, string script)
    {
        // The same split the provider's start-up does: GO is a client-side batch separator.
        var batches = System.Text.RegularExpressions.Regex.Split(script, @"^\s*GO\s*$",
            System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (trimmed.Length > 0)
                await ownerContext.ExecuteAsync(trimmed);
        }
    }

    [Fact]
    public async Task Upgrade_RefusesToNarrowValueString_WhileLongValuesExist()
    {
        // Owner decision 2026-09-02: _value_string is an identifier column (450); long text belongs
        // in _note. A database already holding longer values must not be truncated silently - the
        // upgrade stops, names the offenders query, and changes nothing.
        await RecreateFreshDatabaseAsync();
        await using (var sp = Build(FreshDatabaseConnectionString, autoApply: true))
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

        // The pre-decision shape: the MAX column with one over-long value, module behind.
        await ExecAdminAsync(FreshDatabaseName, "DROP INDEX [IX__objects__value_string] ON [dbo].[_objects]");
        await ExecAdminAsync(FreshDatabaseName, "ALTER TABLE [dbo].[_objects] ALTER COLUMN [_value_string] NVARCHAR(MAX) NULL");
        await ExecAdminAsync(FreshDatabaseName, """
            INSERT INTO [dbo].[_objects] (_id, _id_scheme, _id_owner, _id_who_change, _name, _value_string)
            SELECT 900000001, (SELECT TOP 1 _id FROM [dbo].[_schemes] ORDER BY _id),
                   (SELECT TOP 1 _id FROM [dbo].[_users] ORDER BY _id),
                   (SELECT TOP 1 _id FROM [dbo].[_users] ORDER BY _id),
                   N'long-value-string', REPLICATE(N'x', 500)
            """);
        await ExecAdminAsync(FreshDatabaseName, """
            ALTER FUNCTION dbo.pvt_module_version() RETURNS nvarchar(50) WITH SCHEMABINDING
            AS BEGIN RETURN N'0.0.0-test'; END
            """);

        await using (var sp = Build(FreshDatabaseConnectionString, autoApply: true))
        {
            var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync();
            (await act.Should().ThrowAsync<Exception>("silent truncation is not an option"))
                .Which.ToString().Should().Contain("_value_string", "the refusal names the column and the way out");
        }

        // The way out the message names: move the text to _note, start again.
        await ExecAdminAsync(FreshDatabaseName,
            "UPDATE [dbo].[_objects] SET _note = _value_string, _value_string = NULL WHERE _id = 900000001");
        await using (var sp = Build(FreshDatabaseConnectionString, autoApply: true))
            await sp.GetRequiredService<IRedbService>().InitializeAsync();

        (await ScalarAdminAsync<int?>(FreshDatabaseName, "SELECT CAST(COL_LENGTH('dbo._objects', '_value_string') AS INT)"))
            .Should().Be(900, "the column is NVARCHAR(450) once the data allows it");
        (await ScalarAdminAsync<object>(FreshDatabaseName,
            "SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'IX__objects__value_string'"))
            .Should().NotBeNull("and the index the other providers always had exists now");
    }
}
