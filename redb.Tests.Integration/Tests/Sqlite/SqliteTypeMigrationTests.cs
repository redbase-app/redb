using redb.Core;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// GitHub #5 — SQLite has no stored functions, so the base scheme-sync path that runs
/// <c>SELECT * FROM migrate_structure_type(...)</c> failed with
/// <c>no such table: migrate_structure_type</c>, taking down <c>InitializeAsync</c> on any property
/// type change. <see cref="redb.SQLite.Providers.SqliteSchemeSyncProvider"/> now handles the migration
/// in C#. These assert the missing-function crash is gone on both Free and Pro SQLite.
/// </summary>
public abstract class SqliteTypeMigrationTestsBase
{
    protected readonly IRedbService Redb;
    protected SqliteTypeMigrationTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task MigrateStructureType_SameColumn_ReturnsResult_NotMissingTable()
    {
        // Int and Long both store in _Long — a no-op — but the old path emitted the missing SQL function
        // and threw "no such table: migrate_structure_type".
        var res = await Redb.MigrateStructureTypeAsync(999_999_999, "Int", "Long", dryRun: false);

        res.Should().NotBeNull();
        res.AffectedRows.Should().Be(0);
    }

    [Fact]
    public async Task MigrateStructureType_CrossColumnNoData_ReturnsResult_NotMissingTable()
    {
        // String -> ListItem is a cross-column change, but with no values under this (non-existent)
        // structure there is nothing to move — must return cleanly, never hit the missing function.
        var res = await Redb.MigrateStructureTypeAsync(999_999_999, "String", "ListItem", dryRun: false);

        res.Should().NotBeNull();
        res.AffectedRows.Should().Be(0);
    }
}

[Collection("Sqlite")]
public class SqliteTypeMigrationTests : SqliteTypeMigrationTestsBase
{
    public SqliteTypeMigrationTests(SqliteFixture fixture) : base(fixture.Redb) { }
}

[Collection("SqlitePro")]
public class SqliteProTypeMigrationTests : SqliteTypeMigrationTestsBase
{
    public SqliteProTypeMigrationTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
