using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.MSSql.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

/// <summary>
/// MSSQL Pro never opted out of the module deployment the way the PostgreSQL and SQLite Pro
/// services did, so this suite starts green - it exists to keep it that way: the upgrade contract
/// belongs to every tier, and the two providers that skipped it left existing databases without the
/// V4 columns. Owns its own database and login.
/// </summary>
public class MsSqlProSchemaUpgradeTests : redb.Tests.Integration.Tests.MsSql.MsSqlSchemaUpgradeTests
{
    protected override string DatabaseName => "redb_upgrade_pro";
    protected override string FreshDatabaseName => "redb_upgrade_pro_fresh";
    protected override string RestrictedLogin => "redb_noddl_pro";

    protected override void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    // Resolves to the Pro overload through this file's using of redb.MSSql.Pro.Extensions.
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UseMsSql(connectionString);
}
