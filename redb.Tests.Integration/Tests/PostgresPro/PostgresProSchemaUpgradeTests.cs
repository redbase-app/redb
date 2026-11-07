using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

/// <summary>
/// The same upgrade contract as the Free suite, but started through <c>AddRedbPro</c>: an existing
/// 3.x database must gain the V4 columns on the first start of a Pro build too. It did not - the Pro
/// service skipped the module deployment entirely ("Pro uses its own C# PVT generator"), which is
/// true of the module's FUNCTIONS but not of the DDL that ships in the same bundle, so
/// <c>_values._unique</c> and its siblings were never created and start-up died on the first read of
/// a scheme. Owns its own database and role: this suite deliberately damages the schema.
/// </summary>
public class PostgresProSchemaUpgradeTests : redb.Tests.Integration.Tests.Postgres.PostgresSchemaUpgradeTests
{
    protected override string DatabaseName => "redb_upgrade_pro";
    protected override string FreshDatabaseName => "redb_upgrade_pro_fresh";
    protected override string RestrictedRole => "redb_noddl_pro";

    protected override void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    // Resolves to the Pro overload through this file's using of redb.Postgres.Pro.Extensions.
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UsePostgres(connectionString);
}
