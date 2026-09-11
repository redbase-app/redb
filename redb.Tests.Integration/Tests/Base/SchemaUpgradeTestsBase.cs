using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Extensions;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Start-up against a database whose SQL module is behind this build — the situation every existing
/// installation is in on the first start after an upgrade — under the three policies that matter:
///
/// <list type="number">
///   <item>the connected role owns the schema: the module is applied automatically (the behaviour
///   every 3.x installation relies on);</item>
///   <item>the connected role may read and write but not change the schema — the DBA revoked owner
///   rights after installing: start-up must stop with <see cref="RedbSchemaOutdatedException"/>
///   naming the script, not with a driver error;</item>
///   <item><c>AutoApplyDatabaseUpgrades = false</c>: start-up stops with the same exception without
///   trying, whatever the rights.</item>
/// </list>
///
/// <para>
/// No shared fixture can model this — the shared test database is upgraded by the first fixture that
/// touches it. Each provider subclass therefore owns a database of its own (<c>redb_upgrade</c>),
/// created from the schema script, and a restricted login of its own. The database is kept between
/// runs; the tests reset its module version themselves.
/// </para>
/// </summary>
public abstract class SchemaUpgradeTestsBase : IAsyncLifetime
{
    /// <summary>Connection string of an owner/admin login to the upgrade database.</summary>
    protected abstract string OwnerConnectionString { get; }

    /// <summary>Connection string of a login that may read and write but not change the schema.</summary>
    protected abstract string RestrictedConnectionString { get; }

    /// <summary>Provider registration for <c>AddRedb</c>.</summary>
    protected abstract void UseProvider(RedbOptionsBuilder options, string connectionString);

    /// <summary>
    /// Creates the database (if absent) and the restricted login. Runs once per test class
    /// instance; idempotent. The schema itself is created by the first owner start-up below.
    /// </summary>
    protected abstract Task PrepareDatabaseAndLoginAsync();

    /// <summary>
    /// Grants the restricted login DML on every table and EXECUTE on every function — after the
    /// schema exists, which is why this is a second step. Idempotent.
    /// </summary>
    protected abstract Task GrantRestrictedAsync();

    /// <summary>Rewrites the module version function in the database to return the given string.</summary>
    protected abstract Task SetDeployedModuleVersionAsync(string version);

    /// <summary>Reads the module version the database currently reports.</summary>
    protected abstract Task<string> ReadDeployedModuleVersionAsync();

    /// <summary>Drops and re-creates a SEPARATE, completely empty database for the fresh-install test.</summary>
    protected abstract Task RecreateFreshDatabaseAsync();

    /// <summary>Owner connection string pointing at that fresh-install database.</summary>
    protected abstract string FreshDatabaseConnectionString { get; }

    private string? _requiredVersion;

    public async Task InitializeAsync()
    {
        await PrepareDatabaseAndLoginAsync();

        // Learn what this build requires by starting once as owner: after that the database is
        // current and the tests below roll it back deliberately.
        await using var sp = Build(OwnerConnectionString, autoApply: true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        _requiredVersion = await ReadDeployedModuleVersionAsync();
        _requiredVersion.Should().NotBeNullOrWhiteSpace();

        await GrantRestrictedAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    /// <summary>
    /// Registration entry point. Pro subclasses swap in <c>AddRedbPro</c>: the upgrade contract is
    /// the tier's, not the provider's, and Pro tiers used to opt out of the module deployment
    /// altogether - which left every existing database without the V4 columns.
    /// </summary>
    protected virtual void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    protected ServiceProvider Build(string connectionString, bool autoApply)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        AddRedbServices(services, options =>
        {
            UseProvider(options, connectionString);
            options.Configure(c =>
            {
                c.AutoApplyDatabaseUpgrades = autoApply;
                c.EnablePropsCache = false;
                // Own cache domain: this process also holds services on the shared test database.
                c.CacheDomain = "schema-upgrade";
            });
        });
        return services.BuildServiceProvider();
    }


    [Fact]
    public async Task FreshDatabase_InitializesFromScratch_AndRoundTrips()
    {
        // BR-5: the generated init ran the 28th migration's cache resync before the 29th file had
        // created the function - a TRULY fresh database died with 42883 mid-init, while every
        // lived-in database (these suites' databases included: they persist between runs) kept the
        // previous version of the function and never noticed. This is the fresh-install test that
        // was missing: an empty database, the full init, a working round trip.
        await RecreateFreshDatabaseAsync();

        await using var sp = Build(FreshDatabaseConnectionString, autoApply: true);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        await redb.SyncSchemeAsync<redb.Tests.Integration.Models.SimpleProps>();
        var id = await redb.SaveAsync(new redb.Core.Models.Entities.RedbObject<redb.Tests.Integration.Models.SimpleProps>
        {
            name = "fresh-install",
            Props = new redb.Tests.Integration.Models.SimpleProps { Title = "fresh", Count = 1 }
        });
        (await redb.LoadAsync<redb.Tests.Integration.Models.SimpleProps>(id, depth: 1))!
            .Props.Title.Should().Be("fresh");
    }
    [Fact]
    public async Task Owner_OutdatedModule_IsUpgradedAutomatically()
    {
        await SetDeployedModuleVersionAsync("0.0.0-test");

        await using var sp = Build(OwnerConnectionString, autoApply: true);
        await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

        (await ReadDeployedModuleVersionAsync()).Should().Be(_requiredVersion,
            "an owner start-up must bring the module to the version the build requires");
    }

    [Fact]
    public async Task RestrictedRole_OutdatedModule_StopsWithSchemaOutdated_NotDriverError()
    {
        await SetDeployedModuleVersionAsync("0.0.0-test");
        try
        {
            await using var sp = Build(RestrictedConnectionString, autoApply: true);
            var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

            var ex = (await act.Should().ThrowAsync<RedbSchemaOutdatedException>()).Which;
            ex.DeployedVersion.Should().Be("0.0.0-test");
            ex.RequiredVersion.Should().Be(_requiredVersion);
            ex.Cause.Should().NotBeNull("the privilege error is kept as the cause");
            ex.Message.Should().Contain("GetUpgradeScript");

            (await ReadDeployedModuleVersionAsync()).Should().Be("0.0.0-test",
                "nothing may have been changed by a role that is not allowed to change it");
        }
        finally
        {
            await RepairAsync();
        }
    }

    [Fact]
    public async Task AutoApplyDisabled_OutdatedModule_StopsWithoutTrying()
    {
        await SetDeployedModuleVersionAsync("0.0.0-test");
        try
        {
            await using var sp = Build(OwnerConnectionString, autoApply: false);
            var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

            var ex = (await act.Should().ThrowAsync<RedbSchemaOutdatedException>()).Which;
            ex.Cause.Should().BeNull("with automatic upgrades disabled nothing is attempted, so there is no cause");

            (await ReadDeployedModuleVersionAsync()).Should().Be("0.0.0-test",
                "the owner role could have upgraded, but was told not to");
        }
        finally
        {
            await RepairAsync();
        }
    }


    /// <summary>
    /// Drops every column and index the V4 "0. Schema upgrades" block adds - _values._unique,
    /// _structures._unique/_unique_version/_lazy, _scheme_metadata_cache._unique/_unique_version/_lazy,
    /// _objects._value_unique and the two partial unique indexes - as a pre-V4 database looks.
    /// </summary>
    protected abstract Task DropV4UpgradeDdlAsync();

    /// <summary>Whether all of the above exist again (eight columns, two indexes).</summary>
    protected abstract Task<bool> V4UpgradeDdlExistsAsync();

    [Fact]
    public async Task DroppedUpgradeColumn_IsRestored_ByNormalStart()
    {
        // A database created before the column existed: no column, and a module version behind the
        // build - exactly what every existing installation is after a package upgrade. The bundle's
        // "0. Schema upgrades" block must put the column and its index back before anything else runs.
        await DropV4UpgradeDdlAsync();
        await SetDeployedModuleVersionAsync("0.0.0-test");

        await using var sp = Build(OwnerConnectionString, autoApply: true);
        await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

        (await V4UpgradeDdlExistsAsync()).Should().BeTrue(
            "the schema-upgrades block of the bundle restores every V4 column and index on existing databases");
        (await ReadDeployedModuleVersionAsync()).Should().Be(_requiredVersion);
    }

    [Fact]
    public async Task UpgradeScript_IsTheBundle_AndAppliedByOwnerRepairsTheDatabase()
    {
        await SetDeployedModuleVersionAsync("0.0.0-test");

        await using var sp = Build(OwnerConnectionString, autoApply: false);
        var redb = sp.GetRequiredService<IRedbService>();
        var script = redb.GetUpgradeScript();
        script.Should().NotBeNullOrWhiteSpace("providers with a versioned module export their upgrade script");
        script!.Should().Contain("pvt_module_version", "the script ends by stamping the version");

        // What the DBA does with the exported text.
        var ctx = sp.GetRequiredService<IRedbContext>();
        await ApplyScriptAsOwnerAsync(ctx, script);

        (await ReadDeployedModuleVersionAsync()).Should().Be(_requiredVersion);
    }

    /// <summary>Applies a script the way the provider's start-up does (MSSQL splits by GO).</summary>
    protected abstract Task ApplyScriptAsOwnerAsync(IRedbContext ownerContext, string script);

    private async Task RepairAsync()
    {
        await using var sp = Build(OwnerConnectionString, autoApply: true);
        await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
    }
}
