using redb.Tests.Integration.Fixtures;
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

    /// <summary>
    /// Rewrites the module compatibility function to return the given value; null drops the function, as a module of
    /// a version before it has none.
    /// </summary>
    protected abstract Task SetDeployedModuleCompatAsync(string? compat);

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

    protected ServiceProvider Build(string connectionString, bool autoApply, string cacheDomain = "schema-upgrade",
        ILoggerProvider? logs = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b =>
        {
            b.SetMinimumLevel(LogLevel.Warning);
            if (logs != null)
                b.AddProvider(logs);
        });
        AddRedbServices(services, options =>
        {
            UseProvider(options, connectionString);
            options.Configure(c =>
            {
                c.AutoApplyDatabaseUpgrades = autoApply;
                c.EnablePropsCache = false;
                // Own cache domain: this process also holds services on the shared test database.
                c.CacheDomain = cacheDomain;
            });
        });
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Starts <paramref name="nodes"/> services at once against one database, each with a cache domain of its own,
    /// as the nodes of a cluster are separate processes. Returns the failures.
    /// </summary>
    private async Task<List<Exception>> StartTogetherAsync(string connectionString, int nodes, bool ensureCreated)
    {
        var providers = Enumerable.Range(0, nodes)
            .Select(i => Build(connectionString, autoApply: true, cacheDomain: $"schema-upgrade-node{i}-{Guid.NewGuid():N}"))
            .ToList();
        try
        {
            var starts = providers.Select(sp => Task.Run(async () =>
            {
                try
                {
                    await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated);
                    return null;
                }
                catch (Exception ex)
                {
                    return ex;
                }
            }));
            return (await Task.WhenAll(starts)).Where(e => e != null).Select(e => e!).ToList();
        }
        finally
        {
            foreach (var sp in providers)
                await sp.DisposeAsync();
        }
    }

    [Fact]
    public async Task Owner_OutdatedModule_NodesStartingTogether_AllStart()
    {
        // FOREIGN-BUGS 7 of the AS4 stand: the panel and the worker of a Tsak cluster both met a database one module
        // behind, both applied the bundle, and the second died on "index already exists". The schema lock makes them
        // take turns; the one that waited finds the module deployed.
        for (var round = 0; round < 5; round++)
        {
            await SetDeployedModuleVersionAsync("0.0.0-test");

            var failures = await StartTogetherAsync(OwnerConnectionString, nodes: 4, ensureCreated: false);

            failures.Should().BeEmpty($"every node starts (round {round})");
            (await ReadDeployedModuleVersionAsync()).Should().Be(_requiredVersion);
        }
    }

    [Fact]
    public async Task FreshDatabase_NodesCreatingTheSchemaTogether_AllStart()
    {
        // Nodes starting together against an empty database: one creates the schema, the others find it created.
        // The scheme synchronization that follows runs under the same lock, so they do not insert the same schemes.
        for (var round = 0; round < 3; round++)
        {
            await RecreateFreshDatabaseAsync();

            var failures = await StartTogetherAsync(FreshDatabaseConnectionString, nodes: 3, ensureCreated: true);

            failures.Should().BeEmpty($"every node starts (round {round})");
        }
    }

    [Fact]
    public async Task RestrictedRole_DeployedDatabase_StartsWithoutSchemaWork()
    {
        // A database in step: tables there, module current, schemes synced by the owner in InitializeAsync above. A
        // role that may not change the schema starts, since nothing is left to change and no lock is taken.
        await using var sp = Build(RestrictedConnectionString, autoApply: true, cacheDomain: $"restricted-{Guid.NewGuid():N}");
        await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
    }

    [Fact]
    public async Task Scheme_NodesSyncingItTogether_AllSucceed()
    {
        // Runtime scheme sync (outside InitializeAsync): the workers of a Tsak cluster load modules sharing a Props
        // class, or a module version adding a field reaches every node at once. The sync that has to write takes the
        // schema lock; the nodes that waited find the scheme in step.
        var providers = Enumerable.Range(0, 4)
            .Select(i => Build(OwnerConnectionString, autoApply: true, cacheDomain: $"scheme-sync-node{i}-{Guid.NewGuid():N}"))
            .ToList();
        try
        {
            var nodes = new List<IRedbService>();
            foreach (var sp in providers)
            {
                var redb = sp.GetRequiredService<IRedbService>();
                await redb.InitializeAsync(ensureCreated: true);
                nodes.Add(redb);
            }

            var failures = await SchemeSyncRace.RunAsync(nodes, providers[0].GetRequiredService<IRedbContext>(), rounds: 8);
            failures.Should().BeEmpty();
        }
        finally
        {
            foreach (var sp in providers)
                await sp.DisposeAsync();
        }
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

    /// <summary>
    /// A database whose _types say String for TimeOnly and TimeSpan (created before they moved to Ticks in _Long) says
    /// Long after the module upgrade, in _types and in the metadata cache, so it writes their Ticks. Stored values are
    /// not converted; the module only changes the type rows.
    /// </summary>
    [Fact]
    public async Task Owner_OutdatedModule_SetsLongForTimeOnlyAndTimeSpan()
    {
        const string ids = "(-9223372036854775685, -9223372036854775684)";
        await using (var sp = Build(OwnerConnectionString, autoApply: true))
        {
            var ctx = sp.GetRequiredService<IRedbContext>();
            await ctx.ExecuteAsync($"UPDATE _types SET _db_type = 'String' WHERE _id IN {ids}");
            await ctx.ExecuteAsync($"UPDATE _scheme_metadata_cache SET db_type = 'String' WHERE _type_id IN {ids}");
        }
        await SetDeployedModuleVersionAsync("0.0.0-test");

        await using (var sp = Build(OwnerConnectionString, autoApply: true))
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            var ctx = sp.GetRequiredService<IRedbContext>();
            (await ctx.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _types WHERE _id IN {ids} AND _db_type = 'Long'"))
                .Should().Be(2, "the module upgrade sets Long for TimeOnly and TimeSpan");
            (await ctx.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _scheme_metadata_cache WHERE _type_id IN {ids} AND db_type <> 'Long'"))
                .Should().Be(0, "and the copy in the metadata cache");
        }
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

    // ===== A module newer than the package: never put back =====

    /// <summary>A version above any real one, new per test: the warning is written once per process and version.</summary>
    private static string NewerVersion() => $"99.0.{Random.Shared.Next(1, 1_000_000)}";

    /// <summary>After a newer-module test: the module marked old again, so a start puts the package's module back.</summary>
    private async Task RepairFromNewerAsync()
    {
        await SetDeployedModuleVersionAsync("0.0.0-test");
        await RepairAsync();
    }

    [Fact]
    public async Task NewerModule_ThatServesThisPackage_IsLeftInPlace_AndTheStartGoesOn()
    {
        var newer = NewerVersion();
        await SetDeployedModuleCompatAsync($"{_requiredVersion}|9.9.0");
        await SetDeployedModuleVersionAsync(newer);
        try
        {
            var logs = new CapturingLoggerProvider();
            await using (var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"newer-{Guid.NewGuid():N}", logs: logs))
                await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

            (await ReadDeployedModuleVersionAsync()).Should().Be(newer, "a package never puts its older module over a newer one");
            logs.Warnings.Should().ContainSingle(w => w.Contains(newer) && w.Contains("left as it is"));

            // The same with automatic upgrades off: a newer module that serves the package is no reason to stop.
            await using (var sp = Build(OwnerConnectionString, autoApply: false, cacheDomain: $"newer-{Guid.NewGuid():N}"))
                await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            (await ReadDeployedModuleVersionAsync()).Should().Be(newer);
        }
        finally
        {
            await RepairFromNewerAsync();
        }
    }

    [Fact]
    public async Task NewerModule_ThatNoLongerServesThisPackage_StopsTheStart_AndChangesNothing()
    {
        var newer = NewerVersion();
        await SetDeployedModuleCompatAsync($"{newer}|9.9.0");
        await SetDeployedModuleVersionAsync(newer);
        try
        {
            await using var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"newer-{Guid.NewGuid():N}");
            var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

            var ex = (await act.Should().ThrowAsync<RedbSchemaNewerException>()).Which;
            ex.DeployedVersion.Should().Be(newer);
            ex.PackageVersion.Should().Be(_requiredVersion);
            ex.RequiredPackageVersion.Should().Be("9.9.0");
            ex.Message.Should().Contain("from 9.9.0 on").And.Contain("Upgrade the redb packages")
                .And.Contain("the database was left unchanged");

            (await ReadDeployedModuleVersionAsync()).Should().Be(newer);
        }
        finally
        {
            await RepairFromNewerAsync();
        }
    }

    [Fact]
    public async Task NewerModule_WithoutTheCompatibilityFunction_StopsTheStart_AndChangesNothing()
    {
        var newer = NewerVersion();
        await SetDeployedModuleCompatAsync(null);
        await SetDeployedModuleVersionAsync(newer);
        try
        {
            await using var sp = Build(OwnerConnectionString, autoApply: true, cacheDomain: $"newer-{Guid.NewGuid():N}");
            var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

            var ex = (await act.Should().ThrowAsync<RedbSchemaNewerException>()).Which;
            ex.RequiredPackageVersion.Should().BeNull("the module states nothing, and no version is guessed");
            ex.Message.Should().Contain("does not state which packages it serves");

            (await ReadDeployedModuleVersionAsync()).Should().Be(newer);
        }
        finally
        {
            await RepairFromNewerAsync();
        }
    }

    /// <summary>After a script the guard refused: drops what the refusal may have left on pooled connections.</summary>
    protected virtual Task AfterRefusedScriptAsync() => Task.CompletedTask;

    /// <summary>
    /// What a second run must not change: the foreign keys, the identity sequence, the triggers, the routines and the
    /// rows of the core tables, as one comparable string.
    /// </summary>
    protected abstract Task<string> ReadSchemaFingerprintAsync();

    /// <summary>
    /// Runs a script the way a person does: sqlcmd and SSMS go on to the next batch after an error, psql without
    /// ON_ERROR_STOP to the next statement. Returns the error messages.
    /// </summary>
    protected abstract Task<List<string>> RunScriptByHandAsync(string script);

    private const string AlreadyDeployed = "already holds the redb schema";

    [Fact]
    public async Task CreationScript_RunAgainOnADeployedDatabase_StopsAtTheGuard_AndChangesNothing()
    {
        await using var sp = Build(OwnerConnectionString, autoApply: false, cacheDomain: $"recreate-{Guid.NewGuid():N}");
        var redb = sp.GetRequiredService<IRedbService>();
        var script = redb.GetSchemaScript();
        var before = await ReadSchemaFingerprintAsync();

        // The way the services run it: the service's own ExecuteSchemaScriptAsync, on the service's context.
        var execute = typeof(RedbServiceBase).GetMethod("ExecuteSchemaScriptAsync",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var act = async () => await (Task)execute.Invoke(redb, [script])!;
        (await act.Should().ThrowAsync<Exception>()).Which.Message.Should().Contain(AlreadyDeployed);
        (await ReadSchemaFingerprintAsync()).Should().Be(before,
            "the script used to drop every foreign key and the identity sequence before it failed");

        // The service goes on working on its own context, with no pool cleared by the test: on SQL Server the guard's
        // NOEXEC stayed on the session the context holds, and every later query answered nothing.
        (await sp.GetRequiredService<IRedbContext>().ExecuteScalarAsync<long>("SELECT COUNT(*) FROM _users"))
            .Should().BeGreaterThan(0, "the context runs commands again after the refused script");

        // The way a person runs it.
        var errors = await RunScriptByHandAsync(script);
        errors.Should().Contain(e => e.Contains(AlreadyDeployed));
        await AfterRefusedScriptAsync();
        (await ReadSchemaFingerprintAsync()).Should().Be(before, "the guard stops the batches after it too");
    }

    [Fact]
    public async Task UpgradeScript_RunByHandOverANewerModule_IsRefused_AndChangesNothing()
    {
        // The script of this package is the "older" one here: the database carries a forged newer module. Start-up
        // never applies the script then; a DBA running it by hand must be stopped by the script itself before its
        // first change. The V4 columns are dropped first: the script's opening block would put them back.
        var newer = NewerVersion();
        await DropV4UpgradeDdlAsync();
        await SetDeployedModuleCompatAsync($"{newer}|9.9.0");
        await SetDeployedModuleVersionAsync(newer);
        try
        {
            await using var sp = Build(OwnerConnectionString, autoApply: false, cacheDomain: $"newer-{Guid.NewGuid():N}");
            var script = sp.GetRequiredService<IRedbService>().GetUpgradeScript();
            script.Should().NotBeNullOrWhiteSpace();

            var act = async () => await ApplyScriptAsOwnerAsync(sp.GetRequiredService<IRedbContext>(), script!);
            (await act.Should().ThrowAsync<Exception>()).Which.Message.Should().Contain(
                $"carries the redb module {newer}, newer than {_requiredVersion} of this package, and it needs redb " +
                "packages from 9.9.0 on. Upgrade the redb packages of this application; the database was left unchanged.");
            await AfterRefusedScriptAsync();

            (await ReadDeployedModuleVersionAsync()).Should().Be(newer, "the script stopped before it dropped the module");
            (await V4UpgradeDdlExistsAsync()).Should().BeFalse("the script stopped before its first change");
        }
        finally
        {
            await RepairFromNewerAsync();
        }
    }
}
