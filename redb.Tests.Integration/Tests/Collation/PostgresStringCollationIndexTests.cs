using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Core.Pro.Extensions;
using redb.Postgres.Extensions;
using redb.Postgres.Pro.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// With <c>StringCollation</c> set, Pro with the planner on pushes an ignore-case search onto <c>_values</c> as
/// <c>(_String COLLATE "und-x-icu") ILIKE</c>, which the stock trigram index over bare <c>_String</c> cannot serve.
/// Pro start-up looks for a trigram index over the collated column and logs a Warning when there is none, or only an
/// INVALID one. The free forms filter the pivoted value and read no trigram index, so the free provider and Pro with
/// the planner off check nothing. The plan test pins why the index is wanted.
/// </summary>
/// <remarks>
/// The database of the fixture has ctype C, under which pg_trgm builds no trigram from Cyrillic: the plan test searches
/// a Latin pattern, the only kind a trigram index can serve there.
/// </remarks>
[Collection("PostgresCollation")]
public class PostgresStringCollationIndexTests
{
    private const string IndexName = "IX__values__String_pattern_und_x_icu";
    private const string WarningMark = "StringCollation is";
    private const string OptionMark = "CreateStringCollationIndex is on";

    private readonly PostgresCollationFixture _fixture;

    public PostgresStringCollationIndexTests(PostgresCollationFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Pro_MissingIndex_LogsAWarningNamingTheCollationTheIndexTheScriptAndTheCtype()
    {
        await DropIndexAsync();

        var warnings = await StartAsync(Tier.Pro, PostgresCollationFixture.Collation);

        warnings.Should().ContainSingle();
        warnings[0].Should().Contain("StringCollation is \"und-x-icu\"")
            .And.Contain("has no trigram index")
            .And.Contain("USING gin ((_String COLLATE \"und-x-icu\") gin_trgm_ops)")
            .And.Contain(IndexName)
            .And.Contain("string_collation_index.sql")
            .And.Contain("The database ctype is C")
            .And.Contain("ASCII letters only");
    }

    [Fact]
    public async Task Pro_InvalidIndex_LogsAWarningToBuildItAgain()
    {
        await CreateIndexAsync();
        // An interrupted CREATE INDEX CONCURRENTLY leaves the index INVALID; the catalog flag stands in for the
        // interruption (the test role is a superuser).
        await ExecuteAsync($"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '\"{IndexName}\"'::regclass");

        var warnings = await StartAsync(Tier.Pro, PostgresCollationFixture.Collation);

        warnings.Should().ContainSingle().Which.Should().Contain("INVALID");
        await DropIndexAsync();
    }

    [Fact]
    public async Task Pro_ValidIndex_NoWarning()
    {
        await CreateIndexAsync();

        (await StartAsync(Tier.Pro, PostgresCollationFixture.Collation)).Should().BeEmpty();
    }

    [Fact]
    public async Task Pro_IndexMatchedByStructure_NotByName()
    {
        await DropIndexAsync();
        await ExecuteAsync(
            "CREATE INDEX IF NOT EXISTS ix_values_trgm_by_hand ON _values USING gin ((_String COLLATE \"ru-x-icu\") gin_trgm_ops)");
        try
        {
            // An index over another collation serves no search of this one.
            (await StartAsync(Tier.Pro, PostgresCollationFixture.Collation)).Should().ContainSingle();

            await ExecuteAsync("DROP INDEX ix_values_trgm_by_hand");
            await ExecuteAsync(
                "CREATE INDEX ix_values_trgm_by_hand ON _values USING gin ((_String COLLATE \"und-x-icu\") gin_trgm_ops)");
            (await StartAsync(Tier.Pro, PostgresCollationFixture.Collation)).Should().BeEmpty();
        }
        finally
        {
            await ExecuteAsync("DROP INDEX IF EXISTS ix_values_trgm_by_hand");
        }
    }

    [Fact]
    public async Task Pro_NoCollation_NoCheck()
    {
        await DropIndexAsync();

        (await StartAsync(Tier.Pro, collation: null)).Should().BeEmpty();
    }

    [Fact]
    public async Task ProPlannerOff_NoCheck()
    {
        await DropIndexAsync();

        (await StartAsync(Tier.ProPlannerOff, PostgresCollationFixture.Collation)).Should().BeEmpty();
    }

    [Fact]
    public async Task Free_NoCheck()
    {
        await DropIndexAsync();

        (await StartAsync(Tier.Free, PostgresCollationFixture.Collation)).Should().BeEmpty();
    }

    [Fact]
    public async Task Pro_IgnoreCaseContains_StockIndexNeverServesIt_CollatedIndexDoes()
    {
        await SeedAsync();
        try
        {
            await AssertPlansAsync();
        }
        finally
        {
            await DeleteObjectsAsync();
        }
    }

    private async Task AssertPlansAsync()
    {
        await using var provider = BuildProvider(Tier.Pro, PostgresCollationFixture.Collation, capture: null);
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync();
        var sql = await redb.Query<CollationProps>()
            .Where(c => c.Text.Contains("hello", StringComparison.OrdinalIgnoreCase))
            .ToSqlStringAsync();
        sql.Should().Contain("(dv._string COLLATE \"und-x-icu\") ILIKE", sql);

        await DropIndexAsync();
        var without = await ExplainAsync(sql);
        without.Should().NotContain("IX__values__String_pattern", without);

        await CreateIndexAsync();
        var with = await ExplainAsync(sql);
        with.Should().Contain($"Bitmap Index Scan on \"{IndexName}\"", with);
    }

    // === CreateStringCollationIndex ===

    [Fact]
    public async Task Pro_OptIn_BuildsTheIndexOnce_ASecondStartBuildsNothing()
    {
        await DropIndexAsync();

        var first = await StartCapturingAsync(Tier.Pro, PostgresCollationFixture.Collation, create: true);
        IndexWarnings(first).Should().BeEmpty();
        first.Entries.Should().Contain(e => e.Message.Contains($"{IndexName} built"));
        (await IndexValidAsync()).Should().BeTrue();

        var second = await StartCapturingAsync(Tier.Pro, PostgresCollationFixture.Collation, create: true);
        IndexWarnings(second).Should().BeEmpty();
        second.Entries.Should().NotContain(e => e.Message.Contains("Building") && e.Message.Contains(IndexName));
    }

    [Fact]
    public async Task Pro_OptIn_RebuildsAnInvalidLeftover()
    {
        await CreateIndexAsync();
        await ExecuteAsync($"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '\"{IndexName}\"'::regclass");

        var capture = await StartCapturingAsync(Tier.Pro, PostgresCollationFixture.Collation, create: true);

        IndexWarnings(capture).Should().BeEmpty();
        (await IndexValidAsync()).Should().BeTrue();
    }

    [Fact]
    public async Task Pro_OptIn_AutoApplyOff_WarnsAndBuildsNothing()
    {
        await DropIndexAsync();

        var capture = await StartCapturingAsync(Tier.Pro, PostgresCollationFixture.Collation, create: true, autoApply: false);

        IndexWarnings(capture).Should().ContainSingle().Which.Should().Contain("AutoApplyDatabaseUpgrades is off");
        (await IndexValidAsync()).Should().BeNull();
    }

    [Fact]
    public async Task Free_OptIn_WarnsNoEffect_BuildsNothing()
    {
        await DropIndexAsync();

        var capture = await StartCapturingAsync(Tier.Free, PostgresCollationFixture.Collation, create: true);

        IndexWarnings(capture).Should().ContainSingle().Which.Should().Contain("no effect on this tier (free provider)");
        (await IndexValidAsync()).Should().BeNull();
    }

    [Fact]
    public async Task ProPlannerOff_OptIn_WarnsNoEffect_BuildsNothing()
    {
        await DropIndexAsync();

        var capture = await StartCapturingAsync(Tier.ProPlannerOff, PostgresCollationFixture.Collation, create: true);

        IndexWarnings(capture).Should().ContainSingle().Which.Should().Contain("no effect on this tier (Pro with EnablePvtPrefilter off)");
        (await IndexValidAsync()).Should().BeNull();
    }

    [Fact]
    public async Task OptIn_WithoutCollation_Warns()
    {
        var capture = await StartCapturingAsync(Tier.Pro, collation: null, create: true);

        IndexWarnings(capture).Should().ContainSingle().Which.Should().Contain("StringCollation is not set");
    }

    // === helpers ===

    private Task<bool?> IndexValidAsync() => _fixture.Redb.Context.ExecuteScalarAsync<bool?>(
        "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
        $"WHERE c.relname = '{IndexName}' AND c.relnamespace = current_schema()::regnamespace");

    private enum Tier { Free, Pro, ProPlannerOff }

    /// <summary>
    /// Two rows to find among a few thousand of the same field: enough for the planner to prefer a selective trigram
    /// index over the index of the structure, which it takes on a handful of rows whatever the trigram index.
    /// </summary>
    private async Task SeedAsync()
    {
        await DeleteObjectsAsync();
        var objects = Enumerable.Range(1, 4000)
            .Select(i => new RedbObject<CollationProps>
            {
                name = "noise-" + i,
                Props = new CollationProps { Text = $"Order {i} shipped {Guid.NewGuid():N}", Label = "noise-" + i }
            })
            .ToList();
        objects.Add(new() { name = "ci-1", Props = new CollationProps { Text = "ПРИВЕТ МИР", Label = "ci-1" } });
        objects.Add(new() { name = "ci-2", Props = new CollationProps { Text = "HELLO WORLD", Label = "ci-2" } });
        await _fixture.Redb.SaveAsync(objects);
        // Both tables: with stale statistics of _objects the planner takes the scheme for one object and drives the
        // plan from _objects, reading each value by object instead of searching the values.
        await ExecuteAsync("ANALYZE _objects");
        await ExecuteAsync("ANALYZE _values");
    }

    private Task DeleteObjectsAsync() => ExecuteAsync(
        "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = " +
        $"'{typeof(CollationProps).FullName}')");

    /// <summary>
    /// EXPLAIN of a Pro preview on a connection of its own, with sequential scans off: the plan shows whether an index
    /// CAN serve the predicate, where on a handful of rows the planner would read the table either way. The parameters
    /// come from the header the preview writes (<c>-- $n=value (Type)</c>).
    /// </summary>
    private static async Task<string> ExplainAsync(string preview)
    {
        await using var connection = new NpgsqlConnection(PostgresCollationFixture.ConnectionString);
        await connection.OpenAsync();
        await using (var off = new NpgsqlCommand("SET enable_seqscan = off", connection))
            await off.ExecuteNonQueryAsync();

        await using var explain = new NpgsqlCommand("EXPLAIN " + preview, connection);
        foreach (Match m in Regex.Matches(preview, @"^-- \$(\d+)=(.*) \((\w+)\)\r?$", RegexOptions.Multiline))
        {
            var raw = m.Groups[2].Value;
            object value = m.Groups[3].Value switch
            {
                "Int64" => long.Parse(raw, System.Globalization.CultureInfo.InvariantCulture),
                "String" => raw.Substring(1, raw.Length - 2).Replace("''", "'"),
                var type => throw new InvalidOperationException($"Parameter type {type} is not handled by the test.")
            };
            explain.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await using var reader = await explain.ExecuteReaderAsync();
        var lines = new List<string>();
        while (await reader.ReadAsync())
            lines.Add(reader.GetString(0));
        return string.Join("\n", lines);
    }

    private Task CreateIndexAsync() => ExecuteAsync(
        $"CREATE INDEX IF NOT EXISTS \"{IndexName}\" ON _values USING gin ((_String COLLATE \"und-x-icu\") gin_trgm_ops) " +
        "WHERE _String IS NOT NULL");

    private Task DropIndexAsync() => ExecuteAsync($"DROP INDEX IF EXISTS \"{IndexName}\"");

    private Task ExecuteAsync(string sql) => _fixture.Redb.Context.ExecuteAsync(sql);

    /// <summary>Starts a service of its own on the collation database and returns the Warnings about the index.</summary>
    private static async Task<IReadOnlyList<string>> StartAsync(Tier tier, string? collation)
        => IndexWarnings(await StartCapturingAsync(tier, collation));

    private static IReadOnlyList<string> IndexWarnings(CapturingLoggerProvider capture)
        => capture.Warnings.Where(w => w.Contains(WarningMark) || w.Contains(OptionMark)).ToList();

    private static async Task<CapturingLoggerProvider> StartCapturingAsync(Tier tier, string? collation,
        bool create = false, bool autoApply = true)
    {
        var capture = new CapturingLoggerProvider();
        await using (var provider = BuildProvider(tier, collation, capture, create, autoApply))
            await provider.GetRequiredService<IRedbService>().InitializeAsync();
        return capture;
    }

    private static ServiceProvider BuildProvider(Tier tier, string? collation, CapturingLoggerProvider? capture,
        bool create = false, bool autoApply = true)
    {
        var services = new ServiceCollection();
        services.AddLogging(b =>
        {
            if (capture != null) b.AddProvider(capture);
            b.SetMinimumLevel(LogLevel.Information);
        });

        void Configure(RedbServiceConfiguration c)
        {
            c.EnablePropsCache = false;
            c.StringCollation = collation;
            c.EnablePvtPrefilter = tier != Tier.ProPlannerOff;
            c.CreateStringCollationIndex = create;
            c.AutoApplyDatabaseUpgrades = autoApply;
            c.CacheDomain = tier == Tier.Free ? "collation-c" : "collation-c-pro";
        }

        if (tier == Tier.Free)
        {
            services.AddRedb(options =>
                PostgresOptionsExtensions.UsePostgres(options, PostgresCollationFixture.ConnectionString).Configure(Configure));
        }
        else
        {
            var license = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build()["Redb:License"];
            services.AddRedbPro(options =>
            {
                PostgresProOptionsExtensions.UsePostgres(options, PostgresCollationFixture.ConnectionString).Configure(Configure);
                if (!string.IsNullOrWhiteSpace(license))
                    options.WithLicense(license);
            });
        }

        return services.BuildServiceProvider();
    }
}
