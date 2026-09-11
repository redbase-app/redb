using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Core.Pro.Extensions;
using redb.SQLite.Data;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// V4: the 128-bit hashes in SQLite — <c>_objects._hash</c>, <c>_schemes._structure_hash</c>,
/// <c>_users._hash</c> — are stored as <c>BLOB(16)</c> in RFC 4122 byte order, not as 36-character
/// TEXT (see <see cref="SqliteHash"/>). Three things have to hold, on Free (native extension writes
/// and reads the hash in its own statements) and on Pro (pure C#) alike:
///
/// <list type="number">
///   <item>the byte order is the text order — one Guid, written from C#, reads back as the same Guid,
///   and <c>hex(_hash)</c> in SQL spells the Guid's canonical text;</item>
///   <item>a database created before V4, holding the hashes as TEXT, is converted on the next open,
///   driven by opening it and nothing else;</item>
///   <item>a query filtering by hash still finds the object — the uuid TEXT the filter carries is
///   converted on the value side, in the extension for Free and in the SQL builder for Pro.</item>
/// </list>
///
/// <para>
/// Each test owns a temporary file and opens it as many times as it needs: the shared fixtures delete
/// their database on every run and can model a new database only.
/// </para>
/// </summary>
public class SqliteHashStorageTests : IDisposable
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"redb_hashblob_{Guid.NewGuid():N}.db");

    private string ConnectionString => $"Data Source={_dbPath}";

    private static ServiceProvider BuildServices(string connectionString, bool pro)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();

        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        if (pro)
        {
            var license = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build()["Redb:License"];
            services.AddRedbPro(options =>
            {
                redb.SQLite.Pro.Extensions.SqliteProOptionsExtensions.UseSqlite(options, connectionString);
                if (!string.IsNullOrWhiteSpace(license))
                    options.WithLicense(license);
            });
        }
        else
        {
            services.AddRedb(options => redb.SQLite.Extensions.SqliteOptionsExtensions.UseSqlite(options, connectionString));
        }
        return services.BuildServiceProvider();
    }

    private static string CanonicalHex(Guid g) => g.ToString("N").ToUpperInvariant();

    /// <summary>No database: the helper alone. The bytes are the text, left to right.</summary>
    [Fact]
    public void ByteOrder_IsTheTextOrder()
    {
        var g = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

        var blob = SqliteHash.ToBlob(g);
        Convert.ToHexString(blob).Should().Be("00112233445566778899AABBCCDDEEFF",
            "Guid.ToByteArray() would give 33221100-5544-7766-…; the stored form is the text order");

        SqliteHash.FromBlob(blob).Should().Be(g);
        SqliteHash.FromDbValue(blob).Should().Be(g);
        SqliteHash.FromDbValue(g.ToString()).Should().Be(g, "a TEXT value (data guid, or a hash not yet converted) still reads");
        SqliteHash.FromText("$1").Should().Be("unhex(replace($1,'-',''))");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NewDatabase_StoresHashesAsBlob_InTextOrder(bool pro)
    {
        await using var sp = BuildServices(ConnectionString, pro);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        var obj = new RedbObject<ProjectMetricsProps>
        {
            name = "hash-blob",
            Props = new ProjectMetricsProps { ProjectId = 1, TasksTotal = 5, TasksCompleted = 2 }
        };
        var id = await redb.SaveAsync(obj);

        var loaded = await redb.LoadAsync<ProjectMetricsProps>(id, depth: 1);
        loaded!.hash.Should().NotBeNull();

        var ctx = sp.GetRequiredService<IRedbContext>();
        (await ctx.ExecuteScalarAsync<string>($"SELECT typeof(_hash) FROM _objects WHERE _id = {id}"))
            .Should().Be("blob");
        (await ctx.ExecuteScalarAsync<string>($"SELECT hex(_hash) FROM _objects WHERE _id = {id}"))
            .Should().Be(CanonicalHex(loaded.hash!.Value), "hex() of the stored bytes spells the Guid's text");

        (await ctx.ExecuteScalarAsync<string>($"SELECT typeof(_structure_hash) FROM _schemes WHERE _id = {loaded.scheme_id}"))
            .Should().Be("blob");

        // The stored hash is the one the object computes for itself — the round trip is lossless.
        var live = loaded.ComputeHash();
        loaded.hash.Should().Be(live);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LegacyTextHashes_AreConvertedOnNextOpen(bool pro)
    {
        long id;
        long schemeId;
        Guid savedHash;

        // First open: create and save, then roll the storage back to what a pre-V4 build wrote —
        // the same value as 36-character lower-case TEXT.
        await using (var sp = BuildServices(ConnectionString, pro))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            id = await redb.SaveAsync(new RedbObject<ProjectMetricsProps>
            {
                name = "legacy",
                Props = new ProjectMetricsProps { ProjectId = 2, TasksTotal = 8, TasksCompleted = 1 }
            });
            var loaded = (await redb.LoadAsync<ProjectMetricsProps>(id, depth: 1))!;
            savedHash = loaded.hash!.Value;
            schemeId = loaded.scheme_id;

            var ctx = sp.GetRequiredService<IRedbContext>();
            foreach (var (table, column) in new[] { ("_objects", "_hash"), ("_schemes", "_structure_hash"), ("_users", "_hash") })
            {
                await ctx.ExecuteAsync(
                    $"UPDATE {table} SET {column} = lower(substr(hex({column}),1,8)||'-'||substr(hex({column}),9,4)||'-'||" +
                    $"substr(hex({column}),13,4)||'-'||substr(hex({column}),17,4)||'-'||substr(hex({column}),21,12)) " +
                    $"WHERE typeof({column}) = 'blob'");
            }

            // A pre-V4 file carries no schema stamp: PRAGMA user_version is 0 there.
            await ctx.ExecuteAsync("PRAGMA user_version = 0");

            (await ctx.ExecuteScalarAsync<string>($"SELECT typeof(_hash) FROM _objects WHERE _id = {id}"))
                .Should().Be("text", "the legacy state must be staged");
            (await ctx.ExecuteScalarAsync<string>($"SELECT _hash FROM _objects WHERE _id = {id}"))
                .Should().Be(savedHash.ToString(), "staged as the canonical text a pre-V4 build wrote");
        }

        // Second open: the same file, a new service — the upgrade path.
        await using (var sp = BuildServices(ConnectionString, pro))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);

            var ctx = sp.GetRequiredService<IRedbContext>();
            (await ctx.ExecuteScalarAsync<string>($"SELECT typeof(_hash) FROM _objects WHERE _id = {id}"))
                .Should().Be("blob", "opening an existing database must convert the hashes");
            (await ctx.ExecuteScalarAsync<string>($"SELECT hex(_hash) FROM _objects WHERE _id = {id}"))
                .Should().Be(CanonicalHex(savedHash), "the conversion keeps the value: same text, now as bytes");
            (await ctx.ExecuteScalarAsync<string>($"SELECT typeof(_structure_hash) FROM _schemes WHERE _id = {schemeId}"))
                .Should().Be("blob");
            (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().BeGreaterThan(0,
                "the pass stamps the file so the next open skips it instead of scanning _objects again");

            var loaded = await redb.LoadAsync<ProjectMetricsProps>(id, depth: 1);
            loaded!.hash.Should().Be(savedHash);
            loaded.Props.TasksTotal.Should().Be(8);
        }

        // Third open: nothing left to convert — the step is a no-op, not a rewrite.
        await using (var sp = BuildServices(ConnectionString, pro))
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            (await redb.LoadAsync<ProjectMetricsProps>(id, depth: 1))!.hash.Should().Be(savedHash);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FilterByHash_FindsTheObject(bool pro)
    {
        await using var sp = BuildServices(ConnectionString, pro);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        var ids = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            ids.Add(await redb.SaveAsync(new RedbObject<ProjectMetricsProps>
            {
                name = $"filter-{i}",
                Props = new ProjectMetricsProps { ProjectId = 10 + i, TasksTotal = 3 + i, TasksCompleted = i }
            }));
        }
        var wanted = (await redb.LoadAsync<ProjectMetricsProps>(ids[1], depth: 1))!.hash!.Value;

        var found = await redb.Query<ProjectMetricsProps>()
            .WhereRedb(o => o.Hash == wanted)
            .ToListAsync();

        found.Should().ContainSingle("the uuid text of the filter is converted to the stored bytes on the value side")
            .Which.id.Should().Be(ids[1]);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
        {
            try { File.Delete(_dbPath + suffix); } catch { /* best effort */ }
        }
    }
}
