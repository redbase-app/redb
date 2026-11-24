using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Export.Providers;
using redb.Export.Services;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// redb.Export loads PostgreSQL with the foreign key triggers off, because the file order needs it: roles point at
/// objects that come later, children may come before their parents. Switching the triggers back on checks nothing,
/// so a file with a reference to a row it does not hold loaded silently, while SQL Server refused the same file.
/// The import now checks every key of the redb tables at the end and refuses such a file by name. Each test runs on
/// scratch databases of its own, dropped afterwards.
/// </summary>
public class PostgresExportForeignKeyTests : IAsyncLifetime
{
    private const long Missing = 999_999_999;

    private string _server = null!;
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    public Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        _server = config.GetConnectionString("Postgres")!;
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        NpgsqlConnection.ClearAllPools();
        foreach (var database in _databases)
            await ExecuteAsync(Maintenance(), $"DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)");
        foreach (var file in _files)
            File.Delete(file);
    }

    [Fact]
    public async Task ASerialImport_OfAFileWithAMissingParent_IsRefusedByKeyName_AndChangesNothing()
    {
        var (file, childId) = await ExportWithMissingParentAsync();
        var target = await NewTargetWithOneObjectAsync();
        var key = await ParentKeyNameAsync(target);
        var before = await ScalarAsync(target, "SELECT count(*) FROM _objects");

        var importing = async () => await ImportAsync(target, file, parallelism: 1);

        var refusal = await importing.Should().ThrowAsync<InvalidOperationException>();
        refusal.Which.Message.Should()
            .Contain($"{key}: 1 row(s) of _objects point at a missing row of _objects (_id)")
            .And.Contain($"_id = {childId}, _id_parent = {Missing}")
            .And.Contain("Nothing was imported; the database is unchanged.");
        (await ScalarAsync(target, "SELECT count(*) FROM _objects")).Should().Be(before,
            "the refusal rolls back the whole import, the cleaning included");
        (await ScalarAsync(target, "SELECT count(*) FROM _objects WHERE _name = 'already-here'")).Should().Be(1);
    }

    [Fact]
    public async Task AParallelImport_OfAFileWithAMissingParent_IsRefusedByKeyName_AndSaysTheRowsAreWritten()
    {
        var (file, childId) = await ExportWithMissingParentAsync();
        var target = await NewTargetWithOneObjectAsync();
        var key = await ParentKeyNameAsync(target);

        var importing = async () => await ImportAsync(target, file, parallelism: 2);

        var refusal = await importing.Should().ThrowAsync<InvalidOperationException>();
        refusal.Which.Message.Should()
            .Contain($"{key}: 1 row(s) of _objects point at a missing row of _objects (_id)")
            .And.Contain($"_id = {childId}, _id_parent = {Missing}")
            .And.Contain("The parallel import has already written its rows");
        (await ScalarAsync(target, "SELECT count(*) FROM pg_indexes WHERE tablename IN ('_objects', '_values')"))
            .Should().BeGreaterThan(0, "the indexes are rebuilt before the check, so a refused parallel import leaves them in place");
    }

    [Fact]
    public async Task AFileExportedWithSchemes_WhoseObjectPointsIntoASchemeLeftOut_IsRefused_NamingTheReference()
    {
        var source = await NewDatabaseAsync("src");
        long parentId, childId;
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<CityProps>();
            await redb.SyncSchemeAsync<SimpleProps>();
            parentId = await redb.SaveAsync(new RedbObject<CityProps> { name = "parent-left-out", Props = new CityProps { Name = "c" } });
            childId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "child-exported", parent_id = parentId, Props = new SimpleProps { Title = "t" } });
        }
        var simpleScheme = await ScalarAsync(source, "SELECT min(_id) FROM _schemes WHERE _name LIKE '%SimpleProps%'");
        var file = await ExportAsync(source, [simpleScheme]);

        var target = await NewTargetWithOneObjectAsync();
        var key = await ParentKeyNameAsync(target);

        var importing = async () => await ImportAsync(target, file, parallelism: 1);

        var refusal = await importing.Should().ThrowAsync<InvalidOperationException>();
        refusal.Which.Message.Should()
            .Contain("--schemes")
            .And.Contain($"{key}: 1 row(s) of _objects point at a missing row of _objects (_id)")
            .And.Contain($"_id = {childId}, _id_parent = {parentId}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task AWholeFile_WithChildrenBeforeTheirParents_StillImports(int parallelism)
    {
        var source = await NewDatabaseAsync("src");
        long childId, parentId;
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            // The child gets the lower id, so the export writes it before its parent, possibly in another batch.
            var child = new RedbObject<SimpleProps> { name = "child", Props = new SimpleProps { Title = "c" } };
            childId = await redb.SaveAsync(child);
            parentId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "parent", Props = new SimpleProps { Title = "p" } });
        }
        await ExecuteAsync(source, $"UPDATE _objects SET _id_parent = {parentId} WHERE _id = {childId}");
        var file = await ExportAsync(source, null);

        var target = await NewTargetWithOneObjectAsync();
        await ImportAsync(target, file, parallelism);

        (await ScalarAsync(target, $"SELECT _id_parent FROM _objects WHERE _id = {childId}")).Should().Be(parentId,
            "a reference the file holds both ends of passes the check, whatever the order of the rows");
    }

    /// <summary>Exports a database whose one object points at a parent that is not there.</summary>
    private async Task<(string File, long ChildId)> ExportWithMissingParentAsync()
    {
        var source = await NewDatabaseAsync("src");
        long childId;
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            childId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "orphan", Props = new SimpleProps { Title = "o" } });
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = "sibling", Props = new SimpleProps { Title = "s" } });
        }
        // The key would refuse the update; with the triggers off it goes in, as it would from a broken file.
        await ExecuteAsync(source,
            $"SET session_replication_role = 'replica'; UPDATE _objects SET _id_parent = {Missing} WHERE _id = {childId}");
        return (await ExportAsync(source, null), childId);
    }

    private async Task<string> NewTargetWithOneObjectAsync()
    {
        var target = await NewDatabaseAsync("dst");
        await using var provider = Build(target);
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SaveAsync(new RedbObject<SimpleProps> { name = "already-here", Props = new SimpleProps { Title = "keep" } });
        return target;
    }

    private static async Task<string> ParentKeyNameAsync(string cs)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("""
            SELECT c.conname::text FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'f' AND c.conrelid = '_objects'::regclass AND a.attname = '_id_parent'
            """, connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private async Task<string> NewDatabaseAsync(string suffix)
    {
        var name = $"redb_export_fk_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Maintenance(), $"CREATE DATABASE \"{name}\"");
        return new NpgsqlConnectionStringBuilder(_server) { Database = name }.ConnectionString;
    }

    private string Maintenance() => new NpgsqlConnectionStringBuilder(_server) { Database = "postgres" }.ConnectionString;

    private static ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options
            .UsePostgres(cs)
            .Configure(c => c.CacheDomain = $"export-fk-{Guid.NewGuid():N}"));
        return services.BuildServiceProvider();
    }

    private async Task<string> ExportAsync(string cs, long[]? schemeIds)
    {
        var file = Path.Combine(Path.GetTempPath(), $"redb-export-fk-{Guid.NewGuid():N}.redb");
        _files.Add(file);
        await using var provider = ProviderFactory.Create("postgres");
        await provider.OpenAsync(cs);
        await new ExportService(provider, verbose: false, batchSize: 1000).ExportAsync(file, schemeIds, compress: false, dryRun: false);
        return file;
    }

    private static async Task ImportAsync(string cs, string file, int parallelism)
    {
        await using var provider = ProviderFactory.Create("postgres");
        await provider.OpenAsync(cs);
        await new ImportService(provider, verbose: false, batchSize: 1000, parallelism: parallelism)
            .ImportAsync(file, clean: true, dryRun: false);
    }

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
}
