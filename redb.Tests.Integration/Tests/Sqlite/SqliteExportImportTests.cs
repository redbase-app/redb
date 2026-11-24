using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Export.Providers;
using redb.Export.Services;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// redb.Export between two SQLite files (review 2026-09-24): a round trip keeps the metadata the schema carries,
/// and an import that cannot finish changes nothing - no half-filled database, no emptied one. Turning the foreign
/// keys back on after the load checks nothing already there, so the import checks the loaded rows itself and refuses
/// a file with references to rows it does not hold.
/// </summary>
public class SqliteExportImportTests
{
    [Fact]
    public async Task ARoundTrip_KeepsSchemeAndFieldTags_AndTheUniqueScope()
    {
        var source = NewDatabase("src");
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
        }

        // Written straight into the catalog, the way an extension or [RedbTags] leaves them. Ids survive an import.
        var schemeId = await ScalarAsync(source, "SELECT MIN(_id) FROM _schemes WHERE _name LIKE '%SimpleProps%'");
        var structureId = await ScalarAsync(source, $"SELECT MIN(_id) FROM _structures WHERE _id_scheme = {schemeId}");
        schemeId.Should().NotBeNull("precondition: the scheme was synced");
        structureId.Should().NotBeNull("precondition: the scheme has fields");
        await ExecuteAsync(source, $"UPDATE _schemes SET _tags = 'scheme-tag' WHERE _id = {schemeId}");
        await ExecuteAsync(source, $"UPDATE _structures SET _tags = 'field-tag', _unique_scope = 2 WHERE _id = {structureId}");

        var file = await ExportAsync(source);
        var target = NewDatabase("dst");
        await CreateEmptyAsync(target);
        await ImportAsync(target, file);

        (await ScalarAsync(target, $"SELECT _tags FROM _schemes WHERE _id = {schemeId}")).Should().Be("scheme-tag",
            "the scheme's marker travels with the export");
        (await ScalarAsync(target, $"SELECT _tags || ':' || _unique_scope FROM _structures WHERE _id = {structureId}")).Should().Be("field-tag:2",
            "so do the field's marker and the scope of its unique key, or the meaning of the constraint changes");
    }

    [Fact]
    public async Task AnImportOfACutFile_ChangesNothing_EvenWithCleaning()
    {
        var source = NewDatabase("src");
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            for (var i = 0; i < 5; i++)
                await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"exported-{i}", Props = new SimpleProps { Title = $"t{i}" } });
        }
        var file = await ExportAsync(source);

        // The file loses its tail - the footer and the last records - as a copy that stopped half-way would.
        var lines = await File.ReadAllLinesAsync(file);
        lines.Should().HaveCountGreaterThan(4, "precondition: an export of a seeded database has records");
        await File.WriteAllLinesAsync(file, lines.Take(lines.Length - 3));

        var target = NewDatabase("dst");
        await using (var provider = Build(target))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = "already-here", Props = new SimpleProps { Title = "keep" } });
        }
        var before = await ScalarAsync(target, "SELECT COUNT(*) FROM _objects");

        var importing = async () => await ImportAsync(target, file);

        await importing.Should().ThrowAsync<InvalidOperationException>().WithMessage("*footer*");
        (await ScalarAsync(target, "SELECT COUNT(*) FROM _objects")).Should().Be(before,
            "a failed import - cleaning included - leaves the database as it found it");
        (await ScalarAsync(target, "SELECT COUNT(*) FROM _objects WHERE _name = 'already-here'")).Should().Be("1");
    }

    [Fact]
    public async Task AnImportOfAFileWithAMissingParent_IsRefusedNamingTheKey_AndChangesNothing()
    {
        const long missing = 999_999_999;
        var source = NewDatabase("src");
        long childId;
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            childId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "orphan", Props = new SimpleProps { Title = "o" } });
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = "sibling", Props = new SimpleProps { Title = "s" } });
        }
        // The key would refuse the update; with the keys off it goes in, as it would from a broken file.
        await ExecuteAsync(source, $"PRAGMA foreign_keys = OFF; UPDATE _objects SET _id_parent = {missing} WHERE _id = {childId}");
        var file = await ExportAsync(source);

        var target = await NewTargetWithOneObjectAsync();
        var before = await ScalarAsync(target, "SELECT COUNT(*) FROM _objects");

        var importing = async () => await ImportAsync(target, file);

        var refusal = await importing.Should().ThrowAsync<InvalidOperationException>();
        refusal.Which.Message.Should()
            .Contain("_objects._id_parent: 1 row(s) of _objects point at a missing row of _objects (_id)")
            .And.Contain($"_id = {childId}, _id_parent = {missing}")
            .And.Contain("Nothing was imported; the database is unchanged.");
        (await ScalarAsync(target, "SELECT COUNT(*) FROM _objects")).Should().Be(before,
            "the refusal rolls back the whole import, the cleaning included");
        (await ScalarAsync(target, "SELECT COUNT(*) FROM _objects WHERE _name = 'already-here'")).Should().Be("1");
    }

    [Fact]
    public async Task AFileExportedWithSchemes_WhoseObjectPointsIntoASchemeLeftOut_IsRefused_NamingTheReference()
    {
        var source = NewDatabase("src");
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
        var simpleScheme = long.Parse((await ScalarAsync(source, "SELECT MIN(_id) FROM _schemes WHERE _name LIKE '%SimpleProps%'"))!);
        var file = await ExportAsync(source, [simpleScheme]);

        var target = await NewTargetWithOneObjectAsync();

        var importing = async () => await ImportAsync(target, file);

        var refusal = await importing.Should().ThrowAsync<InvalidOperationException>();
        refusal.Which.Message.Should()
            .Contain("--schemes")
            .And.Contain("_objects._id_parent: 1 row(s) of _objects point at a missing row of _objects (_id)")
            .And.Contain($"_id = {childId}, _id_parent = {parentId}");
    }

    [Fact]
    public async Task AWholeFile_WithAChildBeforeItsParent_StillImports()
    {
        var source = NewDatabase("src");
        long childId, parentId;
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<SimpleProps>();
            // The child gets the lower id, so the export writes it before its parent.
            childId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "child", Props = new SimpleProps { Title = "c" } });
            parentId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = "parent", Props = new SimpleProps { Title = "p" } });
        }
        await ExecuteAsync(source, $"UPDATE _objects SET _id_parent = {parentId} WHERE _id = {childId}");
        var file = await ExportAsync(source);

        var target = await NewTargetWithOneObjectAsync();
        await ImportAsync(target, file);

        (await ScalarAsync(target, $"SELECT _id_parent FROM _objects WHERE _id = {childId}")).Should().Be(parentId.ToString(),
            "a reference the file holds both ends of passes the check, whatever the order of the rows");
    }

    private static async Task<string> NewTargetWithOneObjectAsync()
    {
        var target = NewDatabase("dst");
        await using var provider = Build(target);
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SaveAsync(new RedbObject<SimpleProps> { name = "already-here", Props = new SimpleProps { Title = "keep" } });
        return target;
    }

    private static string NewDatabase(string suffix)
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_export_{suffix}_{Guid.NewGuid():N}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        return cs;
    }

    private static ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.CacheDomain = $"export-{Guid.NewGuid():N}");
        });
        return services.BuildServiceProvider();
    }

    private static async Task CreateEmptyAsync(string cs)
    {
        await using var provider = Build(cs);
        await provider.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
    }

    private static async Task<string> ExportAsync(string cs, long[]? schemeIds = null)
    {
        var file = Path.Combine(Path.GetTempPath(), $"redb-export-{Guid.NewGuid():N}.redb");
        await using var provider = ProviderFactory.Create("sqlite");
        await provider.OpenAsync(cs);
        await new ExportService(provider, verbose: false, batchSize: 1000).ExportAsync(file, schemeIds, compress: false, dryRun: false);
        return file;
    }

    private static async Task ImportAsync(string cs, string file)
    {
        await using var provider = ProviderFactory.Create("sqlite");
        await provider.OpenAsync(cs);
        await new ImportService(provider, verbose: false, batchSize: 1000).ImportAsync(file, clean: true, dryRun: false);
    }

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (await command.ExecuteScalarAsync())?.ToString();
    }
}
