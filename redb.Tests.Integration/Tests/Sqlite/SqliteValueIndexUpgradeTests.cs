using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// Schema step 11 of <c>ApplySchemaUpgradesAsync</c>: a file of schema version 10 gets the value-leading
/// indexes IX__values__structure_&lt;value&gt;_object and loses the object-leading *_not_null,
/// ix__values__array_null_structure and the IX__values__scalar_*_object of SQLite Pro. An index of one of
/// the new names with another definition is rebuilt. Staged the way the other upgrade tests stage it:
/// create a current file, put the old indexes back, stamp version 10, reopen.
/// </summary>
public class SqliteValueIndexUpgradeTests : IDisposable
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"redb_valueidx_{Guid.NewGuid():N}.db");

    private static readonly string[] Columns =
        ["String", "Long", "Numeric", "Double", "DateTimeOffset", "Boolean", "Guid", "ListItem", "Object"];

    private static readonly string[] Removed =
    [
        "IX__values__String_not_null", "IX__values__Long_not_null", "IX__values__DateTimeOffset_not_null",
        "IX__values__Numeric_not_null", "ix__values__array_null_structure",
        "IX__values__scalar_String_object", "IX__values__scalar_Long_object",
        "IX__values__scalar_Numeric_object", "IX__values__scalar_DateTimeOffset_object",
    ];

    private ServiceProvider BuildServices()
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseSqlite($"Data Source={_dbPath}"));
        return services.BuildServiceProvider();
    }

    private static Task<string?> IndexSqlAsync(IRedbContext ctx, string name)
        => ctx.ExecuteScalarAsync<string?>($"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = '{name}'");

    [Fact]
    public async Task Version10File_GetsValueLeadingIndexes_AndLosesTheOldOnes()
    {
        await using (var sp = BuildServices())
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            var ctx = sp.GetRequiredService<IRedbContext>();

            foreach (var column in Columns)
                (await IndexSqlAsync(ctx, $"IX__values__structure_{column.ToLowerInvariant()}_object"))
                    .Should().NotBeNull($"a new file is created with the {column} index");

            // A version 10 file: the old indexes, one new name with the research definition, the stamp.
            await ctx.ExecuteAsync("DROP INDEX \"IX__values__structure_long_object\"");
            await ctx.ExecuteAsync("CREATE INDEX \"IX__values__structure_long_object\" ON _values (_id_structure, _Long, _id_object) WHERE _array_index IS NULL AND _Long IS NOT NULL");
            await ctx.ExecuteAsync("DROP INDEX \"IX__values__structure_guid_object\"");
            await ctx.ExecuteAsync("CREATE INDEX \"IX__values__String_not_null\" ON _values (_id_structure, _id_object, _String) WHERE _String IS NOT NULL");
            await ctx.ExecuteAsync("CREATE INDEX \"ix__values__array_null_structure\" ON _values (_id_structure, _id_object, _Long) WHERE _array_index IS NULL");
            await ctx.ExecuteAsync("CREATE INDEX \"IX__values__scalar_Numeric_object\" ON _values (_id_structure, _Numeric, _id_object) WHERE _array_index IS NULL AND _Numeric IS NOT NULL");
            await ctx.ExecuteAsync("PRAGMA user_version = 10");
        }

        await using (var sp = BuildServices())
        {
            await sp.GetRequiredService<IRedbService>().InitializeAsync();
            var ctx = sp.GetRequiredService<IRedbContext>();

            foreach (var column in Columns)
            {
                var name = $"IX__values__structure_{column.ToLowerInvariant()}_object";
                var sql = await IndexSqlAsync(ctx, name);
                sql.Should().NotBeNull($"step 11 builds {name}");
                System.Text.RegularExpressions.Regex.Replace(sql!, @"\s+", " ")
                    .Should().Contain($"ON _values (_id_structure, _{column}, _id_object, _array_index) WHERE _{column} IS NOT NULL",
                        "an index of the same name with another definition is rebuilt");
            }

            foreach (var name in Removed)
                (await IndexSqlAsync(ctx, name)).Should().BeNull($"step 11 drops {name}");

            (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().BeGreaterThanOrEqualTo(11);
        }
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
