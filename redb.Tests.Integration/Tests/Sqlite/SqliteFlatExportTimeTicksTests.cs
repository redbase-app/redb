using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Export.Flat;
using redb.Export.Providers;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// The flat export reads TimeSpan and TimeOnly from _values._Long (their Ticks) and writes the target's own time forms;
/// it read them from _String before, where they no longer are.
/// </summary>
public class SqliteFlatExportTimeTicksTests : IDisposable
{
    private readonly string _source = Path.Combine(Path.GetTempPath(), $"redb_flat_src_{Guid.NewGuid():N}.db");
    private readonly string _target = Path.Combine(Path.GetTempPath(), $"redb_flat_dst_{Guid.NewGuid():N}.db");

    [Fact]
    public async Task FlatExport_WritesTimeSpanAndTimeOnly_FromTheirTicks()
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var span = TimeSpan.FromDays(3).Add(TimeSpan.FromHours(-5)).Add(TimeSpan.FromTicks(123));
        var clock = new TimeOnly(21, 45, 7, 250);

        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseSqlite($"Data Source={_source}"));
        await using (var sp = services.BuildServiceProvider())
        {
            var redb = sp.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true);
            await redb.SyncSchemeAsync<TimeTicksProps>();
            await redb.SaveAsync(new RedbObject<TimeTicksProps> { name = "flat", Props = new TimeTicksProps { Tag = "flat", Span = span, Clock = clock } });
        }
        SqliteConnection.ClearAllPools();

        await using (var source = ProviderFactory.Create("sqlite"))
        await using (var target = ProviderFactory.Create("sqlite"))
        {
            await source.OpenAsync($"Data Source={_source}");
            await target.OpenAsync($"Data Source={_target}");
            await new FlatExportService(source, target, verbose: false, batchSize: 100).ExportAsync([typeof(TimeTicksProps)]);
        }
        SqliteConnection.ClearAllPools();

        await using var connection = new SqliteConnection($"Data Source={_target}");
        await connection.OpenAsync();
        string? table = null;
        await using (var tables = connection.CreateCommand())
        {
            tables.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table'";
            await using var reader = await tables.ExecuteReaderAsync();
            var names = new List<string>();
            while (await reader.ReadAsync()) names.Add(reader.GetString(0));
            foreach (var name in names)
            {
                await using var columns = connection.CreateCommand();
                columns.CommandText = $"SELECT COUNT(*) FROM pragma_table_info('{name.Replace("'", "''")}') WHERE name IN ('Span', 'Clock')";
                if ((long)(await columns.ExecuteScalarAsync())! == 2) table = name;
            }
        }
        table.Should().NotBeNull("the export writes a table with the Span and Clock columns");

        await using var select = connection.CreateCommand();
        select.CommandText = $"SELECT \"Span\", \"Clock\" FROM \"{table}\"";
        await using var row = await select.ExecuteReaderAsync();
        (await row.ReadAsync()).Should().BeTrue();
        TimeSpan.Parse(row.GetString(0), System.Globalization.CultureInfo.InvariantCulture).Should().Be(span);
        TimeOnly.Parse(row.GetString(1), System.Globalization.CultureInfo.InvariantCulture).Should().Be(clock);
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { _source, _target })
        foreach (var x in new[] { f, f + "-wal", f + "-shm" })
            if (File.Exists(x)) File.Delete(x);
    }
}
