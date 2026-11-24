using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A SQLite file written by a newer build is never taken back by an older one: the older build opens it when the
/// file's <c>_schema_compat</c> row allows (a warning, nothing changed) and stops with RedbSchemaNewerException when it
/// does not, or when the row is missing. Staged the way the other upgrade tests stage a file: create a current file,
/// then rewrite its stamp and its row as a newer build would have left them, and reopen.
/// </summary>
public abstract class SqliteSchemaNewerTestsBase : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"redb_newer_{Guid.NewGuid():N}.db");

    /// <summary>The tier's registration (AddRedb or AddRedbPro) with the SQLite provider on the given connection string.</summary>
    protected abstract void AddRedbServices(IServiceCollection services, string connectionString);

    private ServiceProvider BuildServices(ILoggerProvider? logs = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b =>
        {
            b.SetMinimumLevel(LogLevel.Warning);
            if (logs != null)
                b.AddProvider(logs);
        });
        AddRedbServices(services, $"Data Source={_dbPath};Pooling=False");
        return services.BuildServiceProvider();
    }

    /// <summary>Creates a current file and returns its schema version and its row.</summary>
    private async Task<(long Version, long MinReader, string Package)> CreateCurrentFileAsync()
    {
        await using var sp = BuildServices();
        await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
        var ctx = sp.GetRequiredService<IRedbContext>();
        var version = await ctx.ExecuteScalarAsync<long>("PRAGMA user_version");
        var minReader = await ctx.ExecuteScalarAsync<long>("SELECT _min_reader FROM _schema_compat");
        var package = (await ctx.ExecuteScalarAsync<string>("SELECT _package FROM _schema_compat"))!;
        return (version, minReader, package);
    }

    /// <summary>Rewrites the file as a newer build would have left it; a null row drops the table.</summary>
    private async Task MakeNewerAsync(long version, long? minReader, string package = "9.9.0")
    {
        await using var sp = BuildServices();
        var ctx = sp.GetRequiredService<IRedbContext>();
        if (minReader is null)
            await ctx.ExecuteAsync("DROP TABLE _schema_compat");
        else
        {
            await ctx.ExecuteAsync("DELETE FROM _schema_compat");
            await ctx.ExecuteAsync($"INSERT INTO _schema_compat(_min_reader, _package) VALUES ({minReader}, '{package}')");
        }
        await ctx.ExecuteAsync($"PRAGMA user_version = {version}");
    }

    private async Task<long> FileVersionAsync()
    {
        await using var sp = BuildServices();
        return await sp.GetRequiredService<IRedbContext>().ExecuteScalarAsync<long>("PRAGMA user_version");
    }

    [Fact]
    public async Task NewFile_CarriesItsCompatibilityRow()
    {
        var (version, minReader, package) = await CreateCurrentFileAsync();

        minReader.Should().BeGreaterThan(0).And.BeLessThanOrEqualTo(version);
        package.Should().MatchRegex(@"^\d+\.\d+\.\d+$");
    }

    [Fact]
    public async Task NewerFile_ThatAllowsThisBuild_IsOpened_AndLeftAsItIs()
    {
        var (version, _, _) = await CreateCurrentFileAsync();
        var newer = version + 1000 + Random.Shared.Next(1, 1_000_000);
        await MakeNewerAsync(newer, minReader: version);

        var logs = new CapturingLoggerProvider();
        await using (var sp = BuildServices(logs))
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

        (await FileVersionAsync()).Should().Be(newer, "an older build never takes a newer file back");
        logs.Warnings.Should().ContainSingle(w => w.Contains(newer.ToString()) && w.Contains("left as it is"));
    }

    [Fact]
    public async Task NewerFile_ThatNoLongerAllowsThisBuild_StopsTheStart_AndChangesNothing()
    {
        var (version, _, _) = await CreateCurrentFileAsync();
        var newer = version + 1000;
        await MakeNewerAsync(newer, minReader: newer);

        await using (var sp = BuildServices())
        {
            var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
            var ex = (await act.Should().ThrowAsync<RedbSchemaNewerException>()).Which;
            ex.DeployedVersion.Should().Be(newer.ToString());
            ex.PackageVersion.Should().Be(version.ToString());
            ex.RequiredPackageVersion.Should().Be("9.9.0");
            ex.Message.Should().Contain("from 9.9.0 on").And.Contain("the database was left unchanged");
        }

        (await FileVersionAsync()).Should().Be(newer);
    }

    [Fact]
    public async Task NewerFile_WithoutTheRow_StopsTheStart()
    {
        var (version, _, _) = await CreateCurrentFileAsync();
        await MakeNewerAsync(version + 1000, minReader: null);

        await using var sp = BuildServices();
        var act = async () => await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);
        var ex = (await act.Should().ThrowAsync<RedbSchemaNewerException>()).Which;
        ex.RequiredPackageVersion.Should().BeNull();
    }

    [Fact]
    public async Task OlderFile_WithoutTheRow_IsUpgraded_AndGetsIt()
    {
        var (version, minReader, _) = await CreateCurrentFileAsync();
        await MakeNewerAsync(version - 1, minReader: null);

        await using (var sp = BuildServices())
            await sp.GetRequiredService<IRedbService>().InitializeAsync(ensureCreated: true);

        await using var check = BuildServices();
        var ctx = check.GetRequiredService<IRedbContext>();
        (await ctx.ExecuteScalarAsync<long>("PRAGMA user_version")).Should().Be(version);
        (await ctx.ExecuteScalarAsync<long>("SELECT _min_reader FROM _schema_compat")).Should().Be(minReader);
    }

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var path in new[] { _dbPath, _dbPath + "-wal", _dbPath + "-shm" })
            try { File.Delete(path); } catch (IOException) { }
    }
}
