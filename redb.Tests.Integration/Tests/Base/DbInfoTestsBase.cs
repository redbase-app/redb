using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Server facts through <see cref="IRedbService"/>: the version and the database size. The SQLite service sent the
/// PostgreSQL text - <c>version()</c> and <c>pg_database_size(current_database())</c> - and failed on its own provider;
/// MSSQL reported the size in kilobytes while the others reported bytes.
/// </summary>
public abstract class DbInfoTestsBase
{
    /// <summary>Registers the provider on the options builder (Free or Pro, see <see cref="Register"/>).</summary>
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    /// <summary>Text every version string of this provider contains.</summary>
    protected abstract string VersionMarker { get; }

    /// <summary>The database size in bytes, straight from the server's own catalog, to pin the unit.</summary>
    protected abstract string SizeInBytesSql { get; }

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c => c.CacheDomain = "db-info");
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Version_ComesFromTheServer()
    {
        await using var sp = Build();
        var service = sp.GetRequiredService<IRedbService>();
        await service.InitializeAsync(ensureCreated: true);

        (await service.GetDbVersionAsync()).Should().Contain(VersionMarker);
        service.dbVersion.Should().Contain(VersionMarker);
    }

    [Fact]
    public async Task Size_IsInBytes()
    {
        await using var sp = Build();
        var service = sp.GetRequiredService<IRedbService>();
        await service.InitializeAsync(ensureCreated: true);

        var reported = service.dbSize;
        var expected = await service.Context.ExecuteScalarAsync<long>(SizeInBytesSql);

        expected.Should().BeGreaterThan(0);
        reported.Should().NotBeNull();
        // Other suites write to the shared database meanwhile: the unit is pinned, not the exact byte count.
        reported!.Value.Should().BeInRange(expected / 2, expected * 2, "dbSize reports bytes on every provider");
    }
}
