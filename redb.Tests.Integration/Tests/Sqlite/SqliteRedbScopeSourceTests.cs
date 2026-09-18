using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// <see cref="IRedbScopeSource"/>: a service opens scopes of its own container for code that holds one service and
/// needs one per unit of work (redb.Route calls outside an exchange). The scope's service reads the same database, the
/// holder owns the scope, and a service with no container - or a container whose scopes read another database - refuses.
/// </summary>
public class SqliteRedbScopeSourceTests
{
    private const int Parallelism = 16;

    [Fact]
    public async Task AScopeOfTheContainer_GivesAnotherServiceOfTheSameDatabase_OwnedByTheHolder()
    {
        await using var provider = Build("same");
        var redb = provider.GetRequiredService<IRedbService>();
        await BootAsync(redb);

        redb.CanCreateScope.Should().BeTrue("the service came from a container");
        var scope = redb.CreateScope();
        scope.Service.Should().NotBeSameAs(redb, "a scope has a service of its own");
        scope.Service.CacheDomain.Should().Be(redb.CacheDomain, "the same database");
        scope.ServiceProvider.Should().NotBeNull();

        var id = await scope.Service.SaveAsync(new RedbObject<SimpleProps> { name = $"scope-{NewTag()}", Props = new SimpleProps { Title = "from the scope" } });
        (await redb.LoadAsync<SimpleProps>(id))!.Props.Title.Should().Be("from the scope", "written to the database the holder reads");

        await scope.DisposeAsync();
        scope.Service.Context.IsDisposed.Should().BeTrue("disposing the scope ends its service");
    }

    [Fact]
    public async Task ScopesOpenedAtOnce_WorkAtOnce()
    {
        await using var provider = Build("parallel");
        var redb = provider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        var tag = NewTag();

        // What one captive service could not do: sixteen units of work on the database at the same moment.
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var work = Enumerable.Range(0, Parallelism).Select(i => Task.Run(async () =>
        {
            await start.Task;
            await using var scope = redb.CreateScope();
            var id = await scope.Service.SaveAsync(new RedbObject<SimpleProps> { name = $"parallel-{tag}-{i}", Props = new SimpleProps { Title = $"unit-{i}" } });
            return (Service: scope.Service, Id: id);
        })).ToArray();
        start.SetResult();
        var results = await Task.WhenAll(work);

        results.Select(r => r.Service).Distinct().Should().HaveCount(Parallelism, "one service per scope");
        foreach (var (_, id) in results)
            (await redb.LoadAsync<SimpleProps>(id)).Should().NotBeNull();
    }

    [Fact]
    public async Task AServiceBuiltWithoutAContainer_CannotOpenAScope()
    {
        await using var provider = Build("bare");
        await BootAsync(provider.GetRequiredService<IRedbService>());

        // The same registrations, but no scope factory: a service assembled by hand from a provider of one's own.
        var bare = new global::redb.SQLite.RedbService(new WithoutScopeFactory(provider));

        bare.CanCreateScope.Should().BeFalse();
        var opening = () => bare.CreateScope();
        opening.Should().Throw<InvalidOperationException>().WithMessage("*IServiceScopeFactory*");
    }

    [Fact]
    public async Task AContainerWhoseScopesReadAnotherDatabase_IsRefused()
    {
        await using var other = Build("other");
        await BootAsync(other.GetRequiredService<IRedbService>());
        await using var otherScope = other.CreateAsyncScope();

        // A container that hands the root one database and every scope another: nothing a scope opened from the root
        // service may silently read.
        var services = Register("split");
        services.AddScoped<IRedbService>(sp => RedbServiceProviders.IsRoot(sp)
            ? new global::redb.SQLite.RedbService(sp)
            : new global::redb.SQLite.RedbService(otherScope.ServiceProvider));
        await using var split = services.BuildServiceProvider();
        var root = split.GetRequiredService<IRedbService>();
        await BootAsync(root);

        root.CanCreateScope.Should().BeTrue();
        var opening = () => root.CreateScope();
        opening.Should().Throw<InvalidOperationException>().WithMessage("*differently per resolution*", "the scope's service reads another database");
    }

    private static ServiceProvider Build(string suffix) => Register(suffix).BuildServiceProvider();

    private static ServiceCollection Register(string suffix)
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_scope_source_{suffix}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.CacheDomain = $"scope-source-{suffix}-{Guid.NewGuid():N}");
        });
        return services;
    }

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    /// <summary>The container's services, minus the scope factory.</summary>
    private sealed class WithoutScopeFactory(IServiceProvider inner) : IServiceProvider
    {
        public object? GetService(Type serviceType)
            => serviceType == typeof(IServiceScopeFactory) ? null : inner.GetService(serviceType);
    }
}
