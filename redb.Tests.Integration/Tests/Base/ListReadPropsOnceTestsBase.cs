using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A list read and a tree read read the values of the objects found in one statement, and hand out what loading the
/// objects one by one hands out, in the order asked for. Free used to build the JSON of every object twice: once in
/// the statement that found the objects (get_object_json per row) and once more in the lazy loader, which then
/// replaced the first copy. Pro reads the values in one bulk statement. The SQL is seen through the test context.
/// </summary>
public abstract class ListReadPropsOnceTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = $"props-once-{GetType().Name}-{Guid.NewGuid():N}";
            });
        });
        RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    /// <summary>A statement that reads the values of objects: Free's get_object_json, Pro's bulk read of _values.</summary>
    private static bool ReadsValues(string sql)
        => sql.Contains("get_object_json", StringComparison.OrdinalIgnoreCase)
           || (sql.Contains("_array_parent_id, _array_index", StringComparison.Ordinal) && sql.Contains("FROM _values", StringComparison.Ordinal));

    private static async Task<(IRedbService Redb, string Tag, long RootId)> SeedAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        var tag = "once-" + Guid.NewGuid().ToString("N")[..8];
        var rootId = await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"{tag}-root", Props = new SimpleProps { Title = $"{tag}-root", Price = 100 } });
        for (var i = 1; i <= 5; i++)
        {
            await redb.SaveAsync(new RedbObject<SimpleProps>
            {
                name = $"{tag}-{i}",
                parent_id = rootId,
                Props = new SimpleProps
                {
                    Title = $"{tag}-{i}", Count = i, Price = 10.5m * i, IsActive = i % 2 == 0, Code = Guid.NewGuid(),
                    CreatedAt = new DateTime(2026, 1, i, 8, 30, 0, DateTimeKind.Utc), Description = i == 3 ? null : $"d{i}",
                    OptionalNumber = i == 2 ? null : i * 1000L
                }
            });
        }
        return (redb, tag, rootId);
    }

    private async Task AssertMatchesSingleLoadsAsync(IRedbService redb, IReadOnlyList<RedbObject<SimpleProps>> page)
    {
        foreach (var obj in page)
        {
            var single = await redb.LoadAsync<SimpleProps>(obj.id);
            obj.Props.Should().BeEquivalentTo(single!.Props, "a list hands out what a single load hands out (object {0})", obj.id);
            obj.name.Should().Be(single.name);
            obj.hash.Should().Be(single.hash);
            obj.parent_id.Should().Be(single.parent_id);
        }
    }

    [Fact]
    public async Task ListRead_ReadsTheValuesInOneStatement_InTheOrderAsked()
    {
        await using var host = Build();
        var (redb, tag, _) = await SeedAsync(host.GetRequiredService<IRedbService>());

        var children = tag + "-";
        _recorder.Start();
        var page = await redb.Query<SimpleProps>().Where(p => p.Title.StartsWith(children) && p.Price < 100)
            .OrderByDescending(p => p.Price).Take(4).ToListAsync();
        var commands = _recorder.Stop();

        page.Select(o => o.Props.Count).Should().Equal(5, 4, 3, 2);
        commands.Count(c => ReadsValues(c.Sql)).Should().Be(1,
            "the values of the page are read once:\n" + string.Join("\n---\n", commands.Select(c => c.Sql)));
        await AssertMatchesSingleLoadsAsync(redb, page);
    }

    [Fact]
    public async Task TreeRead_ReadsTheValuesInOneStatement_InTheOrderAsked()
    {
        await using var host = Build();
        var (redb, _, rootId) = await SeedAsync(host.GetRequiredService<IRedbService>());

        _recorder.Start();
        var nodes = await redb.TreeQuery<SimpleProps>(rootId).WhereRedb(o => o.Id != rootId)
            .OrderBy(p => p.Price).ToListAsync();
        var commands = _recorder.Stop();

        nodes.Select(o => o.Props.Count).Should().Equal(1, 2, 3, 4, 5);
        commands.Count(c => ReadsValues(c.Sql)).Should().Be(1,
            "the values of the tree are read once:\n" + string.Join("\n---\n", commands.Select(c => c.Sql)));
        await AssertMatchesSingleLoadsAsync(redb, nodes.Cast<RedbObject<SimpleProps>>().ToList());
    }
}
