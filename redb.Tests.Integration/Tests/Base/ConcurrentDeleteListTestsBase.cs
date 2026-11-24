using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A list query reads the objects first and their values after, each statement on its own snapshot. An object deleted in
/// between used to come back with null Props. It now leaves the list: the page is shorter by it, and every object handed
/// out has its Props. The delete is put exactly between the two statements through the test context, no code of the
/// library takes part in it.
/// </summary>
public abstract class ConcurrentDeleteListTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(bool decorate)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = $"concurrent-delete-{GetType().Name}-{Guid.NewGuid():N}";
            });
        });
        if (decorate)
            RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    /// <summary>The statement that reads the values of the objects found: Pro's bulk read of _values, Free's get_object_json.</summary>
    private static bool ReadsValues(string sql)
        => sql.Contains("get_object_json", StringComparison.OrdinalIgnoreCase)
           || (sql.Contains("_array_parent_id, _array_index", StringComparison.Ordinal) && sql.Contains("FROM _values", StringComparison.Ordinal));

    [Fact]
    public async Task ObjectDeletedBetweenTheQueryAndItsValues_LeavesThePage_NoObjectWithoutProps()
    {
        await using var writerHost = Build(decorate: false);
        var writer = writerHost.GetRequiredService<IRedbService>();
        await writer.InitializeAsync(ensureCreated: true);
        await writer.SyncSchemeAsync<SimpleProps>();

        var tag = "cdel-" + Guid.NewGuid().ToString("N")[..8];
        var ids = new List<long>();
        for (var i = 0; i < 3; i++)
            ids.Add(await writer.SaveAsync(new RedbObject<SimpleProps> { name = $"{tag}-{i}", Props = new SimpleProps { Title = $"{tag}-{i}", Price = i } }));

        await using var readerHost = Build(decorate: true);
        var reader = readerHost.GetRequiredService<IRedbService>();
        await reader.InitializeAsync(ensureCreated: true);

        var deleted = 0;
        _recorder.BeforeQuery = async sql =>
        {
            if (!ReadsValues(sql) || Interlocked.Exchange(ref deleted, 1) == 1) return;
            await writer.DeleteAsync(ids[1]);
        };
        List<RedbObject<SimpleProps>> page;
        try
        {
            page = await reader.Query<SimpleProps>().Where(p => p.Title.StartsWith(tag)).OrderBy(p => p.Price).ToListAsync();
        }
        finally
        {
            _recorder.BeforeQuery = null;
        }

        deleted.Should().Be(1, "precondition: the delete ran between the read of the objects and of their values");
        page.Select(o => o.id).Should().Equal(ids[0], ids[2]);
        page.Should().OnlyContain(o => o.Props != null);
        page.Select(o => o.Props.Title).Should().Equal($"{tag}-0", $"{tag}-2");

        await writer.DeleteAsync(new[] { ids[0], ids[2] });
    }
}
