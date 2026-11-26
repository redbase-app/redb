using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A scheme whose property names differ from C# keywords only by case (Ref, Default, Event, String, Object) works end to
/// end: sync, save, load, Where, OrderBy, GroupBy, with the planner off and on. The names reach the SQL of every provider
/// as structure ids and quoted aliases, never folded to lower case.
/// </summary>
public abstract class KeywordNamesTestsBase
{
    protected readonly IRedbService Redb;

    protected KeywordNamesTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<long>> SeedAsync(string tag)
    {
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var objects = Enumerable.Range(0, 6).Select(i => new RedbObject<KeywordNamesProps>
        {
            name = $"{tag}_{i}",
            Props = new KeywordNamesProps
            {
                Ref = 100 + i,
                Default = i % 2 == 0 ? $"{tag}-even" : $"{tag}-odd",
                Event = start.AddDays(i),
                String = i == 5 ? null : $"{tag}-s{i}",
                Object = 10.5m * i
            }
        }).ToList();
        return await Redb.SaveAsync(objects);
    }

    private async Task EachFormAsync(Func<string, Task> check)
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            foreach (var on in new[] { false, true })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = on);
                await check(on ? "planner on" : "planner off");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    [Fact]
    public async Task KeywordLikeNames_SaveAndLoad()
    {
        var tag = $"kwl{Guid.NewGuid():N}"[..12];
        var ids = await SeedAsync(tag);

        var loaded = await Redb.LoadAsync<KeywordNamesProps>(ids[3]);

        loaded.Should().NotBeNull();
        loaded!.Props.Ref.Should().Be(103);
        loaded.Props.Default.Should().Be($"{tag}-odd");
        loaded.Props.Event.Should().Be(new DateTime(2026, 1, 4, 0, 0, 0, DateTimeKind.Utc));
        loaded.Props.String.Should().Be($"{tag}-s3");
        loaded.Props.Object.Should().Be(31.5m);
    }

    [Fact]
    public async Task KeywordLikeNames_WhereOrderByGroupBy()
    {
        var tag = $"kwq{Guid.NewGuid():N}"[..12];
        await SeedAsync(tag);

        await EachFormAsync(async form =>
        {
            IRedbQueryable<KeywordNamesProps> Mine() => Redb.Query<KeywordNamesProps>().Where(p => p.Default.StartsWith(tag));

            var byRef = await Mine().Where(p => p.Ref >= 102 && p.Object < 40m).OrderByDescending(p => p.Event).ToListAsync();
            byRef.Select(o => o.Props.Ref).Should().Equal(new long[] { 103, 102 }, form);

            var byDefault = await Mine().Where(p => p.Default == $"{tag}-even").OrderBy(p => p.Object).ToListAsync();
            byDefault.Select(o => o.Props.Ref).Should().Equal(new long[] { 100, 102, 104 }, form);

            var byString = await Mine().Where(p => p.String == null).ToListAsync();
            byString.Select(o => o.Props.Ref).Should().Equal(new long[] { 105 }, form);

            var byEvent = await Mine().Where(p => p.Event > new DateTime(2026, 1, 4, 0, 0, 0, DateTimeKind.Utc)).OrderBy(p => p.String).ToListAsync();
            byEvent.Select(o => o.Props.Ref).Should().Equal(new long[] { 105, 104 }, form);

            var groups = await Mine()
                .GroupBy(p => p.Default)
                .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, p => p.Object), Max = Agg.Max(g, p => p.Ref) });
            groups.OrderBy(g => g.Key).Select(g => (g.Key, g.Count, g.Sum, g.Max)).Should().Equal(
                new[] { ($"{tag}-even", 3, 63m, 104L), ($"{tag}-odd", 3, 94.5m, 105L) }, form);
        });
    }
}
