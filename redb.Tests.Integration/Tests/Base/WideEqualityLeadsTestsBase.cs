using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A page sorted by a field with an index of values and filtered by one equality: the equality drives while a capped
/// count finds fewer matches than PvtPrefilterPlanner.WideEqualityCap, and the sort field's index is walked from the cap
/// on. Both sides of the cap must give the answer computed from the loaded objects, with the planner off and on: 4 000
/// objects, "wide" on 3 240 of them, "rare" on 40; descending (an object without the sort value comes last), ascending,
/// with Skip, and with a second condition on the sort field.
/// </summary>
public abstract class WideEqualityLeadsTestsBase
{
    private const int Objects = 4000;
    private static readonly SemaphoreSlim SeedLock = new(1, 1);

    protected readonly IRedbService Redb;

    protected WideEqualityLeadsTestsBase(IRedbService redb) => Redb = redb;

    private static string Tag(int i) => i % 100 == 0 ? "rare" : i < 3600 && i % 10 != 5 ? "wide" : "other";

    private async Task<List<RedbObject<WideEqualityLeadsProps>>> SeedAsync()
    {
        await SeedLock.WaitAsync();
        try
        {
            var all = await Redb.Query<WideEqualityLeadsProps>().ToListAsync();
            if (all.Count == Objects)
                return all;
            if (all.Count > 0)
                await Redb.DeleteAsync(all.Select(o => o.Id).ToList());

            var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var objects = Enumerable.Range(0, Objects).Select(i => new RedbObject<WideEqualityLeadsProps>
            {
                name = $"wide-equality-{i:0000}",
                Props = new WideEqualityLeadsProps
                {
                    Tag = Tag(i),
                    Bucket = Tag(i) switch { "rare" => 1, "wide" => 2, _ => 3 },
                    Score = (i * 37L) % Objects,
                    When = i % 50 == 7 ? null : start.AddMinutes((i * 53L) % Objects)
                }
            }).ToList();
            for (var i = 0; i < objects.Count; i += 1000)
                await Redb.SaveAsync(objects.Skip(i).Take(1000).ToList());
            all = await Redb.Query<WideEqualityLeadsProps>().ToListAsync();
            all.Should().HaveCount(Objects);
            all.Count(o => o.Props.Tag == "wide").Should().BeGreaterThan(redb.Core.Pro.Query.PvtPrefilterPlanner.WideEqualityCap,
                "the wide value is past the cap");
            return all;
        }
        finally
        {
            SeedLock.Release();
        }
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

    [Theory]
    [InlineData("wide", 0)]
    [InlineData("wide", 30)]
    [InlineData("rare", 0)]
    [InlineData("rare", 30)]
    public async Task ByNumberDescending_BothSidesOfTheCap(string tag, int skip)
    {
        var all = await SeedAsync();
        var expected = all.Where(o => o.Props.Tag == tag).OrderByDescending(o => o.Props.Score).ThenBy(o => o.Id)
            .Skip(skip).Take(20).Select(o => o.Id).ToList();

        await EachFormAsync(async form =>
        {
            var page = await Redb.Query<WideEqualityLeadsProps>().Where(p => p.Tag == tag)
                .OrderByDescending(p => p.Score).ThenByRedb(o => o.Id).Skip(skip).Take(20).ToListAsync();
            page.Select(o => o.Id).Should().Equal(expected, $"{tag}, skip {skip}, {form}");

            // The same split as a number, the equality SQL Server can count.
            long bucket = tag == "rare" ? 1 : 2;
            var byBucket = await Redb.Query<WideEqualityLeadsProps>().Where(p => p.Bucket == bucket)
                .OrderByDescending(p => p.Score).ThenByRedb(o => o.Id).Skip(skip).Take(20).ToListAsync();
            byBucket.Select(o => o.Id).Should().Equal(expected, $"bucket {bucket}, skip {skip}, {form}");
        });
    }

    [Theory]
    [InlineData("wide")]
    [InlineData("rare")]
    public async Task ByDateDescending_AbsentDatesComeLast(string tag)
    {
        var all = await SeedAsync();
        // LINQ puts null last when descending.
        var expected = all.Where(o => o.Props.Tag == tag).OrderByDescending(o => o.Props.When).ThenBy(o => o.Id)
            .Take(20).Select(o => o.Id).ToList();
        var lastPage = all.Where(o => o.Props.Tag == tag).OrderByDescending(o => o.Props.When).ThenBy(o => o.Id)
            .Skip(all.Count(o => o.Props.Tag == tag) - 10).Take(20).Select(o => o.Id).ToList();

        await EachFormAsync(async form =>
        {
            var page = await Redb.Query<WideEqualityLeadsProps>().Where(p => p.Tag == tag)
                .OrderByDescending(p => p.When).ThenByRedb(o => o.Id).Take(20).ToListAsync();
            page.Select(o => o.Id).Should().Equal(expected, $"{tag}, first page, {form}");

            var tail = await Redb.Query<WideEqualityLeadsProps>().Where(p => p.Tag == tag)
                .OrderByDescending(p => p.When).ThenByRedb(o => o.Id).Skip(all.Count(o => o.Props.Tag == tag) - 10).Take(20).ToListAsync();
            tail.Select(o => o.Id).Should().Equal(lastPage, $"{tag}, the page with the absent dates, {form}");
        });
    }

    [Theory]
    [InlineData("wide")]
    [InlineData("rare")]
    public async Task Ascending_AndWithAConditionOnTheSortField(string tag)
    {
        var all = await SeedAsync();
        var ascending = all.Where(o => o.Props.Tag == tag).OrderBy(o => o.Props.When).ThenBy(o => o.Id)
            .Take(20).Select(o => o.Id).ToList();
        var bounded = all.Where(o => o.Props.Tag == tag && o.Props.Score >= 100).OrderBy(o => o.Props.Score).ThenBy(o => o.Id)
            .Take(20).Select(o => o.Id).ToList();

        await EachFormAsync(async form =>
        {
            var page = await Redb.Query<WideEqualityLeadsProps>().Where(p => p.Tag == tag)
                .OrderBy(p => p.When).ThenByRedb(o => o.Id).Take(20).ToListAsync();
            page.Select(o => o.Id).Should().Equal(ascending, $"{tag}, ascending, absent dates first, {form}");

            var withRange = await Redb.Query<WideEqualityLeadsProps>().Where(p => p.Tag == tag && p.Score >= 100)
                .OrderBy(p => p.Score).ThenByRedb(o => o.Id).Take(20).ToListAsync();
            withRange.Select(o => o.Id).Should().Equal(bounded, $"{tag}, ascending with Score >= 100, {form}");
        });
    }
}
