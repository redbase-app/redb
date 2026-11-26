using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A page sorted by one field under an open range on another: on SQL Server the range is counted first and drives the
/// page at or below a cap, the statement without the count runs above it. Every side of the cap must give the answer
/// computed from the loaded objects, with the planner off and on: a number and a date range, sorted by a number and a
/// date, ascending (objects without the sort value first) and descending, with Skip.
/// </summary>
public abstract class RareRangePagesTestsBase
{
    private const int Objects = 3000;
    private static readonly SemaphoreSlim SeedLock = new(1, 1);
    private static readonly DateTime Start = new(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    protected readonly IRedbService Redb;

    protected RareRangePagesTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<RedbObject<RareRangeProps>>> SeedAsync()
    {
        await SeedLock.WaitAsync();
        try
        {
            var all = await Redb.Query<RareRangeProps>().ToListAsync();
            if (all.Count == Objects)
                return all;
            if (all.Count > 0)
                await Redb.DeleteAsync(all.Select(o => o.Id).ToList());

            var objects = Enumerable.Range(0, Objects).Select(i => new RedbObject<RareRangeProps>
            {
                name = $"rare-range-{i:0000}",
                Props = new RareRangeProps
                {
                    // A permutation of 0..2999, absent on every 37th object.
                    Score = i % 37 == 0 ? null : (i * 7919L) % Objects,
                    Salary = i % 41 == 0 ? null : 1000m + (i * 131L) % 5000,
                    When = i % 43 == 0 ? null : Start.AddMinutes((i * 211L) % Objects)
                }
            }).ToList();
            for (var i = 0; i < objects.Count; i += 1000)
                await Redb.SaveAsync(objects.Skip(i).Take(1000).ToList());
            all = await Redb.Query<RareRangeProps>().ToListAsync();
            all.Should().HaveCount(Objects);
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

    private static List<long> Page<TKey>(IEnumerable<RedbObject<RareRangeProps>> all, Func<RareRangeProps, bool> keep,
        Func<RareRangeProps, TKey> key, bool descending, int skip)
    {
        // The first object holds none of the three values: no row at all, no Props, and no range keeps it.
        var kept = all.Where(o => o.Props != null && keep(o.Props));
        var ordered = descending ? kept.OrderByDescending(o => key(o.Props)) : kept.OrderBy(o => key(o.Props));
        return ordered.ThenBy(o => o.Id).Skip(skip).Take(20).Select(o => o.Id).ToList();
    }

    [Theory]
    [InlineData(12, false, 0)]
    [InlineData(12, true, 0)]
    [InlineData(900, false, 10)]
    [InlineData(900, true, 0)]
    [InlineData(1500, false, 0)]
    [InlineData(1500, true, 10)]
    [InlineData(2900, false, 10)]
    [InlineData(2900, true, 0)]
    public async Task NumberRange_SortedByNumber(long below, bool descending, int skip)
    {
        var all = await SeedAsync();
        var withSortCondition = Page(all, p => p.Salary > 0 && p.Score < below, p => p.Salary, descending, skip);
        var rangeAlone = Page(all, p => p.Score < below, p => p.Salary, descending, skip);

        await EachFormAsync(async form =>
        {
            var q = Redb.Query<RareRangeProps>().Where(p => p.Salary > 0m && p.Score < below);
            var page = await (descending ? q.OrderByDescending(p => p.Salary) : q.OrderBy(p => p.Salary)).ThenByRedb(o => o.Id)
                .Skip(skip).Take(20).ToListAsync();
            page.Select(o => o.Id).Should().Equal(withSortCondition, $"Salary > 0 and Score < {below}, {form}");

            var alone = Redb.Query<RareRangeProps>().Where(p => p.Score < below);
            var alonePage = await (descending ? alone.OrderByDescending(p => p.Salary) : alone.OrderBy(p => p.Salary)).ThenByRedb(o => o.Id)
                .Skip(skip).Take(20).ToListAsync();
            alonePage.Select(o => o.Id).Should().Equal(rangeAlone, $"Score < {below} alone, objects without Salary where the order puts them, {form}");

            var noTiebreak = await (descending ? alone.OrderByDescending(p => p.Salary) : alone.OrderBy(p => p.Salary))
                .Skip(skip).Take(20).ToListAsync();
            noTiebreak.Should().HaveCount(rangeAlone.Count, $"Score < {below}, no tiebreak, {form}");
        });
    }

    [Theory]
    [InlineData(12, false)]
    [InlineData(12, true)]
    [InlineData(2900, false)]
    [InlineData(2900, true)]
    public async Task NumberRange_SortedByDate(long below, bool descending)
    {
        var all = await SeedAsync();
        var expected = Page(all, p => p.Score < below, p => p.When, descending, 0);

        await EachFormAsync(async form =>
        {
            var q = Redb.Query<RareRangeProps>().Where(p => p.Score < below);
            var page = await (descending ? q.OrderByDescending(p => p.When) : q.OrderBy(p => p.When)).ThenByRedb(o => o.Id)
                .Take(20).ToListAsync();
            page.Select(o => o.Id).Should().Equal(expected, $"Score < {below} by When, {form}");
        });
    }

    [Theory]
    [InlineData(12, false)]
    [InlineData(12, true)]
    [InlineData(2900, false)]
    [InlineData(2900, true)]
    public async Task DateRange_SortedByNumber(int minutes, bool descending)
    {
        var all = await SeedAsync();
        var before = Start.AddMinutes(minutes);
        var expected = Page(all, p => p.When < before, p => p.Salary, descending, 0);

        await EachFormAsync(async form =>
        {
            var q = Redb.Query<RareRangeProps>().Where(p => p.When < before);
            var page = await (descending ? q.OrderByDescending(p => p.Salary) : q.OrderBy(p => p.Salary)).ThenByRedb(o => o.Id)
                .Take(20).ToListAsync();
            page.Select(o => o.Id).Should().Equal(expected, $"When before {minutes} minutes by Salary, {form}");
        });
    }

    [Fact]
    public async Task ThePreviewRunsNothing_AndShowsTheBatch()
    {
        await SeedAsync();
        var sql = await Redb.Query<RareRangeProps>().Where(p => p.Score < 12).OrderBy(p => p.Salary).Take(20).ToSqlStringAsync();
        sql.Should().NotBeNullOrWhiteSpace();
        var commands = await Helpers.CommandCounter.CountAsync(async () =>
            await Redb.Query<RareRangeProps>().Where(p => p.Score < 12).OrderBy(p => p.Salary).Take(20).ToSqlStringAsync());
        commands.Should().Be(0, "a preview builds the statement, it runs nothing");
    }
}
