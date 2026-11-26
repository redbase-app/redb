using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A condition on the value of a dictionary entry, Dict[key] == x, is a leaf the index of values can lead, like a scalar
/// equality: the entry is one row per object, its key on the row. Before, the general form read every entry of the key
/// and sorted them. The answers must not change: every query here runs with the planner off and on and is compared with
/// the answer computed from the loaded objects, at a value on 1 % and on a third of the objects, sorted by id and by
/// another field. Every other key holds the same values, so a lost key cannot pass.
/// </summary>
public abstract class DictionaryEntryLeadsTestsBase
{
    private const int Objects = 300;
    private static readonly SemaphoreSlim SeedLock = new(1, 1);

    protected readonly IRedbService Redb;

    protected DictionaryEntryLeadsTestsBase(IRedbService redb) => Redb = redb;

    private static readonly (int Year, string Quarter) Q1 = (2024, "Q1");

    // 1 % rare, a third wide, the rest other; the same three values under the second key, shifted.
    private static string Grade(int i) => i % 100 == 0 ? "rare" : i % 3 == 0 ? "wide" : "other";

    private async Task<List<RedbObject<DictionaryEntryLeadsProps>>> SeedAsync()
    {
        await SeedLock.WaitAsync();
        try
        {
            var all = await Redb.Query<DictionaryEntryLeadsProps>().ToListAsync();
            if (all.Count == Objects)
                return all;
            if (all.Count > 0)
                await Redb.DeleteAsync(all.Select(o => o.Id).ToList());

            var objects = Enumerable.Range(0, Objects).Select(i => new RedbObject<DictionaryEntryLeadsProps>
            {
                name = $"dict-entry-{i:000}",
                Props = new DictionaryEntryLeadsProps
                {
                    Rank = (i * 7) % Objects,
                    // No Tag and no 2024-Q1 entry on every tenth object: the leaf's row is missing there.
                    Tag = i % 10 == 5 ? null : Grade(i),
                    Reviews = i % 10 == 5 ? new() { [(2024, "Q2")] = Grade(i + 1) } : new() { [Q1] = Grade(i), [(2024, "Q2")] = Grade(i + 1) },
                    Scores = new() { ["math"] = i, ["art"] = Objects - 1 - i },
                    Places = new()
                    {
                        ["home"] = new DictionaryEntryPlace { City = Grade(i), Floor = i % 5 },
                        ["work"] = new DictionaryEntryPlace { City = Grade(i + 1), Floor = i % 7 }
                    }
                }
            }).ToList();
            await Redb.SaveAsync(objects);
            all = await Redb.Query<DictionaryEntryLeadsProps>().ToListAsync();
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

    private static List<long> ById(IEnumerable<RedbObject<DictionaryEntryLeadsProps>> all,
        Func<DictionaryEntryLeadsProps, bool> keep, int take)
        => all.Where(o => keep(o.Props)).Select(o => o.Id).OrderBy(id => id).Take(take).ToList();

    private static List<long> ByRank(IEnumerable<RedbObject<DictionaryEntryLeadsProps>> all,
        Func<DictionaryEntryLeadsProps, bool> keep, int take)
        => all.Where(o => keep(o.Props)).OrderBy(o => o.Props.Rank).ThenBy(o => o.Id).Select(o => o.Id).Take(take).ToList();

    private static string? Review(DictionaryEntryLeadsProps p, (int Year, string Quarter) key)
        => p.Reviews != null && p.Reviews.TryGetValue(key, out var v) ? v : null;

    /// <summary>
    /// A page ordered by the id alone and led by an equality, on a scalar and on a dictionary entry, with Skip and both
    /// directions: SQLite reads the ids off the row of the equality's field (its index of values is in object order within
    /// the value). Every tenth object has no such row at all, which the equality must keep out as before.
    /// </summary>
    [Theory]
    [InlineData("rare", 0)]
    [InlineData("wide", 0)]
    [InlineData("wide", 25)]
    public async Task PageById_LedByEquality_SkipAndMissingRows(string grade, int skip)
    {
        var all = await SeedAsync();
        var key = Q1;
        List<long> Expected(Func<DictionaryEntryLeadsProps, bool> keep, bool descending)
        {
            var ids = all.Where(o => keep(o.Props)).Select(o => o.Id);
            return (descending ? ids.OrderByDescending(id => id) : ids.OrderBy(id => id)).Skip(skip).Take(10).ToList();
        }

        await EachFormAsync(async form =>
        {
            foreach (var descending in new[] { false, true })
            {
                var scalar = Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Tag == grade);
                var scalarPage = await (descending ? scalar.OrderByDescendingRedb(o => o.Id) : scalar.OrderByRedb(o => o.Id))
                    .Skip(skip).Take(10).ToListAsync();
                scalarPage.Select(o => o.Id).Should().Equal(Expected(p => p.Tag == grade, descending),
                    $"scalar {grade}, skip {skip}, {(descending ? "desc" : "asc")}, {form}");

                var entry = Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == grade);
                var entryPage = await (descending ? entry.OrderByDescendingRedb(o => o.Id) : entry.OrderByRedb(o => o.Id))
                    .Skip(skip).Take(10).ToListAsync();
                entryPage.Select(o => o.Id).Should().Equal(Expected(p => Review(p, key) == grade, descending),
                    $"entry {grade}, skip {skip}, {(descending ? "desc" : "asc")}, {form}");
            }
        });
    }

    [Theory]
    [InlineData("rare")]
    [InlineData("wide")]
    public async Task TupleKey_Equality_ById_AndByAnotherField(string grade)
    {
        var all = await SeedAsync();
        var key = Q1;
        bool Keep(DictionaryEntryLeadsProps p) => p.Reviews != null && p.Reviews.TryGetValue(key, out var v) && v == grade;

        await EachFormAsync(async form =>
        {
            var byId = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == grade)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            byId.Select(o => o.Id).Should().Equal(ById(all, Keep, 10), $"by id, {grade}, {form}");

            var byRank = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == grade)
                .OrderBy(p => p.Rank).ThenByRedb(o => o.Id).Take(10).ToListAsync();
            byRank.Select(o => o.Id).Should().Equal(ByRank(all, Keep, 10), $"by rank, {grade}, {form}");

            (await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == grade).CountAsync())
                .Should().Be(all.Count(o => Keep(o.Props)), $"count, {grade}, {form}");
            (await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == grade).AnyAsync())
                .Should().BeTrue($"any, {grade}, {form}");
        });
    }

    [Theory]
    [InlineData(297, 1000)]     // 1 %
    [InlineData(0, 99)]         // a third
    public async Task TextKey_NumberRange_ById_AndByAnotherField(int from, int to)
    {
        var all = await SeedAsync();
        decimal lo = from, hi = to;
        bool Keep(DictionaryEntryLeadsProps p) => p.Scores != null && p.Scores.TryGetValue("math", out var v) && v >= lo && v <= hi;

        await EachFormAsync(async form =>
        {
            var byId = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Scores!["math"] >= lo && p.Scores!["math"] <= hi)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            byId.Select(o => o.Id).Should().Equal(ById(all, Keep, 10), $"by id, {from}-{to}, {form}");

            var byRank = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Scores!["math"] >= lo && p.Scores!["math"] <= hi)
                .OrderBy(p => p.Rank).ThenByRedb(o => o.Id).Take(10).ToListAsync();
            byRank.Select(o => o.Id).Should().Equal(ByRank(all, Keep, 10), $"by rank, {from}-{to}, {form}");

            var open = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Scores!["math"] >= lo)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            open.Select(o => o.Id).Should().Equal(
                ById(all, p => p.Scores != null && p.Scores.TryGetValue("math", out var v) && v >= lo, 10), $"open range, {from}, {form}");
        });
    }

    [Theory]
    [InlineData("rare")]
    [InlineData("wide")]
    public async Task ClassValueField_Equality_SameAnswer(string grade)
    {
        var all = await SeedAsync();
        bool Keep(DictionaryEntryLeadsProps p) => p.Places != null && p.Places.TryGetValue("home", out var v) && v.City == grade;

        await EachFormAsync(async form =>
        {
            var byId = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Places!["home"].City == grade)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            byId.Select(o => o.Id).Should().Equal(ById(all, Keep, 10), $"by id, {grade}, {form}");

            var byRank = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Places!["home"].City == grade)
                .OrderBy(p => p.Rank).ThenByRedb(o => o.Id).Take(10).ToListAsync();
            byRank.Select(o => o.Id).Should().Equal(ByRank(all, Keep, 10), $"by rank, {grade}, {form}");
        });
    }

    [Fact]
    public async Task EntryNextToAnotherCondition_AndAll_SameAnswer()
    {
        var all = await SeedAsync();
        var key = Q1;

        await EachFormAsync(async form =>
        {
            var both = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == "wide" && p.Scores!["art"] < 150m)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            both.Select(o => o.Id).Should().Equal(ById(all,
                p => Review(p, key) == "wide" && p.Scores!["art"] < 150m, 10), $"entry and entry, {form}");

            var withRank = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == "wide" && p.Rank < 30)
                .OrderBy(p => p.Rank).ThenByRedb(o => o.Id).Take(10).ToListAsync();
            withRank.Select(o => o.Id).Should().Equal(ByRank(all,
                p => Review(p, key) == "wide" && p.Rank < 30, 10), $"entry and scalar, {form}");

            var either = await Redb.Query<DictionaryEntryLeadsProps>().Where(p => p.Reviews![key] == "rare" || p.Scores!["math"] == 5m)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            either.Select(o => o.Id).Should().Equal(ById(all,
                p => Review(p, key) == "rare" || p.Scores!["math"] == 5m, 10), $"entry or entry, {form}");

            // On the key every object holds: an absent entry makes no comparison true, so All over 2024-Q1 would be false.
            var everyone = (Year: 2024, Quarter: "Q2");
            (await Redb.Query<DictionaryEntryLeadsProps>().AllAsync(p => p.Reviews![everyone] != "missing"))
                .Should().BeTrue($"all, {form}");
            (await Redb.Query<DictionaryEntryLeadsProps>().AllAsync(p => p.Reviews![everyone] == "wide"))
                .Should().BeFalse($"all, {form}");
        });
    }
}
