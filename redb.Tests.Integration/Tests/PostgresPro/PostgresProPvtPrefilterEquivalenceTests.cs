using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProPvtPrefilterEquivalenceTests : PvtPrefilterEquivalenceTestsBase
{
    public PostgresProPvtPrefilterEquivalenceTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    /// <summary>
    /// PostgreSQL only: an aggregate, a grouping and a window over an open range read the range from the
    /// index of values first, as a page does (weight over 7 500, 410 points of 689 thousand: 330-410 ms
    /// -> 9-11 ms). Asserted on the SQL of the grouping; every answer must not change.
    /// </summary>
    [Fact]
    public async Task OpenRange_ProbedInAggregateGroupingAndWindow()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            IRedbQueryable<EmployeeProps> Q() => Redb.Query<EmployeeProps>().Where(e => e.Salary > 70000m && e.Age >= 30);

            (await Q().GroupBy(x => x.Department)
                .ToSqlStringAsync(g => new { g.Key, Count = Agg.Count(g) }))
                .Should().Contain("pvt_narrow", "the open range is probed");

            async Task<string> Answers()
            {
                var agg = await Q().AggregateAsync(x => new { Sum = Agg.Sum(x.Props.Salary), Count = Agg.Count() });
                var groups = (await Q().GroupBy(x => x.Department)
                    .SelectAsync(g => new { g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, x => x.Salary) }))
                    .OrderBy(g => g.Key).ToList();
                var ranked = (await Q().Take(20)
                    .WithWindow(w => w.PartitionBy(x => x.Department).OrderByDesc(x => x.Salary))
                    .SelectAsync(x => new { x.Props.Salary, Rank = Win.RowNumber() }))
                    .Select(x => x.Salary).OrderByDescending(s => s).ToList();
                return System.Text.Json.JsonSerializer.Serialize(new { agg, groups, ranked });
            }

            var on = await Answers();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await Answers();
            on.Should().Be(off, "the probe changes the plan, not the answer");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: a page sorted by a Props field and then by others (ThenBy) is led by the first
    /// field, and the walk reads on through the run of the value it stops at (pvt_bound); a run of equal
    /// first keys cut by the walk would come out in the wrong order of the rest. Pages of 689 thousand
    /// points 548-592 ms -> 10-11 ms. Compared by the sequence of both keys.
    /// </summary>
    [Fact]
    public async Task LeadingForm_ThenBy_SameKeysAsAggregateForm()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            IRedbQueryable<EmployeeProps> Page(int skip) => Redb.Query<EmployeeProps>()
                .Where(e => e.Salary > 0m && e.Age >= 20)
                .OrderByDescending(e => e.Salary).ThenBy(e => e.Age).ThenBy(e => e.LastName)
                .Skip(skip).Take(10);

            var sql = await Page(0).ToSqlStringAsync();
            sql.Should().Contain("-- PVT leading form:");
            sql.Should().Contain("pvt_bound", "with ThenBy the walk must know where it stops");

            foreach (var skip in new[] { 0, 5 })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = (await Page(skip).ToListAsync()).Select(r => (r.Props.Salary, r.Props.Age, r.Props.LastName)).ToList();
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = (await Page(skip).ToListAsync()).Select(r => (r.Props.Salary, r.Props.Age, r.Props.LastName)).ToList();
                on.Should().Equal(off, $"the leading form changes the plan, not the order (Skip {skip})");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: with ThenBy the walk of the leading form reads on to the end of the run of equal
    /// sort values it stops in (pvt_bound, pvt_span). Take 10 walks 1 000 entries: 5 objects at the top
    /// value, then a run of 1 200 equal values whose smallest titles sit past the first thousand entries.
    /// A run longer than twice the walk (2 100 objects) leaves the page to the streams. Both pages are
    /// compared with the prefilter off by the sequence of both keys.
    /// </summary>
    [Fact]
    public async Task LeadingForm_ThenBy_RunOfEqualValuesAtTheBound()
    {
        const long top = 9_000_100, run = 9_000_090, longRun = 9_000_080;
        var objects = new List<RedbObject<SimpleProps>>();
        void Add(long value, int count)
        {
            // Titles fall as the objects are created, so the first titles of a run are its last entries.
            for (var i = 0; i < count; i++)
                objects.Add(new RedbObject<SimpleProps>
                {
                    name = $"run-{value}-{i}",
                    Props = new SimpleProps { Title = $"t{value}-{count - i:D5}", OptionalNumber = value }
                });
        }
        Add(top, 5);
        Add(run, 1200);
        Add(longRun, 2100);
        await Redb.SaveAsync(objects);

        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<SimpleProps> Run() => Redb.Query<SimpleProps>()
                .Where(p => p.OptionalNumber >= run)
                .OrderByDescending(p => p.OptionalNumber).ThenBy(p => p.Title)
                .Take(10);
            IRedbQueryable<SimpleProps> LongRun() => Redb.Query<SimpleProps>()
                .Where(p => p.OptionalNumber >= longRun)
                .OrderBy(p => p.OptionalNumber).ThenBy(p => p.Title)
                .Take(10);

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var sql = await Run().ToSqlStringAsync();
            sql.Should().Contain("-- PVT leading form:");
            sql.Should().Contain("pvt_span", "with ThenBy the walk reads the run it stops in to the end");

            foreach (var (name, page) in new[] { ("run", (Func<IRedbQueryable<SimpleProps>>)Run), ("long run", LongRun) })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = (await page().ToListAsync()).Select(r => (r.Props.OptionalNumber, r.Props.Title)).ToList();
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = (await page().ToListAsync()).Select(r => (r.Props.OptionalNumber, r.Props.Title)).ToList();
                on.Should().HaveCount(10);
                on.Should().Equal(off, $"the leading form changes the plan, not the order ({name})");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: under a filter on the object's own columns (WhereRedb) a text field is read by a probe per
    /// object (LATERAL), not by a hash join over every row of the field in the scheme; a number field keeps the plain
    /// join, its value is in the index. A window of 2 843 objects of 50 000 with 128 MB of shared buffers took
    /// 1.4-5 s for a grouping or a null check by a text field. Asserted on the SQL; the answers must not change.
    /// </summary>
    [Fact]
    public async Task WhereRedb_TextFieldReadPerObject_SameAnswers()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            var from = DateTime.UtcNow.AddDays(-1);
            IRedbQueryable<EmployeeProps> Window() => Redb.Query<EmployeeProps>().WhereRedb(o => o.DateCreate >= from);

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var sql = await Window().Where(e => e.EmployeeCode == null).Take(10).ToSqlStringAsync();
            sql.Should().Contain("LEFT JOIN LATERAL (SELECT f._string", "the text field is read per object of the window");
            (await Window().Where(e => e.Age > 30).Take(10).ToSqlStringAsync())
                .Should().NotContain("LATERAL", "a number field is in the index of values and keeps the plain join");

            async Task<(int Nulls, List<(string Key, int Count)> Groups)> AnswersAsync() => (
                await Window().Where(e => e.EmployeeCode == null).CountAsync(),
                (await Window().GroupBy(e => e.Department).SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) }))
                    .Select(g => (g.Key, g.Count)).OrderBy(g => g.Key).ToList());

            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Nulls.Should().Be(off.Nulls);
            on.Nulls.Should().BeGreaterThan(0, "the seed leaves every third employee without a code");
            on.Groups.Should().Equal(off.Groups);
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: an OR of narrow branches that the row prefilter cannot take (a sort, a projection or an aggregate
    /// over another field, a leaf next to the OR) is probed: its branches are read from the index of values up to the cap
    /// and seed the streams (a sort by another field over 100 000 objects: 224 ms -> 13 ms). The pure OR keeps the row
    /// prefilter; an OR with a wide branch and an OR under NOT are not narrowed. Every shape, pages with Skip included,
    /// must answer as with the prefilter off.
    /// </summary>
    [Fact]
    public async Task NarrowOr_ProbedWhereTheRowPrefilterCannot_SameAnswers()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var from = DateTime.UtcNow.AddDays(-1);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<EmployeeProps> Or() => Redb.Query<EmployeeProps>().Where(e => e.Department == "Engineering" || e.Position == "Developer");
            const string orProbe = "-- PVT probe: an OR of narrow branches";

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            (await Or().OrderBy(e => e.Salary).Skip(3).Take(5).ToSqlStringAsync())
                .Should().Contain(orProbe, "a sort by another field leaves the OR to the streams");
            (await Redb.Query<EmployeeProps>().Where(e => e.LastName == "Last005" && (e.Department == "Engineering" || e.Position == "Developer"))
                    .Take(5).ToSqlStringAsync())
                .Should().Contain(orProbe, "next to a leaf the probe decides which of the two drives");
            var pure = await Or().ToSqlStringAsync();
            pure.Should().Contain("-- PVT prefilter: Row form").And.NotContain(orProbe, "the pure OR keeps the row prefilter");
            (await Or().GroupBy(e => e.Position).ToSqlStringAsync(g => new { g.Key, Count = Agg.Count(g) }))
                .Should().Contain("pvt_narrow", "a grouping over the OR is probed");
            (await Redb.Query<EmployeeProps>().Where(e => e.Department == "Engineering" || e.Salary > 60000m).OrderBy(e => e.Age).Take(5).ToSqlStringAsync())
                .Should().NotContain(orProbe, "a wide branch leaves nothing to narrow");
            (await Redb.Query<EmployeeProps>().Where(e => !(e.Department == "Engineering" || e.Position == "Developer")).OrderBy(e => e.Age).Take(5).ToSqlStringAsync())
                .Should().NotContain(orProbe, "an OR under NOT does not bound the objects");

            static string Ids(IEnumerable<IRedbObject> rows) => string.Join(",", rows.Select(r => r.Id));
            async Task<string> AnswersAsync()
            {
                var parts = new List<string>
                {
                    "sort other: " + Ids(await Or().OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Skip(3).Take(5).ToListAsync()),
                    // Other tests seed the same names into this database: the id settles ties.
                    "sort own: " + Ids(await Or().OrderBy(e => e.Department).ThenBy(e => e.LastName).ThenByRedb(o => o.Id).Skip(2).Take(5).ToListAsync()),
                    "sort id: " + Ids(await Or().OrderByRedb(o => o.Id).Skip(2).Take(5).ToListAsync()),
                    "leading: " + Ids(await Redb.Query<EmployeeProps>().Where(e => (e.Department == "Engineering" || e.Position == "Developer") && e.Salary > 0m)
                        .OrderByDescending(e => e.Salary).ThenBy(e => e.Age).ThenByRedb(o => o.Id).Skip(1).Take(5).ToListAsync()),
                    "projection: " + string.Join(",", (await Or().OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Take(5)
                        .Select(x => new { x.Props.LastName, x.Props.Salary }).ToListAsync()).Select(x => $"{x.LastName}:{x.Salary}")),
                    $"count: {await Or().CountAsync()} any: {await Or().AnyAsync()}",
                    $"leaf next to it: {await Redb.Query<EmployeeProps>().Where(e => e.LastName == "Last005" && (e.Department == "Engineering" || e.Position == "Developer")).CountAsync()}",
                    "range next to it: " + Ids(await Redb.Query<EmployeeProps>().Where(e => (e.Department == "Engineering" || e.Position == "Developer") && e.Age > 30)
                        .OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Skip(1).Take(5).ToListAsync()),
                    $"sum: {await Or().AggregateAsync(x => new { Sum = Agg.Sum(x.Props.Salary), Count = Agg.Count() })}",
                    "groups: " + string.Join(",", (await Or().GroupBy(e => e.Position).SelectAsync(g => new { g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, x => x.Salary) }))
                        .OrderBy(g => g.Key).Select(g => $"{g.Key}:{g.Count}:{g.Sum}")),
                    "window: " + string.Join(",", (await Or().Take(10).WithWindow(w => w.PartitionBy(x => x.Department).OrderByDesc(x => x.Salary))
                        .SelectAsync(x => new { x.Props.Salary, Rank = Win.RowNumber() })).Select(x => x.Salary).OrderByDescending(s => s)),
                    $"whereredb: {await Or().WhereRedb(o => o.DateCreate >= from).CountAsync()}",
                    $"wide branch: {await Redb.Query<EmployeeProps>().Where(e => e.Department == "Engineering" || e.Salary > 60000m).CountAsync()}",
                    $"under not: {await Redb.Query<EmployeeProps>().Where(e => !(e.Department == "Engineering" || e.Position == "Developer")).CountAsync()}",
                };
                return string.Join("\n", parts);
            }

            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "the probe of the OR changes the plan, not the answer");
            on.Should().NotContain("count: 0 ", "the seed has engineers and developers");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: both sides of the cap of the OR probe (5 000 rows). Ten objects match the OR of a small tag, 5 100
    /// the OR of a large one: the first is seeded from the probe, the second keeps the form it has without the probe.
    /// Pages with Skip, counts and a grouping must answer as with the prefilter off on both sides.
    /// </summary>
    [Fact]
    public async Task NarrowOr_BothSidesOfTheProbeCap_SameAnswers()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var (small, large) = ($"or-small-{tag}", $"or-large-{tag}");
        var objects = Enumerable.Range(0, 5110).Select(i => (IRedbObject<SimpleProps>)new RedbObject<SimpleProps>
        {
            name = $"or-cap-{tag}-{i}",
            Props = new SimpleProps { Title = i < 10 ? small : large, Price = 1000m - i % 997, Count = i % 7, IsActive = i % 2 == 0, Description = $"d-{tag}-{i}" }
        }).ToList();
        await Redb.AddNewObjectsAsync(objects);

        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<SimpleProps> Or(string title) => Redb.Query<SimpleProps>().Where(p => p.Title == title || p.Description == $"d-{tag}-none");

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            foreach (var title in new[] { small, large })
                (await Or(title).OrderBy(p => p.Price).Take(5).ToSqlStringAsync())
                    .Should().Contain("-- PVT probe: an OR of narrow branches", "the cap is checked when the query runs, the SQL holds both forms");

            async Task<string> AnswersAsync(string title) => string.Join("\n",
                string.Join(",", (await Or(title).OrderBy(p => p.Price).ThenByRedb(o => o.Id).Skip(4).Take(5).ToListAsync()).Select(r => r.Id)),
                string.Join(",", (await Or(title).OrderByDescending(p => p.Count).ThenBy(p => p.Price).ThenByRedb(o => o.Id).Skip(1).Take(3).ToListAsync()).Select(r => r.Id)),
                $"{await Or(title).CountAsync()}",
                string.Join(",", (await Or(title).GroupBy(p => p.IsActive).SelectAsync(g => new { g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, x => x.Price) }))
                    .OrderBy(g => g.Key).Select(g => $"{g.Key}:{g.Count}:{g.Sum}")));

            foreach (var title in new[] { small, large })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = await AnswersAsync(title);
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = await AnswersAsync(title);
                on.Should().Be(off, $"the probe changes the plan, not the answer ({title})");
            }
            (await Or(large).CountAsync()).Should().Be(5100, "the large tag is past the cap");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: under WhereRedb the text field of a narrow equality or IN of the top-level AND is joined plainly,
    /// not by the LATERAL probe per object, so its condition reaches the index of text values and PostgreSQL may start
    /// from the matching values (one department among 95 000 objects of a wide window: 166 ms -> 2.5 ms). The id
    /// conditions of the top-level AND are repeated on that join; one under OR or NOT is not. Another text field keeps
    /// the LATERAL read. Asserted on the SQL; every answer must equal the one with the prefilter off.
    /// </summary>
    [Fact]
    public async Task WhereRedb_NarrowTextLeafJoinedPlainly_IdBoundsCarried_SameAnswers()
    {
        var ids = (await TestDataFactory.SeedEmployees(Redb, 40)).OrderBy(i => i).ToList();
        var (low, high) = (ids[5], ids[30]);
        var picked = new[] { ids[2], ids[3], ids[10], ids[11] };
        var one = ids[2];
        var from = DateTime.UtcNow.AddDays(-1);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<EmployeeProps> Engineering() => Redb.Query<EmployeeProps>().Where(e => e.Department == "Engineering");
            var shapes = new (string Name, Func<IRedbQueryable<EmployeeProps>> Query, string[] Carried)[]
            {
                ("date window", () => Engineering().WhereRedb(o => o.DateCreate >= from), []),
                ("id range", () => Engineering().WhereRedb(o => o.Id > low), [">"]),
                ("id both ends", () => Engineering().WhereRedb(o => o.Id >= low && o.Id < high), [">=", "<"]),
                ("id both ends and a date", () => Engineering().WhereRedb(o => o.Id > low && o.DateCreate >= from && o.Id <= high), [">", "<="]),
                ("id equality", () => Engineering().WhereRedb(o => o.Id == one), ["="]),
                ("id in", () => Engineering().WhereInRedb(o => o.Id, picked), ["= ANY("]),
                ("id under or", () => Engineering().WhereRedb(o => o.Id > high || o.Id < low), []),
                ("id under not", () => Engineering().WhereRedb(o => !(o.Id > low && o.Id < high)), []),
            };

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            foreach (var (name, query, carried) in shapes)
            {
                var sql = await query().Take(10).ToSqlStringAsync();
                sql.Should().NotContain("LATERAL", $"the equality's own text field is joined plainly ({name})");
                // The id conditions on the join, not the join itself (f0._id_object = o_src._id).
                var bounds = System.Text.RegularExpressions.Regex.Matches(sql, @"f\d+\._id_object (>=|<=|>|<|= ANY\(|=)(?! o_src\.)")
                    .Select(m => m.Groups[1].Value).OrderBy(s => s, StringComparer.Ordinal).ToArray();
                bounds.Should().Equal(carried.OrderBy(s => s, StringComparer.Ordinal), $"the id bounds carried over ({name})");
            }

            // A text field that is not the chosen leaf keeps its LATERAL read; the equality's field does not.
            var mixed = await Engineering().Where(e => e.LastName != "nobody").WhereRedb(o => o.Id > low).Take(10).ToSqlStringAsync();
            System.Text.RegularExpressions.Regex.Matches(mixed, "LEFT JOIN LATERAL").Count
                .Should().Be(1, "LastName is read per object, Department is joined plainly");

            async Task<string> AnswersAsync()
            {
                var parts = new List<string>();
                foreach (var (name, query, _) in shapes)
                {
                    var count = await query().CountAsync();
                    var page = (await query().OrderByRedb(o => o.Id).Take(5).ToListAsync()).Select(r => r.id);
                    var any = await query().AnyAsync();
                    parts.Add($"{name}: {count} {any} [{string.Join(",", page)}]");
                }
                parts.Add($"mixed: {await Engineering().Where(e => e.LastName != "nobody").WhereRedb(o => o.Id > low).CountAsync()}");
                return string.Join("; ", parts);
            }

            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "the plain join and the carried bounds change the plan, not the answer");
            on.Should().Contain("id range: ").And.NotContain("id range: 0 ", "the seed has engineers past the bound");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: an aggregate without any condition reads each field from the index range of that field alone, the
    /// trash subtracted, and counts the objects in _objects (Sum of one field over 100 000 objects: 35 ms -> 18 ms). Its
    /// answers must equal the joined form's (prefilter off) for numbers of every column type, nulls, a nested-class field,
    /// base fields, an empty scheme and objects in the trash; an array field, a filter or WhereRedb keep the other forms.
    /// This also guards the invariant the form relies on: the values of a structure of the scheme belong to objects of the
    /// scheme or to the trash.
    /// </summary>
    [Fact]
    public async Task UnfilteredAggregate_ReadFromTheIndexOfValues_SameAnswers()
    {
        var ids = await TestDataFactory.SeedEmployees(Redb, 30);
        await Redb.SoftDeleteAsync(new[] { ids[1], ids[2], ids[7] });
        await Redb.SyncSchemeAsync<UnfilteredAggregateEmptyProps>();
        const string fromValues = "AND v._array_index IS NULL AND v._id_object NOT IN (SELECT t._id FROM _objects t WHERE t._id_scheme = -10))";
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            IRedbQueryable<EmployeeProps> All() => Redb.Query<EmployeeProps>();
            (await All().ToAggregateSqlStringAsync(x => new { S = Agg.Sum(x.Props.Salary), C = Agg.Count() }))
                .Should().Contain(fromValues, "no condition: the field is read from its index range");
            (await All().Where(e => e.Age > 0).ToAggregateSqlStringAsync(x => new { S = Agg.Sum(x.Props.Salary) }))
                .Should().NotContain(fromValues, "a filter keeps the streams form");
            (await All().WhereRedb(o => o.Id > 0).ToAggregateSqlStringAsync(x => new { S = Agg.Sum(x.Props.Salary) }))
                .Should().NotContain(fromValues, "WhereRedb keeps the streams form");
            (await All().ToAggregateSqlStringAsync(x => new { S = Agg.Sum(x.Props.SkillLevels!) }))
                .Should().NotContain(fromValues, "an array is aggregated over its element rows elsewhere");

            async Task<string> AnswersAsync() => System.Text.Json.JsonSerializer.Serialize(new object[]
            {
                await All().AggregateAsync(x => new
                {
                    Sum = Agg.Sum(x.Props.Salary), Avg = Agg.Average(x.Props.Salary), MinAge = Agg.Min(x.Props.Age),
                    MaxAge = Agg.Max(x.Props.Age), SumAge = Agg.Sum(x.Props.Age), AvgAge = Agg.Average(x.Props.Age), Count = Agg.Count()
                }),
                // Rating is a double, null on every fourth employee.
                await All().AggregateAsync(x => new { Sum = Agg.Sum(x.Props.Rating), Min = Agg.Min(x.Props.Rating), Max = Agg.Max(x.Props.Rating), Count = Agg.Count() }),
                await All().AggregateAsync(x => new { Floors = Agg.Sum(x.Props.HomeAddress!.Building!.Floor), Top = Agg.Max(x.Props.HomeAddress!.Building!.Floor) }),
                await All().AggregateAsync(x => new { FirstId = Agg.Min(x.Id), LastId = Agg.Max(x.Id), Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary) }),
                await All().AggregateAsync(x => new { Skills = Agg.Sum(x.Props.SkillLevels!) }),
                await Redb.Query<UnfilteredAggregateEmptyProps>().AggregateAsync(x => new
                {
                    Sum = Agg.Sum(x.Props.Number), Min = Agg.Min(x.Props.Amount), Max = Agg.Max(x.Props.Ratio), Count = Agg.Count()
                }),
                await All().CountAsync(),
            });

            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "reading the index of values changes the plan, not the answer");
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            (await All().AggregateAsync(x => new { Count = Agg.Count() })).Count
                .Should().Be(await All().CountAsync(), "COUNT(*) counts the live objects, not the rows of a field");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: AllAsync over one text field without any other condition first looks for a counterexample among the
    /// first 1 000 objects of the scheme, then compares the objects of the scheme with the values of the field that satisfy
    /// the predicate (165 ms -> 35 ms over 100 000 objects when All holds). 1 100 objects of a scheme of their own put the
    /// counterexamples past the probe, so the counts decide: a different value at the end, an object without the value, the
    /// same object moved to the trash; a pattern; All after Where and WhereRedb and over a number field keep their forms.
    /// Every answer must equal the one with the prefilter off.
    /// </summary>
    [Fact]
    public async Task AllOfATextField_ProbeThenCounts_SameAnswers()
    {
        var objects = Enumerable.Range(0, 1100).Select(i => (IRedbObject<AllTextProbeProps>)new RedbObject<AllTextProbeProps>
        {
            name = $"all-text-{i}",
            // The last object holds another name; the one before it has no note.
            Props = new AllTextProbeProps { Name = i == 1099 ? "b" : "a", Note = i == 1098 ? null : "n" }
        }).ToList();
        var ids = await Redb.AddNewObjectsAsync(objects);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<AllTextProbeProps> Q() => Redb.Query<AllTextProbeProps>();
            async Task<string> AnswersAsync() => string.Join(",",
                await Q().AllAsync(p => p.Name == "a"),
                await Q().AllAsync(p => p.Name != "zzz"),
                await Q().AllAsync(p => p.Name.StartsWith("a")),
                await Q().AllAsync(p => p.Note != null),
                await Q().AllAsync(p => p.Note == "n"),
                await Q().Where(p => p.Note == "n").AllAsync(p => p.Name == "a"),
                await Q().WhereRedb(o => o.Id < ids[1099]).AllAsync(p => p.Name == "a"));

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "the probe and the counts change the plan, not the answer");
            on.Should().Be("False,True,False,False,False,False,True",
                "one late other name, one object without a note; Where keeps the object with another name");

            // The objects past the probe moved to the trash: their values stay until the purge and must not count.
            await Redb.SoftDeleteAsync(new[] { ids[1098], ids[1099] });
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var trashedOn = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var trashedOff = await AnswersAsync();
            trashedOn.Should().Be(trashedOff, "the trash is subtracted from the values counted");
            trashedOn.Should().Be("True,True,True,True,True,True,True", "the objects left all hold the name and the note");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: a grouping, an aggregate or a window driven off a narrow leaf reads each text field by its own row per
    /// object (LATERAL), not through the index by object that holds every row of the object (the Pachot grouping by the nested
    /// lab name, 12 575 objects with 11 fields each: 86 ms -> 53 ms). A text field the filter narrows keeps its plain join,
    /// so its condition reaches the index of text values. Asserted on the SQL; every answer must equal the one with the
    /// prefilter off, for a wide set, a narrow one and a narrow title next to a wide count.
    /// </summary>
    [Fact]
    public async Task TextReadAfterANarrowLeaf_ByItsOwnRow_SameAnswers()
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var objects = Enumerable.Range(0, 610).Select(i => (IRedbObject<SimpleProps>)new RedbObject<SimpleProps>
        {
            name = $"text-read-{tag}-{i}",
            // 600 objects with Count 777 011 and Price 41; 10 with Count 777 012 and Price 42. The description groups them
            // into seven, one of them without a value.
            Props = new SimpleProps
            {
                Title = $"t-{tag}-{i}", Count = i < 600 ? 777_011 : 777_012, Price = i < 600 ? 41m : 42m,
                Description = i % 7 == 0 ? null : $"d-{tag}-{i % 7}", IsActive = i % 2 == 0
            }
        }).ToList();
        await Redb.AddNewObjectsAsync(objects);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<SimpleProps> Wide() => Redb.Query<SimpleProps>().Where(p => p.Count == 777_011 && p.Price == 41m);
            IRedbQueryable<SimpleProps> Narrow() => Redb.Query<SimpleProps>().Where(p => p.Count == 777_012 && p.Price == 42m);
            var title = $"t-{tag}-17";
            IRedbQueryable<SimpleProps> Tie() => Redb.Query<SimpleProps>().Where(p => p.Count == 777_011 && p.Title == title);

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            (await Wide().GroupBy(p => p.Description).ToSqlStringAsync(g => new { g.Key, C = Agg.Count(g) }))
                .Should().Contain("LEFT JOIN LATERAL (SELECT f._string", "the description is read by its own row per object");
            var tie = await Tie().GroupBy(p => p.Description).ToSqlStringAsync(g => new { g.Key, C = Agg.Count(g) });
            System.Text.RegularExpressions.Regex.Matches(tie, "LEFT JOIN LATERAL").Count
                .Should().Be(1, "the title the filter narrows keeps its plain join, the description goes per object");

            async Task<string> AnswersAsync(Func<IRedbQueryable<SimpleProps>> q) => string.Join("|",
                string.Join(",", (await q().GroupBy(p => p.Description).SelectAsync(g => new { g.Key, C = Agg.Count(g), S = Agg.Sum(g, x => x.Price) }))
                    .Select(g => $"{g.Key}:{g.C}:{g.S}").OrderBy(s => s, StringComparer.Ordinal)),
                string.Join(",", (await q().GroupBy(p => p.IsActive).SelectAsync(g => new { g.Key, C = Agg.Count(g), M = Agg.Max(g, x => x.Description) }))
                    .Select(g => $"{g.Key}:{g.C}:{g.M}").OrderBy(s => s, StringComparer.Ordinal)),
                $"{await q().AggregateAsync(x => new { S = Agg.Sum(x.Props.Price), C = Agg.Count() })}");

            foreach (var (name, q) in new[] { ("wide", (Func<IRedbQueryable<SimpleProps>>)Wide), ("narrow", Narrow), ("tie", Tie) })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = await AnswersAsync(q);
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = await AnswersAsync(q);
                on.Should().Be(off, $"reading the text by its own row changes the plan, not the answer ({name})");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// PostgreSQL only: the OFFSET 0 fence of the streams form stays for a wide filter and is dropped
    /// when the top-level AND holds a narrow leaf. With the fence such a leaf was checked above the
    /// streams after every object of the scheme had been joined: 2.4 s for one order number among
    /// 689 thousand points, 31 ms without it. Asserted on the SQL, and the answer must not change.
    /// </summary>
    [Fact]
    public async Task Streams_NarrowLeafDropsTheFence_WideFilterKeepsIt()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            IRedbQueryable<EmployeeProps> Narrow() => Redb.Query<EmployeeProps>()
                .Where(e => e.Department == "Engineering" && e.Age > 30)
                .OrderByRedb(o => o.Id).Take(100);
            IRedbQueryable<EmployeeProps> Wide() => Redb.Query<EmployeeProps>()
                .Where(e => e.Age > 30 && e.Department != "")
                .OrderByRedb(o => o.Id).Take(100);

            var narrow = await Narrow().ToSqlStringAsync();
            narrow.Should().Contain("-- PVT form: Streams,");
            narrow.Should().Contain("-- PVT streams fence: dropped",
                "an equality in the top-level AND is narrow by its shape");
            narrow.Should().NotContain("OFFSET 0");

            var wide = await Wide().ToSqlStringAsync();
            wide.Should().Contain("-- PVT form: Streams,");
            wide.Should().Contain("OFFSET 0",
                "an open range and an inequality are wide by their shape, the fence keeps merge joins");

            // An OR of narrow branches cannot be pushed into one stream: it stays on the aggregate
            // form, where the row prefilter reads every branch from the index.
            var narrowOr = await Redb.Query<EmployeeProps>()
                .Where(e => e.Department == "Engineering" || e.Position == "Developer" || e.LastName == "Smith")
                .ToSqlStringAsync();
            narrowOr.Should().Contain("Streams not applied, the filter is an OR of narrow branches");
            narrowOr.Should().Contain("-- PVT prefilter: Row form, 3 branch(es)");

            var on = (await Narrow().ToListAsync()).Select(r => r.id).ToList();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = (await Narrow().ToListAsync()).Select(r => r.id).ToList();
            on.Should().Equal(off, "dropping the fence changes the plan, not the answer");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }
}
