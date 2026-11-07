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
