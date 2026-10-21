using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProPvtPrefilterEquivalenceTests : PvtPrefilterEquivalenceTestsBase
{
    private readonly SqliteProFixture _fixture;

    public SqliteProPvtPrefilterEquivalenceTests(SqliteProFixture fixture) : base(fixture.Redb)
        => _fixture = fixture;

    /// <summary>
    /// SQLite only, and a performance rule rather than a correctness one, so it is asserted on the
    /// generated SQL instead of on results.
    ///
    /// A multi-branch prefilter under a limit with no ordering makes SQLite abandon
    /// IX__values__object_structure_lookup for MULTI-INDEX OR. Rows stop arriving in _id_object
    /// order, GROUP BY needs a temp B-tree, and the limit no longer stops anything: measured on
    /// 100 000 objects the same query went from 6 ms to 355 ms. Add any ordering and materialisation
    /// happens regardless, so the prefilter wins again (1150 ms against 311 ms) and must stay.
    ///
    /// See SqlitePrefilterGuards.StreamingLimitBeatsPrefilter.
    /// </summary>
    [Fact]
    public async Task UnorderedLimit_DropsMultiBranchPrefilter_ButOrderedKeepsIt()
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            var unordered = await Unordered().ToSqlStringAsync();
            var ordered = await Ordered().ToSqlStringAsync();

            // Asserted on the planner trace rather than on the whole SQL text: the trace names the
            // decision, while a text comparison would also fail on any unrelated formatting change.
            unordered.Should().Contain("-- PVT form: Streams",
                "an unordered limit goes to the streams form, which stops after the first matches; " +
                "a multi-branch prefilter over the aggregate would take that away for a 40x loss");
            unordered.Should().NotContain("AND ((v._id_structure",
                "and no prefilter must be rendered into a value scan");

            ordered.Should().Contain("-- PVT prefilter: Row form",
                "with an ORDER BY the aggregate is materialised either way, so the prefilter is pure " +
                "gain and must still be emitted");
            ordered.Should().Contain("AND ((v._id_structure",
                "and it must actually reach the value scan");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }

        IRedbQueryable<EmployeeProps> Unordered() => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .Take(100);

        // An expression over Id: a plain OrderByRedb(o => o.Id) goes inside the streams form now, and
        // the prefilter under test lives in the aggregate form.
        IRedbQueryable<EmployeeProps> Ordered() => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .OrderByRedb(o => o.Id + 0)
            .Take(100);
    }

    /// <summary>
    /// A page sorted by a Props field and then by others (ThenBy) is led by the first field; the walk is
    /// materialized and read on through the run of the value it stopped at (pvt_bound, pvt_run); a longer
    /// run leaves the page to the streams (weight descending then number in route, 689 thousand points:
    /// 1.7 s -> 41 ms). Compared by the sequence of all keys.
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
            sql.Should().Contain("pvt_bound", "with ThenBy the walk must know where it stopped");

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
    /// With ThenBy the walk of the leading form reads on through the run of equal sort values it stops in
    /// (pvt_run). Take 10 walks 1 000 entries: 5 objects at the top value, then a run of 1 200 equal values
    /// whose smallest titles sit past the first thousand entries. A run longer than the walk (2 100 objects)
    /// leaves the page to the streams. Both pages are compared with the prefilter off by both keys.
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
            sql.Should().Contain("pvt_run", "with ThenBy the walk reads the run it stops in to the end");

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
    /// An aggregate, a grouping and a window over an open range read the range from the index of values
    /// first (weight over 7 500, 410 points of 689 thousand: 1.3 s -> 36-47 ms). Asserted on the SQL of
    /// the grouping; every answer must not change.
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
                .Should().Contain("pvt_probe", "the open range is probed");

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
    /// A page sorted by one Props field whose NULLs the filter rejects is led by that field's index of
    /// values (weight over 0, by weight descending, 689 thousand points: 1.3 s -> 35 ms); under a
    /// WhereRedb it is not. The answer must not change: a page the walk fills, and a page nothing passes,
    /// where only the streams branch answers. Compared by the sequence of sort keys, ties may swap ids.
    /// </summary>
    [Fact]
    public async Task LeadingForm_PageSortedByField_SameKeysAsAggregateForm()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            IRedbQueryable<EmployeeProps> Page(int minAge) => Redb.Query<EmployeeProps>()
                .Where(e => e.Salary > 0m && e.Age >= minAge)
                .OrderByDescending(e => e.Salary).Take(10);

            (await Page(30).ToSqlStringAsync()).Should().Contain("-- PVT leading form:");
            (await Redb.Query<EmployeeProps>()
                .WhereRedb(o => o.Id > 0)
                .Where(e => e.Salary > 0m && e.Age >= 30)
                .OrderByDescending(e => e.Salary).Take(10)
                .ToSqlStringAsync()).Should().NotContain("-- PVT leading form:",
                "under a base filter the objects it keeps may be anywhere in the order of the field");

            foreach (var minAge in new[] { 30, 1000 })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = (await Page(minAge).ToListAsync()).Select(r => r.Props.Salary).ToList();
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = (await Page(minAge).ToListAsync()).Select(r => r.Props.Salary).ToList();
                on.Should().Equal(off, $"the leading form changes the plan, not the answer (Age >= {minAge})");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// An open range is probed from the index of values on a page not sorted by a Props field: fewer
    /// rows than the cap, and the streams start from them (weight over 7 500 among 689 thousand points,
    /// by id: 1.2 s -> 35 ms). A page sorted by that field walks its index instead and gets no probe.
    /// Asserted on the SQL; the answer must not change, with the probe narrow and with it wide.
    /// </summary>
    [Fact]
    public async Task OpenRange_ProbedOnPageById_NotOnPageSortedByTheField()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            IRedbQueryable<EmployeeProps> ById(decimal salary) => Redb.Query<EmployeeProps>()
                .Where(e => e.Salary > salary && e.Age >= 30)
                .OrderByRedb(o => o.Id).Take(100);

            (await ById(70000m).ToSqlStringAsync()).Should().Contain("-- PVT probe:");
            (await Redb.Query<EmployeeProps>()
                .Where(e => e.Salary > 70000m && e.Age >= 30)
                .OrderByDescending(e => e.Salary).Take(100)
                .ToSqlStringAsync()).Should().NotContain("-- PVT probe:",
                "a page sorted by the field walks its index of values");

            foreach (var salary in new[] { 70000m, 0m })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = (await ById(salary).ToListAsync()).Select(r => r.id).ToList();
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = (await ById(salary).ToListAsync()).Select(r => r.id).ToList();
                on.Should().Equal(off, $"the probe changes the plan, not the answer (Salary > {salary})");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// An OR of narrow branches on an ordered page stays on the aggregate form, where the row
    /// prefilter reads every branch from the index of values; as streams it was checked above every
    /// object of the scheme (examples, three branches: 1.3 s against 26 ms). A narrow leaf in the
    /// top-level AND keeps the streams. Asserted on the SQL, and the answer must not change.
    /// </summary>
    [Fact]
    public async Task Streams_NarrowOrGoesToRowPrefilter_NarrowAndKeepsStreams()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            IRedbQueryable<EmployeeProps> NarrowOr() => Redb.Query<EmployeeProps>()
                .Where(e => e.Department == "Engineering" || e.Position == "Developer" || e.LastName == "Smith")
                .OrderByRedb(o => o.Id).Take(100);

            var narrowOr = await NarrowOr().ToSqlStringAsync();
            narrowOr.Should().Contain("Streams not applied, the filter is an OR of narrow branches");
            narrowOr.Should().Contain("-- PVT prefilter: Row form, 3 branch(es)");

            var narrowAnd = await Redb.Query<EmployeeProps>()
                .Where(e => e.Department == "Engineering" && (e.Age > 30 || e.Position == "Developer"))
                .OrderByRedb(o => o.Id).Take(100)
                .ToSqlStringAsync();
            narrowAnd.Should().Contain("-- PVT form: Streams,",
                "an equality in the top-level AND is pushed into its stream");

            var on = (await NarrowOr().ToListAsync()).Select(r => r.id).ToList();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = (await NarrowOr().ToListAsync()).Select(r => r.id).ToList();
            on.Should().Equal(off, "the form changes the plan, not the answer");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }
}
