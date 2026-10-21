using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProPvtPrefilterEquivalenceTests : PvtPrefilterEquivalenceTestsBase
{
    public MsSqlProPvtPrefilterEquivalenceTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    /// <summary>
    /// An OR of narrow branches stays on the aggregate form, where the row prefilter reads every
    /// branch from the index of values; as streams it was checked above every object of the scheme
    /// (examples, three string branches: 1 694 ms against 95 ms). A narrow leaf in the top-level AND
    /// keeps the streams. Asserted on the SQL, and the answer must not change.
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

    /// <summary>
    /// A page sorted by a Props field and then by others (ThenBy) is led by the first field as a batch:
    /// the walk fills a table variable, the streams fill it when the walk did not. Without ThenBy the
    /// streams stay, SQL Server walks the index on its own there. Compared by the sequence of all keys,
    /// ThenBy Id included.
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
            IRedbQueryable<EmployeeProps> ById() => Redb.Query<EmployeeProps>()
                .Where(e => e.Age >= 20)
                .OrderBy(e => e.Age).ThenByRedb(o => o.Id)
                .Take(10);

            var sql = await Page(0).ToSqlStringAsync();
            sql.Should().Contain("-- PVT leading form:");
            sql.Should().Contain("@pvt_bound", "with ThenBy the walk reads the run it stops in to the end");

            var withoutThenBy = await Redb.Query<EmployeeProps>()
                .Where(e => e.Salary > 0m && e.Age >= 20)
                .OrderByDescending(e => e.Salary).Take(10)
                .ToSqlStringAsync();
            withoutThenBy.Should().NotContain("-- PVT leading form:", "without ThenBy SQL Server walks the index itself");

            foreach (var skip in new[] { 0, 5 })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
                var on = (await Page(skip).ToListAsync()).Select(r => (r.Props.Salary, r.Props.Age, r.Props.LastName)).ToList();
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
                var off = (await Page(skip).ToListAsync()).Select(r => (r.Props.Salary, r.Props.Age, r.Props.LastName)).ToList();
                on.Should().Equal(off, $"the leading form changes the plan, not the order (Skip {skip})");
            }

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            (await ById().ToSqlStringAsync()).Should().Contain("-- PVT leading form:");
            var onIds = (await ById().ToListAsync()).Select(r => r.Id).ToList();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var offIds = (await ById().ToListAsync()).Select(r => r.Id).ToList();
            onIds.Should().Equal(offIds, "ThenBy Id orders the run of equal ages by id in both forms");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// With ThenBy the walk of the leading form reads on to the end of the run of equal sort values it
    /// stops in. Take 10 walks 1 000 entries: 5 objects at the top value, then a run of 1 200 equal values
    /// whose smallest titles sit past the first thousand entries. A run longer than twice the walk (2 100
    /// objects) leaves the page to the streams. Both pages are compared with the prefilter off.
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
            (await Run().ToSqlStringAsync()).Should().Contain("-- PVT leading form:");

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
}
