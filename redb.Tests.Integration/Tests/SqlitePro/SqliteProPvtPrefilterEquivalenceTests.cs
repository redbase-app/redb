using redb.Core.Query;
using redb.Tests.Integration.Fixtures;
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
            unordered.Should().Contain("DialectDeclined",
                "an unordered limit lets SQLite stream the aggregate, and a multi-branch prefilter " +
                "would take that away for a 40x loss");
            unordered.Should().NotContain("AND ((v._id_structure",
                "and the declined plan must not be rendered into the value scan");

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

        IRedbQueryable<EmployeeProps> Ordered() => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .OrderByRedb(o => o.Id)
            .Take(100);
    }
}
