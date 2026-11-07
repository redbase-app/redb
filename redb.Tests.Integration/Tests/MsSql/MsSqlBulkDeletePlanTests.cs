using redb.Core;
using redb.Core.Attributes;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// Plan-shape pin for <c>BulkDeleteValuesByListItemIdsAsync</c> (E114 finding, 2026-09-11):
/// <c>_values._ListItem</c> is covered by a FILTERED index (<c>IX__values__ListItem_not_null</c>),
/// and SQL Server does not match a filtered index through a STRING_SPLIT semi-join - that form
/// degrades to a full clustered scan of <c>_values</c> (33s cold on a seeded 8.4M-row table,
/// surfaced by E114 recreating its list). A parameterized IN list seeks the filtered index.
///
/// The pin executes the REAL provider method and asserts the cached plan's ACCESS operator
/// (a DELETE plan name-drops every index it maintains, so index-name presence proves
/// nothing). MSSQL-only: the unfiltered-index bulk deletes of this provider seek fine
/// through STRING_SPLIT, and the other providers' partial indexes match their IN/ANY forms.
/// </summary>
[Collection("MsSql")]
public class MsSqlBulkDeletePlanTests
{
    protected readonly IRedbService Redb;

    public MsSqlBulkDeletePlanTests(MsSqlFixture fixture) => Redb = fixture.Redb;

    [Fact]
    public async Task BulkDeleteValuesByListItemIds_PlanSeeksTheFilteredIndex()
    {
        await Redb.SyncSchemeAsync<PlanPinPaddingProps>();

        // The filtered-index matching failure is size-independent (verified by SHOWPLAN on
        // both a seeded 8.4M-row table and a near-empty one), but a few hundred rows keep the
        // cost model from ever rating a scan competitive, so the pin cannot flake on an
        // unusually empty fixture database.
        var padding = Enumerable.Range(0, 200).Select(i => new RedbObject<PlanPinPaddingProps>
        {
            name = $"plan-pin-{i}",
            Props = new PlanPinPaddingProps { A = i, B = $"pad-{i}" }
        }).ToList();
        await Redb.SaveAsync(padding);

        // The ids need not exist: DELETE ... WHERE _ListItem IN (...) compiles and caches its
        // plan no matter how many rows match.
        await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync([-101L, -102L, -103L]);

        var plan = await Redb.Context.ExecuteScalarAsync<string>(
            """
            SELECT TOP 1 CAST(qp.query_plan AS NVARCHAR(MAX))
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
            WHERE st.text LIKE N'%DELETE FROM [_]values WHERE [_]ListItem IN%'
              AND st.text NOT LIKE N'%dm[_]exec%'
            ORDER BY qs.last_execution_time DESC
            """);

        plan.Should().NotBeNullOrEmpty(
            "the bulk delete just executed, so its plan must be in the plan cache");
        // The index NAME proves nothing: a DELETE plan lists every index it maintains, the
        // filtered one included. The tell is the ACCESS operator - the STRING_SPLIT semi-join
        // cannot match the filtered index and reads _values through a Clustered Index Scan
        // (33s cold on a seeded 8.4M-row table); the parameterized IN form seeks.
        plan.Should().NotContain("Clustered Index Scan",
            "the delete-by-list-item form must locate rows through the filtered FK index, " +
            "not a full clustered scan of _values");
    }
}

[RedbScheme(Name = "MsSqlPlanPinPadding")]
public class PlanPinPaddingProps
{
    public long? A { get; set; }
    public string? B { get; set; }
}
