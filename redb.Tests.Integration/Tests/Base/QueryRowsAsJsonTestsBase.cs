using System.Text.Json;
using redb.Core;
using redb.Tests.Integration.Helpers;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// IRedbContext.QueryRowsAsJsonAsync on every provider: each row an object keyed by column name, numbers as numbers,
/// text as text, NULL kept as null, a parameter bound, "[]" for no rows, and one command (one telemetry span) per call.
/// </summary>
public abstract class QueryRowsAsJsonTestsBase
{
    protected readonly IRedbService Redb;

    protected QueryRowsAsJsonTestsBase(IRedbService redb) => Redb = redb;

    private const string TwoRows =
        "SELECT $1 AS label, 7 AS n, 2.5 AS d, CAST(NULL AS INT) AS empty " +
        "UNION ALL SELECT 'b', 8, 3.5, CAST(NULL AS INT)";

    [Fact]
    public async Task Rows_AreObjectsKeyedByColumn_WithNullsKept()
    {
        var json = await Redb.Context.QueryRowsAsJsonAsync(TwoRows, new object[] { "a" });

        using var doc = JsonDocument.Parse(json);
        var rows = doc.RootElement.EnumerateArray().OrderBy(r => r.GetProperty("label").GetString()).ToList();
        rows.Should().HaveCount(2);
        rows[0].GetProperty("label").GetString().Should().Be("a", "the parameter is bound");
        rows[0].GetProperty("n").GetInt32().Should().Be(7);
        rows[0].GetProperty("d").GetDecimal().Should().Be(2.5m);
        rows[0].GetProperty("empty").ValueKind.Should().Be(JsonValueKind.Null, "a NULL column is kept, as null");
        rows[1].GetProperty("label").GetString().Should().Be("b");
        rows[1].GetProperty("n").GetInt32().Should().Be(8);
        rows[1].GetProperty("d").GetDecimal().Should().Be(3.5m);
    }

    [Fact]
    public async Task NoRows_IsAnEmptyArray()
        => (await Redb.Context.QueryRowsAsJsonAsync("SELECT 1 AS n WHERE 1 = 0", Array.Empty<object>())).Should().Be("[]");

    [Fact]
    public async Task OneCall_IsOneCommand()
        => (await CommandCounter.CountAsync(() => Redb.Context.QueryRowsAsJsonAsync(TwoRows, new object[] { "a" })))
            .Should().Be(1);
}
