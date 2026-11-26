using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProRowMapperKindsTests : RowMapperKindsTestsBase
{
    public SqliteProRowMapperKindsTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    // Datetimes are Julian days (REAL), guids text, booleans integers: the SQLite mapper reads them as such.
    protected override string KindsSql => @"SELECT 1 AS _id, 7 AS count, NULL AS maybe_long, 12.50 AS amount, 1.5 AS from_double,
        0.25 AS ratio, 1 AS flag, '6f1d0c3e-2b4a-4c8e-9a51-3d2f7e8b9c01' AS key_value, julianday('2026-01-02 03:04:05') AS when_at,
        NULL AS day, X'0102' AS bytes, 'abc' AS NAME, 'shown' AS display_name, julianday('2026-03-04 00:00:00') AS first_seen,
        'x' AS zzz";

    protected override string EnumSql => "SELECT 2 AS kind";

    /// <summary>SQLite refuses a value it cannot convert (SL-57): an integer is not an enum to Convert.ChangeType.</summary>
    protected override async Task AssertEnumAsync()
    {
        var read = async () => await Redb.Context.QueryAsync<MapperEnumRow>(EnumSql);
        await read.Should().ThrowAsync<InvalidOperationException>().WithMessage("*kind*");
    }
}
