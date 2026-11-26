using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProRowMapperKindsTests : RowMapperKindsTestsBase
{
    public MsSqlProRowMapperKindsTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    protected override string KindsSql => @"SELECT CAST(1 AS bigint) AS _id, CAST(7 AS bigint) AS count, CAST(NULL AS bigint) AS maybe_long,
        CAST(12.50 AS decimal(18,2)) AS amount, CAST(1.5 AS float) AS from_double, CAST(0.25 AS float) AS ratio, CAST(1 AS bit) AS flag,
        CAST('6f1d0c3e-2b4a-4c8e-9a51-3d2f7e8b9c01' AS uniqueidentifier) AS key_value,
        CAST('2026-01-02T03:04:05+00:00' AS datetimeoffset) AS when_at, CAST(NULL AS datetime2) AS day, 0x0102 AS bytes,
        'abc' AS NAME, 'shown' AS display_name, CAST('2026-03-04T00:00:00' AS datetime2) AS first_seen, 'x' AS zzz";

    protected override string EnumSql => "SELECT 2 AS kind";
}
