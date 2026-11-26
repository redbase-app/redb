using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProRowMapperKindsTests : RowMapperKindsTestsBase
{
    public PostgresProRowMapperKindsTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    protected override string KindsSql => @"SELECT 1::bigint AS _id, 7::bigint AS count, NULL::bigint AS maybe_long,
        12.50::numeric AS amount, 1.5::float8 AS from_double, 0.25::float8 AS ratio, true AS flag,
        '6f1d0c3e-2b4a-4c8e-9a51-3d2f7e8b9c01'::uuid AS key_value, '2026-01-02 03:04:05+00'::timestamptz AS when_at,
        NULL::timestamp AS day, '\x0102'::bytea AS bytes, 'abc'::text AS ""NAME"", 'shown'::text AS display_name,
        '2026-03-04 00:00:00+00'::timestamptz AS first_seen, 'x'::text AS zzz";

    protected override string EnumSql => "SELECT 2 AS kind";
}
