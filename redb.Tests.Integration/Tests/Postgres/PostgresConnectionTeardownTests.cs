using System.Globalization;
using redb.Core.Data;
using redb.Postgres.Data;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresConnectionTeardownTests : ConnectionTeardownTestsBase
{
    protected override IRedbConnection CreateConnection()
        => new NpgsqlRedbConnection(ConnString("Postgres"));

    protected override string SleepSql(double seconds)
        => $"SELECT COUNT(*) FROM pg_sleep({seconds.ToString(CultureInfo.InvariantCulture)})";
}
