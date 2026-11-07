namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// SQLite has no sleep: a recursive count that keeps one connection busy for a couple of seconds, long enough for a
/// second command on the same connection to arrive while it runs.
/// </summary>
internal static class SqliteSlowScalar
{
    public const string Sql =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 30000000) SELECT count(*) FROM c";
}
