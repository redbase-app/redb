using redb.Core.Data;

namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// Refuses a fixture wipe of a database that does not look like a test database. The fixtures
/// clean their database COMPLETELY on start (DELETE FROM _values / _objects) - lawful on the
/// dedicated test database, catastrophic on anything else: on 2026-09-10 a freshly seeded
/// 100k-object examples database shared the test connection string and was lost to exactly this
/// (and the 8M-row cascade-trigger delete масked itself as an hours-long hang first).
/// Examples/seeding live in their own databases (redb_examples) since then; this guard is the
/// belt to that suspenders.
/// </summary>
public static class FixtureWipeGuard
{
    /// <summary>Way above anything the test suites ever create, way below any real seed.</summary>
    public const long MaxObjectsForWipe = 50_000;

    public static async Task EnsureLooksLikeATestDatabaseAsync(IRedbContext ctx, string fixtureName)
    {
        var count = await ctx.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM _objects");
        if (count > MaxObjectsForWipe)
            throw new InvalidOperationException(
                $"{fixtureName}: refusing to wipe this database - _objects holds {count:N0} rows " +
                $"(threshold {MaxObjectsForWipe:N0}). It looks seeded or production-like, and the " +
                "fixture cleanup deletes EVERYTHING. Point the tests at a dedicated test database; " +
                "the examples seed belongs in redb_examples.");
    }
}
