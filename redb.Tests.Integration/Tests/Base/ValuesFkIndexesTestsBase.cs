using redb.Core;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Schema-delivery pin for the FK-column indexes of <c>_values</c> (perf finding, 2026-09-10):
/// <c>_values._Object</c> and <c>_values._ListItem</c> carry foreign keys, and without a leading
/// index every DELETE of a referenced object or list item scans the whole table to check for
/// referencing rows (measured on ~8.4M rows: SQLite 1.5-5.5s, PostgreSQL 5.9s, MSSQL 6.5-7s per
/// check - milliseconds with the indexes, which stay nearly empty thanks to the partial form).
///
/// The pin asserts DELIVERY, not just DDL: both a fresh database and an existing one upgraded
/// through the provider's versioned mechanism (PG/MSSQL: pvt module version + block 0;
/// SQLite: PRAGMA user_version + ApplySchemaUpgradesAsync) must end up with both indexes.
/// </summary>
public abstract class ValuesFkIndexesTestsBase
{
    protected readonly IRedbService Redb;

    protected ValuesFkIndexesTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Provider-specific catalog probe: does an index with this exact name exist?</summary>
    protected abstract Task<bool> IndexExistsAsync(string indexName);

    [Theory]
    [InlineData("IX__values__ListItem_not_null")]
    [InlineData("IX__values__Object_not_null")]
    public async Task FkIndex_IsDelivered_ByInitialize(string indexName)
    {
        // Idempotent on a current database; on a database behind the schema version it applies
        // the upgrade pass - exactly the path an existing installation takes on package update.
        await Redb.InitializeAsync();

        (await IndexExistsAsync(indexName)).Should().BeTrue(
            $"{indexName} must exist after InitializeAsync - without it every DELETE of a " +
            "referenced row scans the whole _values table for the FK check");
    }
}
