using System.Data.Common;

namespace redb.Export.Providers;

/// <summary>
/// Abstracts database-specific operations required by the export/import pipeline.
/// <para>
/// Each implementation handles connection management, bulk-insert strategy,
/// constraint toggling, and sequence manipulation for its target RDBMS.
/// </para>
/// </summary>
public interface IDataProvider : IAsyncDisposable
{
    /// <summary>
    /// Short provider identifier (e.g. <c>"postgres"</c>, <c>"mssql"</c>).
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Opens a connection to the database using the supplied connection string.
    /// </summary>
    /// <param name="connectionString">ADO.NET connection string.</param>
    /// <param name="ct">Cancellation token.</param>
    Task OpenAsync(string connectionString, CancellationToken ct = default);

    /// <summary>
    /// Returns the underlying <see cref="DbConnection"/> instance.
    /// Must be called after <see cref="OpenAsync"/>.
    /// </summary>
    DbConnection Connection { get; }

    /// <summary>
    /// Truncates all REDB tables in the correct foreign-key order, leaving the schema intact.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task CleanDatabaseAsync(CancellationToken ct = default);

    /// <summary>
    /// Returns the current value of the <c>global_identity</c> sequence.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task<long> GetSequenceValueAsync(CancellationToken ct = default);

    /// <summary>
    /// Resets the <c>global_identity</c> sequence to the specified value
    /// (typically the value stored in the export footer).
    /// </summary>
    /// <param name="value">New sequence value.</param>
    /// <param name="ct">Cancellation token.</param>
    Task SetSequenceValueAsync(long value, CancellationToken ct = default);
    // Contract of the pair: GetSequenceValueAsync returns the LAST id handed out, and SetSequenceValueAsync(v) makes
    // the NEXT id handed out v + 1. SQL Server's RESTART WITH names the next value itself, so it restarts at v + 1
    // (review 2026-09-24: it restarted at v, and the first object saved after an import took an id already in use).

    /// <summary>
    /// Starts the import transaction. Everything the import writes from here on - cleaning, rows, the
    /// sequence, the constraint mode - commits or rolls back as one: an import that fails half-way (a broken
    /// file, a cancelled run, a server error) leaves the database as it was, cleaning included.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task BeginImportAsync(CancellationToken ct = default);

    /// <summary>Commits the import transaction and restores the session state the import changed.</summary>
    /// <param name="ct">Cancellation token.</param>
    Task CommitImportAsync(CancellationToken ct = default);

    /// <summary>
    /// Rolls back whatever the import wrote and restores the session state it changed. Called on every path
    /// that did not commit; does nothing after a commit.
    /// </summary>
    Task AbortImportAsync();

    /// <summary>
    /// Disables foreign-key constraints and triggers so that rows can be
    /// bulk-inserted in arbitrary order.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task DisableConstraintsAsync(CancellationToken ct = default);

    /// <summary>
    /// Re-enables foreign-key constraints and triggers after bulk import.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task EnableConstraintsAsync(CancellationToken ct = default);

    /// <summary>
    /// Performs a bulk insert of the supplied <see cref="System.Data.DataTable"/>
    /// into the specified database table using the most efficient provider-specific mechanism
    /// (e.g. <c>COPY FROM STDIN</c> for PostgreSQL, <c>SqlBulkCopy</c> for SQL Server).
    /// </summary>
    /// <param name="tableName">Target table name (e.g. <c>"_objects"</c>).</param>
    /// <param name="data">Data to insert.</param>
    /// <param name="ct">Cancellation token.</param>
    Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default);

    /// <summary>
    /// Reads a uuid-semantic column value into its portable <see cref="Guid"/> form.
    /// PostgreSQL/MSSQL store these columns natively; SQLite stores them as a 16-byte BLOB in
    /// RFC 4122 (text) order - <c>_users._hash</c>, <c>_schemes._structure_hash</c>,
    /// <c>_objects._hash</c>, <c>_values._unique</c>. Reading such a BLOB through the driver's
    /// <c>GetGuid</c> would byte-swap the first three groups; this seam keeps the JSONL canonical.
    /// Genuine binary columns (<c>_value_bytes</c>, <c>_values._ByteArray</c>) are NOT uuids and
    /// never go through this conversion.
    /// </summary>
    Guid GuidFromDb(object raw);

    /// <summary>
    /// Converts a portable <see cref="Guid"/> into the value this provider stores in a
    /// uuid-semantic column (see <see cref="GuidFromDb"/>): the <see cref="Guid"/> itself for
    /// PostgreSQL/MSSQL, the RFC 4122-ordered 16-byte BLOB for SQLite.
    /// </summary>
    object GuidToDb(Guid value);
}
