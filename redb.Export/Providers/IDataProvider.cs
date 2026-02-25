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
}
