using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace redb.Export.Providers;

/// <summary>
/// <see cref="IDataProvider"/> implementation for SQLite (via <c>Microsoft.Data.Sqlite</c>).
/// <para>
/// SQLite has no <c>TRUNCATE</c>, no server-side sequences, and no bulk-copy protocol,
/// so this provider uses <c>DELETE FROM</c> for cleaning, the AUTOINCREMENT high-water
/// mark in <c>sqlite_sequence</c> (row <c>_global_identity</c>) for the identity counter,
/// and batched parameterized <c>INSERT</c>s inside a single transaction for bulk import.
/// Foreign keys are toggled per-connection via <c>PRAGMA foreign_keys</c> (they default to
/// OFF in Microsoft.Data.Sqlite and must be enabled per connection, never inside a transaction).
/// </para>
/// </summary>
public sealed class SqliteProvider : IDataProvider
{
    /// <summary>
    /// Name of the AUTOINCREMENT table whose <c>sqlite_sequence</c> row holds the
    /// global identity high-water mark. Must match the schema in <c>redbSqlite.sql</c>.
    /// </summary>
    private const string SequenceName = "_global_identity";

    private SqliteConnection? _connection;
    private SqliteTransaction? _transaction;

    /// <inheritdoc />
    public async Task BeginImportAsync(CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");
        if (_transaction is not null) throw new InvalidOperationException("An import transaction is already open.");
        // Foreign keys can only be switched outside a transaction, so they go off before it begins and come back
        // after it ends, whichever way it ends.
        await ExecuteAsync("PRAGMA foreign_keys = OFF", ct);
        _transaction = (SqliteTransaction)await _connection.BeginTransactionAsync(ct);
    }

    /// <inheritdoc />
    public async Task CommitImportAsync(CancellationToken ct = default)
    {
        if (_transaction is null) throw new InvalidOperationException("No import transaction is open.");
        await _transaction.CommitAsync(ct);
        await _transaction.DisposeAsync();
        _transaction = null;
        await ExecuteAsync("PRAGMA foreign_keys = ON", ct);
    }

    /// <inheritdoc />
    public async Task AbortImportAsync()
    {
        if (_transaction is null) return;
        await _transaction.RollbackAsync();
        await _transaction.DisposeAsync();
        _transaction = null;
        await ExecuteAsync("PRAGMA foreign_keys = ON", CancellationToken.None);
    }

    /// <inheritdoc />
    public string Name => "sqlite";

    /// <inheritdoc />
    public DbConnection Connection => _connection
        ?? throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

    /// <inheritdoc />
    public async Task OpenAsync(string connectionString, CancellationToken ct = default)
    {
        _connection = new SqliteConnection(connectionString);
        await _connection.OpenAsync(ct);
    }

    /// <inheritdoc />
    public async Task CleanDatabaseAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        // Dependents before parents. SQLite has no TRUNCATE, so we DELETE.
        // _scheme_metadata_cache is a derived cache (no FKs); clearing it is safe and
        // it is rebuilt by the runtime on the next scheme sync. We deliberately do NOT
        // touch sqlite_sequence — the _global_identity high-water mark must survive.
        var tables = new[]
        {
            "_values",
            "_list_items",
            "_objects",
            "_permissions",
            "_functions",
            "_dependencies",
            "_structures",
            "_schemes",
            "_users_roles",
            "_users",
            "_roles",
            "_lists",
            "_links",
            "_types",
            "_scheme_metadata_cache"
        };

        // Foreign keys are off for the whole import transaction (BeginImportAsync), so the order of the deletes
        // does not matter. A table the schema does not have is skipped by the catalog, not by swallowing an
        // error: the old catch also hid a locked database, and the import went on over a half-cleaned one.
        foreach (var table in tables)
        {
            await using var exists = _connection.CreateCommand();
            exists.Transaction = _transaction;
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @name";
            exists.Parameters.AddWithValue("@name", table);
            if (Convert.ToInt64(await exists.ExecuteScalarAsync(ct)) == 0)
                continue;

            await ExecuteAsync($"DELETE FROM {table}", ct);
        }
    }

    /// <inheritdoc />
    public async Task<long> GetSequenceValueAsync(CancellationToken ct = default)
    {
        if (_connection is null) return 0;

        await using var cmd = _connection.CreateCommand();
        cmd.Transaction = _transaction;
        cmd.CommandText = "SELECT seq FROM sqlite_sequence WHERE name = @name";
        cmd.Parameters.AddWithValue("@name", SequenceName);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is null or DBNull ? 0 : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task SetSequenceValueAsync(long value, CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using (var update = _connection.CreateCommand())
        {
            update.Transaction = _transaction;
            update.CommandText = "UPDATE sqlite_sequence SET seq = @value WHERE name = @name";
            update.Parameters.AddWithValue("@value", value);
            update.Parameters.AddWithValue("@name", SequenceName);
            var affected = await update.ExecuteNonQueryAsync(ct);
            if (affected > 0) return;
        }

        // No sqlite_sequence row yet (DB created without materializing it): create it.
        await using var insert = _connection.CreateCommand();
        insert.Transaction = _transaction;
        insert.CommandText = "INSERT INTO sqlite_sequence (name, seq) VALUES (@name, @value)";
        insert.Parameters.AddWithValue("@name", SequenceName);
        insert.Parameters.AddWithValue("@value", value);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public Task DisableConstraintsAsync(CancellationToken ct = default)
        // PRAGMA foreign_keys is a no-op inside a transaction. Within an import it was switched off before the
        // transaction began and comes back on after it ends (BeginImportAsync / CommitImportAsync / AbortImportAsync).
        => _transaction is not null ? Task.CompletedTask : ExecuteAsync("PRAGMA foreign_keys = OFF", ct);

    /// <inheritdoc />
    public Task EnableConstraintsAsync(CancellationToken ct = default)
        => _transaction is not null ? Task.CompletedTask : ExecuteAsync("PRAGMA foreign_keys = ON", ct);

    /// <inheritdoc />
    public async Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default)
    {
        if (_connection is null || data.Rows.Count == 0) return;

        var columns = data.Columns.Cast<System.Data.DataColumn>().ToArray();
        var columnList = string.Join(", ", columns.Select(c => c.ColumnName));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));

        // Inside the import transaction every batch joins it; outside one (a caller of its own) a batch is atomic alone.
        var ownTransaction = _transaction is null
            ? (SqliteTransaction)await _connection.BeginTransactionAsync(ct)
            : null;

        await using var cmd = _connection.CreateCommand();
        cmd.Transaction = _transaction ?? ownTransaction;
        cmd.CommandText = $"INSERT INTO {tableName} ({columnList}) VALUES ({paramList})";

        // Create the parameter set once and reuse it across every row (prepared once).
        var parameters = new SqliteParameter[columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            parameters[i] = cmd.CreateParameter();
            parameters[i].ParameterName = $"@p{i}";
            cmd.Parameters.Add(parameters[i]);
        }
        cmd.Prepare();

        foreach (System.Data.DataRow row in data.Rows)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                var value = row[i];
                parameters[i].Value = value is null or DBNull ? DBNull.Value : value;
            }
            await cmd.ExecuteNonQueryAsync(ct);
        }

        if (ownTransaction is not null)
        {
            await ownTransaction.CommitAsync(ct);
            await ownTransaction.DisposeAsync();
        }
    }

    /// <summary>
    /// Executes a non-query statement on the open connection.
    /// </summary>
    private async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        await using var cmd = _connection!.CreateCommand();
        cmd.Transaction = _transaction;
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// redb's SQLite schema stores uuid-semantic columns as a 16-byte BLOB in RFC 4122 (text)
    /// order (see SqliteHash in redb.SQLite); the driver's <c>GetGuid</c> would byte-swap the
    /// first three groups. Legacy pre-V4 databases may still hold the TEXT form.
    /// </remarks>
    public Guid GuidFromDb(object raw) => raw switch
    {
        byte[] blob => Guid.ParseExact(Convert.ToHexString(blob), "N"),
        string text => Guid.Parse(text),
        Guid guid => guid,
        _ => throw new InvalidOperationException(
            $"Unexpected uuid column representation: {raw.GetType().Name}")
    };

    /// <inheritdoc />
    public object GuidToDb(Guid value) => Convert.FromHexString(value.ToString("N"));
}
