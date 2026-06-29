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

        // FKs off so order-insensitive deletes never trip a constraint.
        await ExecuteAsync("PRAGMA foreign_keys = OFF", ct);

        foreach (var table in tables)
        {
            try
            {
                await ExecuteAsync($"DELETE FROM {table}", ct);
            }
            catch (SqliteException)
            {
                // Table might not exist (e.g. cache table on an older schema); skip.
            }
        }
    }

    /// <inheritdoc />
    public async Task<long> GetSequenceValueAsync(CancellationToken ct = default)
    {
        if (_connection is null) return 0;

        await using var cmd = _connection.CreateCommand();
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
            update.CommandText = "UPDATE sqlite_sequence SET seq = @value WHERE name = @name";
            update.Parameters.AddWithValue("@value", value);
            update.Parameters.AddWithValue("@name", SequenceName);
            var affected = await update.ExecuteNonQueryAsync(ct);
            if (affected > 0) return;
        }

        // No sqlite_sequence row yet (DB created without materializing it): create it.
        await using var insert = _connection.CreateCommand();
        insert.CommandText = "INSERT INTO sqlite_sequence (name, seq) VALUES (@name, @value)";
        insert.Parameters.AddWithValue("@name", SequenceName);
        insert.Parameters.AddWithValue("@value", value);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public Task DisableConstraintsAsync(CancellationToken ct = default)
        // PRAGMA foreign_keys is a no-op inside a transaction; callers invoke this
        // outside any ambient transaction (ImportService does).
        => ExecuteAsync("PRAGMA foreign_keys = OFF", ct);

    /// <inheritdoc />
    public Task EnableConstraintsAsync(CancellationToken ct = default)
        => ExecuteAsync("PRAGMA foreign_keys = ON", ct);

    /// <inheritdoc />
    public async Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default)
    {
        if (_connection is null || data.Rows.Count == 0) return;

        var columns = data.Columns.Cast<System.Data.DataColumn>().ToArray();
        var columnList = string.Join(", ", columns.Select(c => c.ColumnName));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));

        await using var tx = (SqliteTransaction)await _connection.BeginTransactionAsync(ct);

        await using var cmd = _connection.CreateCommand();
        cmd.Transaction = tx;
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

        await tx.CommitAsync(ct);
    }

    /// <summary>
    /// Executes a non-query statement on the open connection.
    /// </summary>
    private async Task ExecuteAsync(string sql, CancellationToken ct)
    {
        await using var cmd = _connection!.CreateCommand();
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
}
