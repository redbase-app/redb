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

    /// <summary>
    /// The redb tables an import writes, dependents before parents. <c>_scheme_metadata_cache</c> is a derived cache
    /// (no foreign keys); clearing it is safe, the runtime rebuilds it on the next scheme sync.
    /// </summary>
    private static readonly string[] RedbTables =
    [
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
    ];

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

        // SQLite has no TRUNCATE, so we DELETE. We deliberately do NOT touch sqlite_sequence: the
        // _global_identity high-water mark must survive.
        // Foreign keys are off for the whole import transaction (BeginImportAsync), so the order of the deletes
        // does not matter. A table the schema does not have is skipped by the catalog, not by swallowing an
        // error: the old catch also hid a locked database, and the import went on over a half-cleaned one.
        // The target keeps its own _types rows: the built-in types of this build; ImportService adds only the file's types
        // the target lacks.
        foreach (var table in RedbTables.Where(t => t != "_types"))
        {
            if (!await TableExistsAsync(table, ct))
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
    // Inside an import the keys come back on only after the commit, and switching them on checks nothing that is
    // already there: the loaded rows are checked here, still inside the transaction, so a refusal rolls back.
    public Task EnableConstraintsAsync(CancellationToken ct = default)
        => _transaction is not null
            ? CheckForeignKeysAsync("Nothing was imported; the database is unchanged.", ct)
            : ExecuteAsync("PRAGMA foreign_keys = ON", ct);

    /// <summary>
    /// Checks the rows of the redb tables against their foreign keys with <c>PRAGMA foreign_key_check</c>, which
    /// works whatever <c>PRAGMA foreign_keys</c> says, and refuses the import when a row points at a row that is not
    /// there. SQLite keeps no name for a key, so a key is named by its table and column.
    /// </summary>
    private async Task CheckForeignKeysAsync(string outcome, CancellationToken ct)
    {
        if (_connection is null) return;

        var broken = new List<string>();
        foreach (var table in RedbTables)
        {
            if (!await TableExistsAsync(table, ct)) continue;

            // One row per violating row: rowid and the id of the key (its position in foreign_key_list).
            var violations = new Dictionary<long, (long Rows, long ExampleRowId)>();
            await using (var check = _connection.CreateCommand())
            {
                check.Transaction = _transaction;
                check.CommandText = $"PRAGMA foreign_key_check({table})";
                await using var reader = await check.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var keyId = reader.GetInt64(3);
                    var rowId = reader.GetInt64(1);
                    violations[keyId] = violations.TryGetValue(keyId, out var seen)
                        ? (seen.Rows + 1, seen.ExampleRowId)
                        : (1, rowId);
                }
            }
            if (violations.Count == 0) continue;

            var keys = new List<(long Id, string Parent, string From, string? To)>();
            await using (var list = _connection.CreateCommand())
            {
                list.Transaction = _transaction;
                list.CommandText = $"SELECT id, \"table\", \"from\", \"to\" FROM pragma_foreign_key_list('{table}') ORDER BY id, seq";
                await using var reader = await list.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    keys.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
            }

            foreach (var (keyId, (rows, exampleRowId)) in violations.OrderBy(v => v.Key))
            {
                var columns = keys.Where(k => k.Id == keyId).ToList();
                var from = columns.Select(k => k.From).ToList();
                var example = await DescribeRowAsync(table, exampleRowId, from, ct);
                broken.Add(ForeignKeyRefusal.Describe(
                    $"{table}.{string.Join(",", from)}", table, rows, columns[0].Parent,
                    columns.Select(k => k.To ?? "primary key"), example));
            }
        }

        if (broken.Count > 0)
            throw ForeignKeyRefusal.Exception(broken, outcome);
    }

    /// <summary>The id and the key columns of one row, as <c>column = value</c> pairs.</summary>
    private async Task<string> DescribeRowAsync(string table, long rowId, IReadOnlyList<string> columns, CancellationToken ct)
    {
        await using var read = _connection!.CreateCommand();
        read.Transaction = _transaction;
        read.CommandText = $"SELECT _id, {string.Join(", ", columns.Select(c => $"\"{c}\""))} FROM {table} WHERE rowid = @rowid";
        read.Parameters.AddWithValue("@rowid", rowId);
        await using var reader = await read.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return $"rowid = {rowId}";
        return string.Join(", ", Enumerable.Range(0, columns.Count + 1)
            .Select(i => $"{(i == 0 ? "_id" : columns[i - 1])} = {reader.GetValue(i)}"));
    }

    private async Task<bool> TableExistsAsync(string table, CancellationToken ct)
    {
        await using var exists = _connection!.CreateCommand();
        exists.Transaction = _transaction;
        exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @name";
        exists.Parameters.AddWithValue("@name", table);
        return Convert.ToInt64(await exists.ExecuteScalarAsync(ct)) > 0;
    }

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
                parameters[i].Value = row[i] switch
                {
                    null or DBNull => DBNull.Value,
                    // The driver would bind a date as TEXT. redb's SQLite schema keeps dates as a
                    // UTC Julian day (REAL) and the provider compares them as numbers, so a TEXT
                    // date matches no date filter and sorts as a string.
                    DateTimeOffset moment => SqliteJulianDay.ToJulian(moment),
                    DateTime moment => SqliteJulianDay.ToJulian(new DateTimeOffset(DateTime.SpecifyKind(moment, DateTimeKind.Utc))),
                    // Same reason, same cure as the redb SQLite provider: the driver binds a decimal
                    // as TEXT, _Numeric and _value_numeric are REAL, and a REAL never compares equal
                    // to, and always sorts before, a TEXT.
                    decimal number => (double)number,
                    var value => value
                };
            }
            await cmd.ExecuteNonQueryAsync(ct);
        }

        if (ownTransaction is not null)
        {
            await ownTransaction.CommitAsync(ct);
            await ownTransaction.DisposeAsync();
        }
    }

    /// <inheritdoc />
    public async Task ExecuteAsync(string sql, CancellationToken ct = default)
    {
        if (_connection is null) throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

        await using var cmd = _connection.CreateCommand();
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
    /// Decoded here, not by the driver: the driver's own Julian conversion returns a date of
    /// unspecified kind and keeps sub-millisecond noise of the REAL, while the redb SQLite provider
    /// reads the number as UTC rounded to the millisecond. A database filled by an earlier import
    /// holds these columns as TEXT; that form is read too.
    /// </remarks>
    public DateTimeOffset ReadDateTimeOffset(DbDataReader reader, int ordinal) => reader.GetValue(ordinal) switch
    {
        double julian => SqliteJulianDay.FromJulian(julian),
        long julian => SqliteJulianDay.FromJulian(julian),
        string text => DateTimeOffset.Parse(text, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal),
        var other => throw new InvalidOperationException(
            $"Unexpected date column representation: {other.GetType().Name}")
    };

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
