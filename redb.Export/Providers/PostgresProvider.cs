using System.Data.Common;
using Npgsql;

namespace redb.Export.Providers;

/// <summary>
/// <see cref="IDataProvider"/> implementation for PostgreSQL.
/// <para>
/// Uses <c>COPY FROM STDIN</c> (TEXT format) for bulk inserts and
/// <c>session_replication_role = 'replica'</c> to bypass foreign-key triggers
/// during import.
/// </para>
/// </summary>
public sealed class PostgresProvider : IDataProvider
{
    private NpgsqlConnection? _connection;

    /// <inheritdoc />
    public string Name => "postgres";

    /// <inheritdoc />
    public DbConnection Connection => _connection
        ?? throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

    /// <inheritdoc />
    public async Task OpenAsync(string connectionString, CancellationToken ct = default)
    {
        _connection = new NpgsqlConnection(connectionString);
        await _connection.OpenAsync(ct);
    }

    /// <inheritdoc />
    public async Task CleanDatabaseAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        // Order matters: dependents before parents.
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
            "_types"
        };

        foreach (var table in tables)
        {
            await using var cmd = new NpgsqlCommand($"TRUNCATE TABLE {table} CASCADE", _connection);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /// <inheritdoc />
    public async Task<long> GetSequenceValueAsync(CancellationToken ct = default)
    {
        if (_connection is null) return 0;

        await using var cmd = new NpgsqlCommand("SELECT last_value FROM global_identity", _connection);
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task SetSequenceValueAsync(long value, CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new NpgsqlCommand($"SELECT setval('global_identity', {value})", _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task DisableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new NpgsqlCommand(
            "SET session_replication_role = 'replica'", _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task EnableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new NpgsqlCommand(
            "SET session_replication_role = 'origin'", _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default)
    {
        if (_connection is null || data.Rows.Count == 0) return;

        var columns = string.Join(", ", data.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName));
        var copyCommand = $"COPY {tableName} ({columns}) FROM STDIN (FORMAT TEXT, NULL '\\N')";

        await using var writer = await _connection.BeginTextImportAsync(copyCommand, ct);

        foreach (System.Data.DataRow row in data.Rows)
        {
            var values = new List<string>();
            for (int i = 0; i < data.Columns.Count; i++)
            {
                values.Add(FormatValueForCopy(row[i]));
            }
            await writer.WriteLineAsync(string.Join("\t", values));
        }
    }

    /// <summary>
    /// Formats a CLR value for the PostgreSQL TEXT <c>COPY</c> protocol.
    /// </summary>
    private static string FormatValueForCopy(object? value)
    {
        if (value is null or DBNull)
            return "\\N";

        return value switch
        {
            bool b => b ? "t" : "f",
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff"),
            DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz"),
            Guid g => g.ToString(),
            byte[] bytes => "\\\\x" + Convert.ToHexString(bytes),
            string s => EscapeCopyString(s),
            decimal d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "\\N"
        };
    }

    /// <summary>
    /// Escapes special characters for the PostgreSQL TEXT <c>COPY</c> format.
    /// </summary>
    private static string EscapeCopyString(string s)
    {
        return s
            .Replace("\\", "\\\\")
            .Replace("\t", "\\t")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r");
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
