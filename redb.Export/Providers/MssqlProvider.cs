using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace redb.Export.Providers;

/// <summary>
/// <see cref="IDataProvider"/> implementation for Microsoft SQL Server.
/// <para>
/// Uses <see cref="SqlBulkCopy"/> for high-throughput inserts and
/// <c>sp_MSforeachtable</c> to toggle constraints and triggers during import.
/// </para>
/// </summary>
public sealed class MssqlProvider : IDataProvider
{
    private SqlConnection? _connection;

    /// <inheritdoc />
    public string Name => "mssql";

    /// <inheritdoc />
    public DbConnection Connection => _connection
        ?? throw new InvalidOperationException("Connection not opened. Call OpenAsync first.");

    /// <inheritdoc />
    public async Task OpenAsync(string connectionString, CancellationToken ct = default)
    {
        _connection = new SqlConnection(connectionString);
        await _connection.OpenAsync(ct);
    }

    /// <inheritdoc />
    public async Task CleanDatabaseAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

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

        // Disable triggers and FK checks before truncation.
        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'", _connection))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? DISABLE TRIGGER ALL'", _connection))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in tables)
        {
            try
            {
                await using var cmd = new SqlCommand($"TRUNCATE TABLE {table}", _connection);
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch
            {
                try
                {
                    await using var cmd = new SqlCommand($"DELETE FROM {table}", _connection);
                    await cmd.ExecuteNonQueryAsync(ct);
                }
                catch
                {
                    // Table might not exist; skip.
                }
            }
        }

        // Re-enable triggers and FK checks.
        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? ENABLE TRIGGER ALL'", _connection))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await using (var cmd = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'", _connection))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    /// <inheritdoc />
    public async Task<long> GetSequenceValueAsync(CancellationToken ct = default)
    {
        if (_connection is null) return 0;

        const string sql = "SELECT CAST(current_value AS BIGINT) FROM sys.sequences WHERE name = 'global_identity'";
        await using var cmd = new SqlCommand(sql, _connection);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is null ? 0 : Convert.ToInt64(result);
    }

    /// <inheritdoc />
    public async Task SetSequenceValueAsync(long value, CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd = new SqlCommand($"ALTER SEQUENCE global_identity RESTART WITH {value}", _connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task DisableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd1 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'", _connection);
        await cmd1.ExecuteNonQueryAsync(ct);

        await using var cmd2 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? DISABLE TRIGGER ALL'", _connection);
        await cmd2.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task EnableConstraintsAsync(CancellationToken ct = default)
    {
        if (_connection is null) return;

        await using var cmd1 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? ENABLE TRIGGER ALL'", _connection);
        await cmd1.ExecuteNonQueryAsync(ct);

        await using var cmd2 = new SqlCommand(
            "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'", _connection);
        await cmd2.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task BulkInsertAsync(string tableName, System.Data.DataTable data, CancellationToken ct = default)
    {
        if (_connection is null || data.Rows.Count == 0) return;

        using var bulkCopy = new SqlBulkCopy(_connection)
        {
            DestinationTableName = tableName,
            BatchSize = 5000,
            BulkCopyTimeout = 600
        };

        foreach (System.Data.DataColumn col in data.Columns)
        {
            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        }

        await bulkCopy.WriteToServerAsync(data, ct);
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
