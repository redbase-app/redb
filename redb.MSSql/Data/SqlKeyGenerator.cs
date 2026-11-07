using Microsoft.Data.SqlClient;
using redb.Core.Data;

namespace redb.MSSql.Data;

/// <summary>
/// MS SQL Server implementation of key generator.
/// Uses global_identity sequence. Caching is static in base class.
/// </summary>
public class SqlKeyGenerator : RedbKeyGeneratorBase
{
    private readonly string _connectionString;
    // The scope's connection wrapper: inside a TransactionScope it works on the transaction's connection.
    private readonly Func<IRedbConnection?>? _scopeConnection;

    private const string SEQUENCE_NAME = "global_identity";

    /// <summary>
    /// Create MSSQL key generator from connection string.
    /// </summary>
    /// <param name="connectionString">MS SQL Server connection string.</param>
    /// <param name="domain">Cache domain for key isolation (optional).</param>
    /// <param name="scopeConnection">The owning scope's connection, used for keys inside an ambient transaction.</param>
    public SqlKeyGenerator(string connectionString, string? domain = null, Func<IRedbConnection?>? scopeConnection = null) : base(domain)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _scopeConnection = scopeConnection;
    }

    // === DB-SPECIFIC IMPLEMENTATIONS ===

    /// <summary>
    /// Generate batch of keys from MSSQL sequence.
    /// Uses sp_sequence_get_range for instant range reservation.
    /// </summary>
    protected override async Task<List<long>> GenerateKeysAsync(int count)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        // sp_sequence_get_range - instant range reservation (1 call for any count)
        var sql = $@"
            DECLARE @first SQL_VARIANT;
            EXEC sp_sequence_get_range @sequence_name = N'{SEQUENCE_NAME}',
                                       @range_size = @count,
                                       @range_first_value = @first OUTPUT;
            SELECT CAST(@first AS BIGINT);";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@count", count);

        var firstValue = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        return Range(firstValue, count);
    }

    /// <summary>
    /// Inside a TransactionScope a refill on a connection of its own would be a second connection in the
    /// transaction, and SQL Server refuses the implicit distributed transaction that makes. The range is
    /// reserved through the scope's wrapper instead - on the transaction's connection. A sequence range is
    /// not transactional: a rollback leaves a gap, never a duplicate.
    /// </summary>
    protected override async Task<List<long>?> TryGenerateKeysInAmbientTransactionAsync(int count)
    {
        if (System.Transactions.Transaction.Current == null || _scopeConnection?.Invoke() is not { } db)
            return null;

        var sql = $@"
            DECLARE @first SQL_VARIANT;
            EXEC sp_sequence_get_range @sequence_name = N'{SEQUENCE_NAME}',
                                       @range_size = $1,
                                       @range_first_value = @first OUTPUT;
            SELECT CAST(@first AS BIGINT);";
        var firstValue = await db.ExecuteScalarAsync<long>(sql, new object[] { count }, CancellationToken.None);
        return Range(firstValue, count);
    }

    private static List<long> Range(long firstValue, int count)
    {
        // Generate keys from range (in memory - instant)
        var keys = new List<long>(count);
        for (int i = 0; i < count; i++)
        {
            keys.Add(firstValue + i);
        }
        return keys;
    }
}
