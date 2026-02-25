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
    
    private const string SEQUENCE_NAME = "global_identity";
    
    /// <summary>
    /// Create MSSQL key generator from connection string.
    /// </summary>
    /// <param name="connectionString">MS SQL Server connection string.</param>
    public SqlKeyGenerator(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
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
        
        // Generate keys from range (in memory - instant)
        var keys = new List<long>(count);
        for (int i = 0; i < count; i++)
        {
            keys.Add(firstValue + i);
        }
        
        return keys;
    }
}

