using redb.Core.Data;

namespace redb.MSSql.Data;

/// <summary>
/// MS SQL Server implementation of REDB context.
/// Assembles all SQL Server components into single facade.
/// </summary>
public class SqlRedbContext : RedbContextBase
{
    private readonly SqlRedbConnection _connection;
    private readonly SqlKeyGenerator _keyGenerator;
    private readonly SqlBulkOperations _bulkOperations;
    
    /// <summary>
    /// Database connection.
    /// </summary>
    public override IRedbConnection Db => _connection;
    
    /// <summary>
    /// Key generator with caching.
    /// </summary>
    public override IKeyGenerator Keys => _keyGenerator;
    
    /// <summary>
    /// Bulk operations (SqlBulkCopy).
    /// </summary>
    public override IBulkOperations Bulk => _bulkOperations;
    
    /// <summary>
    /// Connection string (for direct access if needed).
    /// </summary>
    public string ConnectionString { get; }
    
    /// <summary>
    /// Create SQL Server context from connection string.
    /// </summary>
    /// <param name="connectionString">MS SQL Server connection string.</param>
    public SqlRedbContext(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
            throw new ArgumentNullException(nameof(connectionString));
        
        ConnectionString = connectionString;
        _connection = new SqlRedbConnection(connectionString);
        _keyGenerator = new SqlKeyGenerator(connectionString);
        _bulkOperations = new SqlBulkOperations(_connection);
    }

    /// <summary>
    /// Dispose context and all components.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
    }
}

