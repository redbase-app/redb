using System;
using Microsoft.Data.Sqlite;
using redb.Core.Data;

namespace redb.SQLite.Data
{
    /// <summary>
    /// SQLite implementation of REDB context.
    /// Assembles all Sqlite components into single facade.
    /// </summary>
    public class SqliteRedbContext : RedbContextBase
    {
        private readonly SqliteRedbConnection _connection;
        private readonly SqliteKeyGenerator _keyGenerator;
        private readonly SqliteBulkOperations _bulkOperations;
        
        /// <summary>
        /// Database connection.
        /// </summary>
        public override IRedbConnection Db => _connection;
        
        /// <summary>
        /// Key generator with caching.
        /// </summary>
        public override IKeyGenerator Keys => _keyGenerator;
        
        /// <summary>
        /// Bulk operations (COPY protocol).
        /// </summary>
        public override IBulkOperations Bulk => _bulkOperations;
        
        /// <summary>
        /// Sqlite data source (for direct access if needed).
        /// </summary>
        public SqliteDataSource DataSource { get; }

        /// <summary>
        /// Create SQLite context from data source.
        /// </summary>
        /// <param name="dataSource">Sqlite data source (pooled connections).</param>
        public SqliteRedbContext(SqliteDataSource dataSource)
        {
            DataSource = dataSource;
            _connection = new SqliteRedbConnection(dataSource);
            var domain = redb.Core.Models.Configuration.RedbServiceConfiguration.ComputeCacheDomain(dataSource.ConnectionString);
            _keyGenerator = new SqliteKeyGenerator(dataSource, domain);
            _bulkOperations = new SqliteBulkOperations(_connection);
        }
        
        /// <summary>
        /// Create SQLite context from connection string.
        /// </summary>
        /// <param name="connectionString">SQLite connection string.</param>
        public SqliteRedbContext(string connectionString)
            : this(SqliteDataSource.Create(connectionString))
        {
        }

        /// <summary>
        /// Dispose context and all components.
        /// </summary>
        public override async ValueTask DisposeAsync()
        {
            await _connection.DisposeAsync();
            // DataSource is shared, don't dispose it here
        }
    }
}

