using System;
using Npgsql;
using redb.Core.Data;

namespace redb.Postgres.Data
{
    /// <summary>
    /// PostgreSQL implementation of REDB context.
    /// Assembles all Npgsql components into single facade.
    /// </summary>
    public class NpgsqlRedbContext : RedbContextBase
    {
        private readonly NpgsqlRedbConnection _connection;
        private readonly NpgsqlKeyGenerator _keyGenerator;
        private readonly NpgsqlBulkOperations _bulkOperations;
        
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
        /// Npgsql data source (for direct access if needed).
        /// </summary>
        public NpgsqlDataSource DataSource { get; }

        /// <summary>
        /// Create PostgreSQL context from data source.
        /// </summary>
        /// <param name="dataSource">Npgsql data source (pooled connections).</param>
        public NpgsqlRedbContext(NpgsqlDataSource dataSource)
        {
            DataSource = dataSource;
            _connection = new NpgsqlRedbConnection(dataSource);
            _keyGenerator = new NpgsqlKeyGenerator(dataSource);
            _bulkOperations = new NpgsqlBulkOperations(_connection);
        }
        
        /// <summary>
        /// Create PostgreSQL context from connection string.
        /// </summary>
        /// <param name="connectionString">PostgreSQL connection string.</param>
        public NpgsqlRedbContext(string connectionString)
            : this(NpgsqlDataSource.Create(connectionString))
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

