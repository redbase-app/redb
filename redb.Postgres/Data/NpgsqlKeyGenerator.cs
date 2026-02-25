using Npgsql;
using redb.Core.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.Postgres.Data
{
    /// <summary>
    /// PostgreSQL implementation of key generator.
    /// Uses global_identity sequence. Caching is static in base class.
    /// </summary>
    public class NpgsqlKeyGenerator : RedbKeyGeneratorBase
    {
        private readonly NpgsqlDataSource _dataSource;
        
        private const string SEQUENCE_NAME = "global_identity";
        
        /// <summary>
        /// Create PostgreSQL key generator.
        /// </summary>
        public NpgsqlKeyGenerator(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }
        
        /// <summary>
        /// Create PostgreSQL key generator from connection string.
        /// </summary>
        public NpgsqlKeyGenerator(string connectionString)
        {
            _dataSource = NpgsqlDataSource.Create(connectionString);
        }

        // === DB-SPECIFIC IMPLEMENTATIONS ===
        
        /// <summary>
        /// Generate batch of keys from PostgreSQL sequence.
        /// </summary>
        protected override async Task<List<long>> GenerateKeysAsync(int count)
        {
            var keys = new List<long>(count);
            
            await using var conn = await _dataSource.OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand(
                $"SELECT nextval('{SEQUENCE_NAME}') FROM generate_series(1, {count})", conn);
            
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                keys.Add(reader.GetInt64(0));
            }
            
            return keys;
        }
    }
}
