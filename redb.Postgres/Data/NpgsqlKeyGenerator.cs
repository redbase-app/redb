using Npgsql;
using redb.Core.Data;
using System;
using System.Collections.Generic;
using System.Threading;
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
        // The scope's connection wrapper: inside a TransactionScope it works on the transaction's connection.
        private readonly Func<IRedbConnection?>? _scopeConnection;

        private const string SEQUENCE_NAME = "global_identity";

        /// <summary>
        /// Create PostgreSQL key generator.
        /// </summary>
        /// <param name="dataSource">Npgsql data source.</param>
        /// <param name="domain">Cache domain for key isolation (optional).</param>
        /// <param name="scopeConnection">The owning scope's connection, used for keys inside an ambient transaction.</param>
        public NpgsqlKeyGenerator(NpgsqlDataSource dataSource, string? domain = null, Func<IRedbConnection?>? scopeConnection = null) : base(domain)
        {
            _dataSource = dataSource;
            _scopeConnection = scopeConnection;
        }

        /// <summary>
        /// Create PostgreSQL key generator from connection string.
        /// </summary>
        public NpgsqlKeyGenerator(string connectionString, string? domain = null) : base(domain)
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

        /// <summary>
        /// Inside a TransactionScope a refill on a connection of its own would be a second connection in the
        /// transaction, which PostgreSQL can only commit as a prepared (two-phase) transaction. The values are
        /// taken through the scope's wrapper instead - on the transaction's connection. nextval is not
        /// transactional: a rollback leaves a gap, never a duplicate.
        /// </summary>
        protected override async Task<List<long>?> TryGenerateKeysInAmbientTransactionAsync(int count)
        {
            if (System.Transactions.Transaction.Current == null || _scopeConnection?.Invoke() is not { } db)
                return null;

            return await db.QueryScalarListAsync<long>(
                $"SELECT nextval('{SEQUENCE_NAME}') FROM generate_series(1, $1)", new object[] { count }, CancellationToken.None);
        }
    }
}
