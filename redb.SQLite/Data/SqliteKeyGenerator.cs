using System;
using Microsoft.Data.Sqlite;
using redb.Core.Data;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.SQLite.Data
{
    /// <summary>
    /// SQLite implementation of key generator. Ids come from the native
    /// AUTOINCREMENT table <c>_global_identity</c>; its high-water mark lives in
    /// <c>sqlite_sequence</c>. A batch of N ids is reserved with one atomic statement:
    ///     UPDATE sqlite_sequence SET seq = seq + @n WHERE name='_global_identity'
    ///     RETURNING seq;
    /// which returns the new top; the reserved ids are [top-N+1 .. top].
    /// No explicit locking is needed — SQLite serializes writers and the
    /// read-modify-write happens inside the single UPDATE (RETURNING: 3.35+).
    /// The same sequence is also advanced by the C extension (nextId) and by
    /// soft-delete (INSERT DEFAULT VALUES); all consumers only ever increase it,
    /// so ids are globally unique. Caching is static in the base class.
    /// </summary>
    public class SqliteKeyGenerator : RedbKeyGeneratorBase
    {
        private readonly SqliteDataSource _dataSource;

        // AUTOINCREMENT table whose sqlite_sequence row is the id high-water mark.
        private const string SEQUENCE_TABLE = "_global_identity";

        /// <summary>
        /// Create SQLite key generator.
        /// </summary>
        public SqliteKeyGenerator(SqliteDataSource dataSource, string? domain = null) : base(domain)
        {
            _dataSource = dataSource;
        }
        
        /// <summary>
        /// Create SQLite key generator from connection string.
        /// </summary>
        public SqliteKeyGenerator(string connectionString, string? domain = null) : base(domain)
        {
            _dataSource = SqliteDataSource.Create(connectionString);
        }

        // === DB-SPECIFIC IMPLEMENTATIONS ===
        
        /// <summary>
        /// Reserve a contiguous batch of <paramref name="count"/> ids by bumping the
        /// AUTOINCREMENT high-water mark in sqlite_sequence via one atomic UPDATE ... RETURNING.
        /// </summary>
        protected override async Task<List<long>> GenerateKeysAsync(int count)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var tid = System.Environment.CurrentManagedThreadId;
            // Console.WriteLine($"[Diag-KeyGen] ENTER count={count} thread={tid}");

            await using var conn = await _dataSource.OpenConnectionAsync();
            var tConn = sw.ElapsedMilliseconds;
            // Console.WriteLine($"[Diag-KeyGen] OpenConnectionAsync +{tConn}ms thread={tid}");

            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "UPDATE sqlite_sequence SET seq = seq + @n WHERE name = @name RETURNING seq";
            cmd.Parameters.AddWithValue("@n", count);
            cmd.Parameters.AddWithValue("@name", SEQUENCE_TABLE);

            object? scalar;
            try
            {
                scalar = await cmd.ExecuteScalarAsync();
                var tScalar = sw.ElapsedMilliseconds - tConn;
                // Console.WriteLine($"[Diag-KeyGen] UPDATE sqlite_sequence RETURNING +{tScalar}ms (total {sw.ElapsedMilliseconds}ms) thread={tid}");
            }
            catch (Microsoft.Data.Sqlite.SqliteException ex)
            {
                // Console.WriteLine($"[Diag-KeyGen] UPDATE FAILED after {sw.ElapsedMilliseconds}ms thread={tid} errno={ex.SqliteErrorCode} msg='{ex.Message}'");
                throw;
            }

            if (scalar is null || scalar == DBNull.Value)
            {
                throw new InvalidOperationException(
                    $"sqlite_sequence row for '{SEQUENCE_TABLE}' not found. " +
                    "Ensure the schema (redbSqlite.sql) was applied — it materializes the sequence row.");
            }

            long top = Convert.ToInt64(scalar);
            var keys = new List<long>(count);
            for (long id = top - count + 1; id <= top; id++)
            {
                keys.Add(id);
            }

            // Console.WriteLine($"[Diag-KeyGen] EXIT keys=[{(keys.Count > 0 ? keys[0] : -1)}..{(keys.Count > 0 ? keys[^1] : -1)}] total={sw.ElapsedMilliseconds}ms thread={tid}");
            return keys;
        }
    }
}
