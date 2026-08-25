using System;
using System.Threading.Tasks;
using Npgsql;
using redb.Core.Query;

namespace redb.Postgres.Data
{
    /// <summary>
    /// Builds the <see cref="NpgsqlDataSource"/> every registration path uses, so the
    /// <c>redb.string_collation</c> setting is installed in one place instead of six.
    ///
    /// <para>
    /// <b>Why a GUC at all.</b> The Free query path builds its SQL inside the database:
    /// <c>pvt_build_query_sql</c> and the functions below it generate the text, and they take twelve
    /// positional parameters with nowhere to put an option. Threading a thirteenth through five
    /// layers would change every one of those signatures. A run-time setting reaches them without
    /// touching a single signature, and <c>pvt_fold_case()</c> reads it with
    /// <c>current_setting('redb.string_collation', true)</c>, which yields NULL when unset. Unset
    /// therefore means byte-for-byte the SQL that was generated before this feature existed.
    /// </para>
    ///
    /// <para>
    /// <b>Why the physical-connection initializer.</b> A GUC set with plain <c>SET</c> lives for the
    /// session, and Npgsql pools sessions. The initializer runs once per physical connection, which
    /// is exactly the lifetime of the setting: set on every new backend, never re-sent on a pooled
    /// handout.
    /// </para>
    /// </summary>
    public static class NpgsqlDataSourceFactory
    {
        /// <summary>
        /// Creates a data source. With <paramref name="stringCollation"/> null this is
        /// <see cref="NpgsqlDataSource.Create(string)"/> and nothing else.
        /// </summary>
        public static NpgsqlDataSource Create(string connectionString, string? stringCollation)
        {
            if (string.IsNullOrWhiteSpace(stringCollation))
                return NpgsqlDataSource.Create(connectionString);

            // Validated before it can reach any SQL. It is also never interpolated: set_config
            // takes the value as a bound parameter, so there is no text to escape here at all.
            // (The in-database side does have to build an identifier, and quotes it there.)
            CollationNameValidator.Validate(stringCollation!);

            var builder = new NpgsqlDataSourceBuilder(connectionString);
            builder.UsePhysicalConnectionInitializer(
                conn => ApplyCollation(conn, stringCollation!),
                async conn => await ApplyCollationAsync(conn, stringCollation!).ConfigureAwait(false));

            return builder.Build();
        }

        private static void ApplyCollation(NpgsqlConnection connection, string collation)
        {
            using var cmd = CreateCommand(connection, collation);
            cmd.ExecuteNonQuery();
        }

        private static async Task ApplyCollationAsync(NpgsqlConnection connection, string collation)
        {
            await using var cmd = CreateCommand(connection, collation);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// <c>set_config</c> rather than <c>SET</c>: it is an ordinary function, so the value binds
        /// as a parameter. <c>SET</c> takes a literal and would have to be built by concatenation.
        /// The third argument false makes the setting session-wide rather than transaction-local.
        /// </summary>
        private static NpgsqlCommand CreateCommand(NpgsqlConnection connection, string collation)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT set_config('redb.string_collation', $1, false)";
            cmd.Parameters.Add(new NpgsqlParameter { Value = collation });
            return cmd;
        }
    }
}
