using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Npgsql;
using redb.Core.Query;

namespace redb.Postgres.Data
{
    /// <summary>
    /// Builds the <see cref="NpgsqlDataSource"/> every registration path uses, so the
    /// <c>redb.string_collation</c> and <c>redb.lazy_refs</c> session settings are installed in one
    /// place instead of six.
    ///
    /// <para>
    /// <b>Why a GUC at all.</b> The Free query path builds its SQL inside the database:
    /// <c>pvt_build_query_sql</c> and the functions below it generate the text, and they take twelve
    /// positional parameters with nowhere to put an option. Threading a thirteenth through five
    /// layers would change every one of those signatures. A run-time setting reaches them without
    /// touching a single signature, and <c>pvt_fold_case()</c> reads it with
    /// <c>current_setting('redb.string_collation', true)</c>, which yields NULL when unset. Unset
    /// therefore means byte-for-byte the SQL that was generated before this feature existed. The
    /// lazy-reference flag (V4, Л2) rides the same mechanism into the JSON builders.
    /// </para>
    ///
    /// <para>
    /// <b>Why on every hand-out, not in the physical-connection initializer.</b> A session GUC set
    /// with <c>set_config(..., false)</c> lives for the session - but Npgsql sends <c>DISCARD ALL</c>
    /// when a connection returns to the pool, and <c>RESET ALL</c> inside it puts every session
    /// setting back to its default. The physical-connection initializer runs once per backend, so
    /// only the first scope on each pooled connection saw the settings; every later scope got a
    /// connection with both GUCs empty (review, V4). The settings are therefore registered per data
    /// source here and re-applied by <see cref="NpgsqlRedbConnection"/> each time it opens its
    /// connection: one round trip per context, the same shape MSSQL and SQLite use.
    /// </para>
    /// </summary>
    public static class NpgsqlDataSourceFactory
    {
        private sealed record SessionSettings(string? Collation, bool LazyReferences);

        // Weak by design: the data source owns its lifetime, the registry never keeps it alive.
        private static readonly ConditionalWeakTable<NpgsqlDataSource, SessionSettings> Registry = new();

        /// <summary>
        /// Creates a data source. With <paramref name="stringCollation"/> null and
        /// <paramref name="lazyReferences"/> false this is <see cref="NpgsqlDataSource.Create(string)"/>
        /// and nothing else.
        /// </summary>
        public static NpgsqlDataSource Create(string connectionString, string? stringCollation, bool lazyReferences = false)
        {
            if (string.IsNullOrWhiteSpace(stringCollation) && !lazyReferences)
                return NpgsqlDataSource.Create(connectionString);

            // Validated before it can reach any SQL. It is also never interpolated: set_config
            // takes the value as a bound parameter, so there is no text to escape here at all.
            // (The in-database side does have to build an identifier, and quotes it there.)
            if (stringCollation != null) CollationNameValidator.Validate(stringCollation);

            var dataSource = NpgsqlDataSource.Create(connectionString);
            Registry.Add(dataSource, new SessionSettings(stringCollation, lazyReferences));
            return dataSource;
        }

        /// <summary>
        /// Re-applies the session settings registered for <paramref name="dataSource"/> on a
        /// connection it has just handed out. No-op for a data source created without settings.
        /// </summary>
        public static Task ApplySessionSettingsAsync(NpgsqlDataSource dataSource, NpgsqlConnection connection)
        {
            if (!Registry.TryGetValue(dataSource, out var settings))
                return Task.CompletedTask;

            return ApplySettingsAsync(connection, settings.Collation, settings.LazyReferences);
        }

        private static async Task ApplySettingsAsync(NpgsqlConnection connection, string? collation, bool lazyReferences)
        {
            if (collation != null) await ApplyCollationAsync(connection, collation).ConfigureAwait(false);
            if (lazyReferences) await ApplyLazyRefsAsync(connection).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronous <see cref="ApplySessionSettingsAsync"/> for the thread-pool-free lazy path:
        /// same settings, executed on the calling thread.
        /// </summary>
        public static void ApplySessionSettings(NpgsqlDataSource dataSource, NpgsqlConnection connection)
        {
            if (!Registry.TryGetValue(dataSource, out var settings))
                return;

            if (settings.Collation != null)
            {
                using var cmd = CreateCommand(connection, settings.Collation);
                cmd.ExecuteNonQuery();
            }
            if (settings.LazyReferences)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT set_config('redb.lazy_refs', '1', false)";
                cmd.ExecuteNonQuery();
            }
        }

        private static async Task ApplyLazyRefsAsync(NpgsqlConnection connection)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT set_config('redb.lazy_refs', '1', false)";
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
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
