using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace redb.SQLite.Data
{
    /// <summary>
    /// Lightweight stand-in for Npgsql's NpgsqlDataSource concept.
    /// Microsoft.Data.Sqlite has no DataSource type — it pools connections
    /// internally, keyed by connection string. This wrapper centralizes
    /// connection creation and applies the per-connection PRAGMAs that SQLite
    /// requires on EVERY connection (they are not persistent):
    ///   - foreign_keys=ON  (so ON DELETE CASCADE / SET NULL fire)
    ///   - busy_timeout     (so a second writer waits instead of failing with
    ///                       SQLITE_BUSY — SQLite is single-writer)
    /// </summary>
    public sealed class SqliteDataSource
    {
        private const int BusyTimeoutMs = 5000;

        static SqliteDataSource()
        {
            // SQLite stores datetimes as REAL Julian day (UTC). Object reads go through
            // get_object_json (already ISO), but analytics paths (Min/Max, AggregateRedbAsync,
            // Window, GroupBy) select the raw column and hand the bare NUMBER to core's
            // converters. Register the numeric→temporal decoder so core can turn it back into a
            // DateTime/DateTimeOffset/DateOnly — without core knowing anything about Julian days.
            // Runs once whenever SQLite is configured (Free and Pro both create a SqliteDataSource);
            // no-op for PostgreSQL/SQL Server, which never return a number for a temporal column.
            Core.Query.Utils.TemporalDecoder.NumericDecoder = static (julian, targetType) =>
            {
                var dto = SqliteJulian.FromJulian(julian);
                if (targetType == typeof(DateTimeOffset)) return dto;
                if (targetType == typeof(DateOnly)) return DateOnly.FromDateTime(dto.UtcDateTime);
                return dto.UtcDateTime; // DateTime
            };
        }

        /// <summary>
        /// Path to the REDB native loadable extension (redb.dll/.so/.dylib, WITHOUT
        /// the file suffix — SQLite appends it). When set, every connection loads it
        /// after PRAGMAs, so the FREE tier's in-DB functions (get_object_json,
        /// save_object_json, pvt_build_query_sql, ...) become callable. REQUIRED for
        /// the Free tier; harmless for Pro (which never calls those functions).
        /// Defaults to the REDB_SQLITE_EXTENSION env var. Load is per-connection
        /// (SQLite extensions are not persistent across connections).
        /// </summary>
        public static string? NativeExtensionPath { get; set; }
            = Environment.GetEnvironmentVariable("REDB_SQLITE_EXTENSION");

        /// <summary>Connection string this data source opens.</summary>
        public string ConnectionString { get; }

        private SqliteDataSource(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>Create a data source for the given connection string.</summary>
        public static SqliteDataSource Create(string connectionString)
            => new SqliteDataSource(connectionString);

        /// <summary>Open a new pooled connection with REDB pragmas applied.</summary>
        public SqliteConnection OpenConnection()
        {
            var conn = new SqliteConnection(ConnectionString);
            conn.Open();
            LoadNativeExtension(conn);
            ApplyPragmas(conn);
            EnsureCleanTransactionState(conn);
            return conn;
        }

        /// <summary>Open a new pooled connection with REDB pragmas applied.</summary>
        public async Task<SqliteConnection> OpenConnectionAsync()
        {
            var conn = new SqliteConnection(ConnectionString);
            await conn.OpenAsync();
            LoadNativeExtension(conn);
            await ApplyPragmasAsync(conn);
            await EnsureCleanTransactionStateAsync(conn);
            return conn;
        }

        /// <summary>Load the REDB native extension on this connection (Free tier). No-op
        /// when NativeExtensionPath is unset (Pro materializes/queries in C#).</summary>
        private static void LoadNativeExtension(SqliteConnection conn)
        {
            var path = NativeExtensionPath;
            if (string.IsNullOrEmpty(path)) return;
            conn.EnableExtensions(true);
            conn.LoadExtension(path);   // entry point sqlite3_redb_init (default by basename)
        }

        /// <summary>
        /// Locate the REDB native extension shipped inside the NuGet package, relative to
        /// the running application (<see cref="AppContext.BaseDirectory"/>). Returns the full
        /// path (suffix included) or null if not found.
        ///
        /// Probes the idiomatic <c>runtimes/&lt;rid&gt;/native</c> layout (populated by a
        /// RID-specific publish and by the package's buildTransitive .targets for
        /// framework-dependent builds), then a flattened copy next to the app. Used by the
        /// Free DI registration to auto-configure <see cref="NativeExtensionPath"/> without
        /// an explicit <c>REDB_SQLITE_EXTENSION</c> env var. As a dev fallback it also walks
        /// up the source tree to the CMake build output, so a ProjectReference run straight
        /// from the repo (a worker, tests) loads the extension with no manual setup.
        /// </summary>
        public static string? LocatePackagedExtension()
        {
            var lib = OperatingSystem.IsWindows() ? "redb.dll"
                    : OperatingSystem.IsMacOS()   ? "redb.dylib"
                    : "redb.so";
            var baseDir = AppContext.BaseDirectory;
            var rid = System.Runtime.InteropServices.RuntimeInformation.RuntimeIdentifier;

            // 1. Exact RID path, then flattened next to the app (RID publish / manual copy).
            foreach (var candidate in new[]
                     {
                         Path.Combine(baseDir, "runtimes", rid, "native", lib),
                         Path.Combine(baseDir, lib),
                     })
            {
                if (File.Exists(candidate)) return candidate;
            }

            // 2. RID-agnostic scan: any runtimes/<rid>/native/<lib> under the app base.
            var runtimesRoot = Path.Combine(baseDir, "runtimes");
            if (Directory.Exists(runtimesRoot))
            {
                foreach (var ridDir in Directory.EnumerateDirectories(runtimesRoot))
                {
                    var candidate = Path.Combine(ridDir, "native", lib);
                    if (File.Exists(candidate)) return candidate;
                }
            }

            // 3. Dev fallback: walk up the source tree to the CMake build output
            //    (redb.SQLite/native/build/redb.<ext>). Lets a ProjectReference run
            //    straight from the repo (worker, tests, console) load the extension
            //    without REDB_SQLITE_EXTENSION — mirrors redb.Examples' resolver.
            for (var dir = new DirectoryInfo(baseDir); dir != null; dir = dir.Parent)
            {
                var devCandidate = Path.Combine(dir.FullName, "redb.SQLite", "native", "build", lib);
                if (File.Exists(devCandidate)) return devCandidate;
            }

            return null;
        }

        private static void ApplyPragmas(SqliteConnection conn)
        {
            using var cmd = conn.CreateCommand();
            // journal_mode=WAL → writers don't block readers (concurrent reads
            // during an open write tx are crucial for redb's check-then-save
            // patterns and for any visibility-of-uncommitted-writes probe).
            // synchronous=NORMAL is the WAL-recommended balance; matches what
            // most production SQLite deployments use under Microsoft.Data.Sqlite.
            cmd.CommandText = $"PRAGMA foreign_keys=ON; PRAGMA busy_timeout={BusyTimeoutMs}; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
            cmd.ExecuteNonQuery();
        }

        private static async Task ApplyPragmasAsync(SqliteConnection conn)
        {
            await using var cmd = conn.CreateCommand();
            // journal_mode=WAL → writers don't block readers (concurrent reads
            // during an open write tx are crucial for redb's check-then-save
            // patterns and for any visibility-of-uncommitted-writes probe).
            // synchronous=NORMAL is the WAL-recommended balance; matches what
            // most production SQLite deployments use under Microsoft.Data.Sqlite.
            cmd.CommandText = $"PRAGMA foreign_keys=ON; PRAGMA busy_timeout={BusyTimeoutMs}; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
            await cmd.ExecuteNonQueryAsync();
        }

        // Defensive pool-poisoning guard. Microsoft.Data.Sqlite's pool returns
        // SqliteConnection wrappers without inspecting the underlying sqlite3
        // handle's autocommit state, so a prior caller that failed to COMMIT or
        // ROLLBACK (e.g. swallowed rollback in a dispose path) hands us a handle
        // with autocommit=0 — the next conn.BeginTransaction(...) then fails
        // with SQLITE_ERROR(1) "cannot start a transaction within a transaction".
        // A speculative ROLLBACK clears any leaked tx; the "no tx active" error
        // is the normal/clean case and is silently caught.
        //
        // Same idiom as Npgsql's DISCARD ALL / RESET ALL on connection acquire.
        private static void EnsureCleanTransactionState(SqliteConnection conn)
        {
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "ROLLBACK";
                cmd.ExecuteNonQuery();
                // Reached only when the pooled connection HAD a leaked tx — log
                // so the source of the leak surfaces in operator logs.
                // Console.WriteLine("[Diag-TXLOCK] POOL-CLEANUP: rolled back leaked tx on pooled connection acquire (sync path).");
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 1)
            {
                // SQLITE_ERROR(1): "cannot rollback - no transaction is active" — clean handle, expected.
            }
        }

        private static async Task EnsureCleanTransactionStateAsync(SqliteConnection conn)
        {
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "ROLLBACK";
                await cmd.ExecuteNonQueryAsync();
                // Console.WriteLine("[Diag-TXLOCK] POOL-CLEANUP: rolled back leaked tx on pooled connection acquire (async path).");
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 1)
            {
                // SQLITE_ERROR(1): "cannot rollback - no transaction is active" — clean handle, expected.
            }
        }
    }
}
