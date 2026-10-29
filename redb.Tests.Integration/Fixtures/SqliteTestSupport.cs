using System.Text.RegularExpressions;
using redb.Core.Data;

namespace redb.Tests.Integration.Fixtures;

/// <summary>Shared helpers for the SQLite fixtures.</summary>
internal static class SqliteTestSupport
{
    /// <summary>
    /// Locate the Free native loadable extension (redbsqlite.{dll,so,dylib}) by walking up from the test
    /// output directory to redb.SQLite/native/build/. Returns null to fall back to the packaged locator.
    /// </summary>
    public static string? ResolveNativeExtension()
    {
        var suffix = OperatingSystem.IsWindows() ? ".dll"
                   : OperatingSystem.IsMacOS() ? ".dylib"
                   : ".so";

        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "redb.SQLite", "native", "build", "redbsqlite" + suffix);
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }

    /// <summary>
    /// Delete the SQLite DB file (and its -wal/-shm siblings) named in the connection string, so each
    /// run starts on a freshly created schema (datetimes are REAL Julian; an old TEXT-schema file is
    /// NOT auto-migrated).
    /// </summary>
    public static void DeleteDbFiles(string connectionString)
    {
        RegisterSweepAtExit();
        var m = Regex.Match(connectionString, @"Data Source\s*=\s*([^;]+)", RegexOptions.IgnoreCase);
        if (!m.Success) return;

        var file = m.Groups[1].Value.Trim();
        foreach (var f in new[] { file, file + "-wal", file + "-shm" })
        {
            try { if (File.Exists(f)) File.Delete(f); } catch { /* locked from a prior run — ignore */ }
        }

        // The id cache is a process-wide static in RedbKeyGeneratorBase, holding up to 10000 ids already
        // reserved against the file we have just deleted. The replacement file starts its sqlite_sequence
        // at zero, so the surviving cache and the fresh sequence hand out overlapping ids and inserts die
        // with "UNIQUE constraint failed: _values._id". Both SQLite fixtures share one file inside one
        // process, so the second one to start is the one that trips over it, which is why this never
        // reproduces when a single test class is run alone.
        RedbKeyGeneratorBase.ClearAllCaches();
    }

    private static int _sweepRegistered;

    /// <summary>
    /// The SQLite files the tests create (relative <c>Data Source=redb_tests_*.db</c>, <c>redb_diag_*.db</c>) are
    /// deleted before a test uses them but were never deleted after, and the ones named per test by a GUID piled
    /// up in the output directory, a few hundred per run. They are swept when the process exits, after the
    /// connection pools are closed (a pooled connection keeps the file open); the sweep goes by name, so it also
    /// takes what a killed run left. A file still open is skipped.
    /// <para>
    /// Registered from <see cref="DeleteDbFiles"/>, which every SQLite fixture and test calls first, and not from a
    /// module initializer: a test loads a copy of this assembly into a collectible context, the initializer ran in
    /// that copy too, and its handler on the process-wide event kept the context alive. Nor at start: the first
    /// call falls between the starts of collections running in parallel, and a sweep then could take a file a
    /// neighbour had just created.
    /// </para>
    /// </summary>
    private static void RegisterSweepAtExit()
    {
        if (Interlocked.Exchange(ref _sweepRegistered, 1) == 1) return;
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            SweepTestDatabases();
        };
    }

    private static void SweepTestDatabases()
    {
        // A relative Data Source resolves against the current directory, which the runner sets to the output
        // directory; both are swept in case they differ.
        var directories = new[] { AppContext.BaseDirectory, Environment.CurrentDirectory }
            .Select(Path.GetFullPath)
            .Distinct(StringComparer.OrdinalIgnoreCase);
        foreach (var directory in directories)
        {
            foreach (var pattern in new[] { "redb_tests_*.db*", "redb_diag_*.db*" })
            {
                string[] files;
                try { files = Directory.GetFiles(directory, pattern); }
                catch (IOException) { continue; }
                catch (UnauthorizedAccessException) { continue; }

                foreach (var file in files)
                {
                    if (!(file.EndsWith(".db", StringComparison.OrdinalIgnoreCase)
                          || file.EndsWith(".db-wal", StringComparison.OrdinalIgnoreCase)
                          || file.EndsWith(".db-shm", StringComparison.OrdinalIgnoreCase)))
                        continue;
                    try { File.Delete(file); }
                    catch (IOException) { /* open in another process or connection */ }
                    catch (UnauthorizedAccessException) { }
                }
            }
        }
    }
}
