using System.Text.RegularExpressions;

namespace redb.Tests.Integration.Fixtures;

/// <summary>Shared helpers for the SQLite fixtures.</summary>
internal static class SqliteTestSupport
{
    /// <summary>
    /// Locate the Free native loadable extension (redb.{dll,so,dylib}) by walking up from the test
    /// output directory to redb.SQLite/native/build/. Returns null to fall back to the packaged locator.
    /// </summary>
    public static string? ResolveNativeExtension()
    {
        var suffix = OperatingSystem.IsWindows() ? ".dll"
                   : OperatingSystem.IsMacOS() ? ".dylib"
                   : ".so";

        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "redb.SQLite", "native", "build", "redb" + suffix);
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
        var m = Regex.Match(connectionString, @"Data Source\s*=\s*([^;]+)", RegexOptions.IgnoreCase);
        if (!m.Success) return;

        var file = m.Groups[1].Value.Trim();
        foreach (var f in new[] { file, file + "-wal", file + "-shm" })
        {
            try { if (File.Exists(f)) File.Delete(f); } catch { /* locked from a prior run — ignore */ }
        }
    }
}
