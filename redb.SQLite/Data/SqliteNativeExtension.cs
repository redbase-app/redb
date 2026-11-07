using System;

namespace redb.SQLite.Data
{
    /// <summary>
    /// The version gate of the native extension: the loaded <c>redbsqlite</c> library must report the version the
    /// dialect requires (<c>pvt_module_version()</c>), as the PostgreSQL and SQL Server modules must. A stale build
    /// next to the application loaded silently and answered in an old shape - list item JSON in the old form, a regex
    /// filter failing with an unrelated error.
    /// </summary>
    public static class SqliteNativeExtension
    {
        /// <summary>
        /// Throws when <paramref name="deployed"/> - what the loaded extension reports, null when it has no
        /// <c>pvt_module_version()</c> - is not <paramref name="required"/>.
        /// </summary>
        public static void EnsureVersion(string path, string? deployed, string required)
        {
            if (string.Equals(deployed, required, StringComparison.Ordinal))
                return;
            var found = deployed == null
                ? "has no pvt_module_version() function"
                : $"reports version {deployed}";
            throw new InvalidOperationException(
                $"The redb native SQLite extension '{path}' {found}; this build of redb.SQLite requires version " +
                $"{required}. The library is built from redb.SQLite/native and ships with the package: update the " +
                "package, or rebuild the extension (redb.SQLite/native/README.md) and replace the file next to the " +
                "application.");
        }
    }
}
