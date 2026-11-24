namespace redb.Core.Data;

/// <summary>
/// The rule of a database module newer than the package: never replaced, used when it serves the package. Shared by
/// the module check of PostgreSQL and SQL Server (versions such as "0.7.24", the compatibility function's
/// "&lt;oldest module served&gt;|&lt;package version&gt;") and the file check of SQLite (whole numbers).
/// </summary>
public static class ModuleCompatibility
{
    /// <summary>
    /// Whether <paramref name="deployed"/> is a newer version than <paramref name="required"/>. A version that does not
    /// read as numbers (a hand-set marker such as "0.0.0-test" reads as 0.0.0, anything else not at all) is not newer:
    /// such a module is replaced as an older one is.
    /// </summary>
    public static bool IsNewer(string? deployed, string required)
        => TryParse(deployed, out var d) && TryParse(required, out var r) && d > r;

    /// <summary>
    /// Whether a module that reports <paramref name="compat"/> (the value of its compatibility function, null when it
    /// has none) serves a package whose own module is <paramref name="required"/>: the first part of the value is not
    /// above it. Also returns the package version the module names, null when it names none.
    /// </summary>
    public static bool Serves(string? compat, string required, out string? fromPackage)
    {
        fromPackage = null;
        if (string.IsNullOrWhiteSpace(compat))
            return false;

        var parts = compat.Split('|');
        if (parts.Length == 2 && !string.IsNullOrWhiteSpace(parts[1]))
            fromPackage = parts[1].Trim();

        return TryParse(parts[0], out var oldest) && TryParse(required, out var own) && oldest <= own;
    }

    private static bool TryParse(string? text, out Version version)
    {
        version = new Version(0, 0);
        if (string.IsNullOrWhiteSpace(text))
            return false;
        var dash = text.IndexOf('-');
        var core = (dash >= 0 ? text[..dash] : text).Trim();
        if (!Version.TryParse(core, out var parsed))
            return false;
        version = parsed;
        return true;
    }
}
