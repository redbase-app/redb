namespace redb.Core.Exceptions;

/// <summary>
/// The database carries a redb module (or, on SQLite, a file schema) newer than this package's, and that newer module
/// does not serve this package. Start-up stops and leaves the database as it is.
///
/// <para>
/// A package never puts its own, older module over a newer one: a node of the previous version in a cluster that is
/// being upgraded would otherwise take the database back for every node. When the newer module still serves this
/// package (it says so itself: <c>pvt_module_compat()</c> on PostgreSQL and SQL Server, the <c>_schema_compat</c> row
/// of a SQLite file) the start goes on with a warning; when it does not, this exception is thrown. The way out is
/// upgrading the redb packages of this application.
/// </para>
///
/// <para>
/// A sibling of <see cref="RedbSchemaOutdatedException"/>, not a case of it: that one asks for the database to be
/// upgraded, this one for the application.
/// </para>
/// </summary>
public class RedbSchemaNewerException : Exception
{
    /// <summary>Module version (SQLite: file schema version) the database reports.</summary>
    public string DeployedVersion { get; }

    /// <summary>Module version (SQLite: file schema version) this package ships.</summary>
    public string PackageVersion { get; }

    /// <summary>
    /// The redb package version from which on the database is served, as the database itself states it; <c>null</c>
    /// when the database states nothing (a module without the compatibility function, a file without the row).
    /// </summary>
    public string? RequiredPackageVersion { get; }

    /// <summary>Provider name as the dialect reports it.</summary>
    public string Provider { get; }

    public RedbSchemaNewerException(string provider, string deployedVersion, string packageVersion, string? requiredPackageVersion)
        : base(BuildMessage(provider, deployedVersion, packageVersion, requiredPackageVersion))
    {
        Provider = provider;
        DeployedVersion = deployedVersion;
        PackageVersion = packageVersion;
        RequiredPackageVersion = requiredPackageVersion;
    }

    /// <summary>
    /// For a caller outside the service start, such as a tool, that reads the version and judges it itself: the same
    /// first sentence, then the caller's own way out instead of upgrading the application's packages.
    /// </summary>
    /// <param name="provider">Provider name as the dialect reports it.</param>
    /// <param name="deployedVersion">Module version (SQLite: file schema version) the database reports.</param>
    /// <param name="packageVersion">Module version (SQLite: file schema version) this package ships.</param>
    /// <param name="requiredPackageVersion">The package version the database names as served from, null when it names none.</param>
    /// <param name="remedy">What the operator does about it: the closing sentence of the message.</param>
    public RedbSchemaNewerException(
        string provider, string deployedVersion, string packageVersion, string? requiredPackageVersion, string remedy)
        : base($"{Newer(provider, deployedVersion, packageVersion, requiredPackageVersion)} The database was left unchanged. {remedy}")
    {
        Provider = provider;
        DeployedVersion = deployedVersion;
        PackageVersion = packageVersion;
        RequiredPackageVersion = requiredPackageVersion;
    }

    private static string BuildMessage(string provider, string deployed, string package, string? requiredPackage) =>
        $"{Newer(provider, deployed, package, requiredPackage)} " +
        "Upgrade the redb packages of this application; the database was left unchanged.";

    /// <summary>The first sentence of every message: what is deployed, what this package ships, what the database asks for.</summary>
    private static string Newer(string provider, string deployed, string package, string? requiredPackage)
    {
        var needs = requiredPackage is null
            ? "it does not state which packages it serves"
            : $"it needs redb packages from {requiredPackage} on";
        return $"The {provider} database carries the redb module {deployed}, newer than {package} of this package, and {needs}.";
    }
}
