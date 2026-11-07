namespace redb.Core.Exceptions;

/// <summary>
/// The database schema is behind the version this build requires, and the application could not
/// bring it up to date itself.
///
/// <para>
/// Start-up applies the versioned SQL module automatically when the connected role is allowed to.
/// It often is not: the DBA grants owner rights for the initial installation and revokes them
/// afterwards, so <c>ALTER TABLE</c> and <c>CREATE OR REPLACE FUNCTION</c> from the application
/// fail with a privilege error. Before this type that surfaced as a raw driver exception with no
/// indication of what to do. Now it says what is deployed, what is required, what could not be
/// applied, and how to get the script for the DBA — the same text the application would have run.
/// </para>
///
/// <para>
/// A distinct type, not <c>InvalidOperationException</c>: a hosting layer has to be able to tell
/// "someone must run the upgrade script" from every other start-up failure without parsing text.
/// </para>
/// </summary>
public class RedbSchemaOutdatedException : Exception
{
    /// <summary>Module version the database reports, or <c>null</c> when the module is absent.</summary>
    public string? DeployedVersion { get; }

    /// <summary>Module version this build ships and requires.</summary>
    public string RequiredVersion { get; }

    /// <summary>Provider name as the dialect reports it, for the operator's convenience.</summary>
    public string Provider { get; }

    /// <summary>
    /// Why the application did not apply the upgrade itself: a privilege error from the driver, or
    /// <c>null</c> when automatic application is disabled by configuration.
    /// </summary>
    public Exception? Cause { get; }

    public RedbSchemaOutdatedException(
        string provider,
        string? deployedVersion,
        string requiredVersion,
        Exception? cause)
        : base(BuildMessage(provider, deployedVersion, requiredVersion, cause), cause)
    {
        Provider = provider;
        DeployedVersion = deployedVersion;
        RequiredVersion = requiredVersion;
        Cause = cause;
    }

    private static string BuildMessage(string provider, string? deployed, string required, Exception? cause)
    {
        var reason = cause is null
            ? "automatic upgrades are disabled (AutoApplyDatabaseUpgrades = false)"
            : $"the connected role is not allowed to apply it ({cause.GetType().Name}: {cause.Message})";

        return
            $"The {provider} database schema is outdated: deployed module version is " +
            $"'{deployed ?? "<none>"}', this build requires '{required}', and {reason}. " +
            "Nothing was changed. Have the schema owner apply the upgrade script: export it with " +
            "IRedbService.GetUpgradeScript() or `redb schema --upgrade --provider <name>`, then " +
            "run it with psql / sqlcmd. The script is idempotent and safe to re-run.";
    }
}
