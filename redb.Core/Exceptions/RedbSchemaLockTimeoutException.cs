using System;

namespace redb.Core.Exceptions;

/// <summary>
/// Start-up waited for the schema lock longer than <see cref="Models.Configuration.RedbServiceConfiguration.SchemaLockTimeout"/>:
/// another process holds it, most likely while it upgrades the same database. Building the indexes of a large table
/// takes minutes, so the usual remedies are a longer time-out or starting the nodes one after another.
///
/// <para>
/// A distinct type: a hosting layer that restarts the process on failure can tell "another node is still upgrading"
/// from a defect without parsing text.
/// </para>
/// </summary>
public class RedbSchemaLockTimeoutException : Exception
{
    /// <summary>Provider name as the dialect reports it.</summary>
    public string Provider { get; }

    /// <summary>The lock as the server knows it.</summary>
    public string LockName { get; }

    /// <summary>How long start-up waited.</summary>
    public TimeSpan Waited { get; }

    public RedbSchemaLockTimeoutException(string provider, string lockName, TimeSpan waited)
        : base($"{provider}: the schema lock '{lockName}' was not released within {waited}. Another process holds it, " +
               "most likely upgrading this database (the indexes of a large table take minutes to build). Raise " +
               "RedbServiceConfiguration.SchemaLockTimeout, or start the nodes one after another.")
    {
        Provider = provider;
        LockName = lockName;
        Waited = waited;
    }
}
