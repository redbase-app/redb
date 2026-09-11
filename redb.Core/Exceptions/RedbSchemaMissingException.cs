namespace redb.Core.Exceptions;

/// <summary>
/// The database has no redb schema at all - the core table <c>_schemes</c> is missing - and
/// initialization was asked not to create it (<c>InitializeAsync()</c> without
/// <c>ensureCreated: true</c>).
///
/// <para>
/// Schema creation is opt-in on purpose: the application role usually may not run DDL, and an
/// unexpected empty database more often means a connection string pointing at the wrong one than
/// a fresh installation. Before this type the first start-up step - the module version probe or
/// the scheme sync - surfaced a raw driver error ("relation _schemes does not exist") that said
/// nothing about how to proceed. Now the failure names the situation and the two ways out.
/// </para>
/// </summary>
public class RedbSchemaMissingException : Exception
{
    /// <summary>Provider name as the service reports it, for the operator's convenience.</summary>
    public string Provider { get; }

    public RedbSchemaMissingException(string provider)
        : base(BuildMessage(provider))
    {
        Provider = provider;
    }

    private static string BuildMessage(string provider) =>
        $"The {provider} database has no redb schema (table _schemes not found) and initialization " +
        "was asked not to create it. Create it with InitializeAsync(ensureCreated: true) " +
        "(RedbServiceConfiguration.EnsureCreated = true for the hosted-service start-up), or have the " +
        "schema owner run the script from IRedbService.GetSchemaScript(). If this database was not " +
        "expected to be empty, check the connection string first.";
}
