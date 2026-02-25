namespace redb.Export.Providers;

/// <summary>
/// Factory that creates <see cref="IDataProvider"/> instances by provider name.
/// </summary>
public static class ProviderFactory
{
    /// <summary>
    /// Creates a new <see cref="IDataProvider"/> for the given provider name.
    /// </summary>
    /// <param name="providerName">
    /// Case-insensitive provider name. Accepted values:
    /// <c>postgres</c>, <c>postgresql</c>, <c>pgsql</c>,
    /// <c>mssql</c>, <c>sqlserver</c>.
    /// </param>
    /// <returns>A new, unopened <see cref="IDataProvider"/> instance.</returns>
    /// <exception cref="NotSupportedException">The provider is recognized but not yet implemented.</exception>
    /// <exception cref="ArgumentException">Unknown provider name.</exception>
    public static IDataProvider Create(string providerName)
    {
        return providerName.ToLowerInvariant() switch
        {
            "postgres" or "postgresql" or "pgsql" => new PostgresProvider(),
            "mssql" or "sqlserver" => new MssqlProvider(),
            "oracle" => throw new NotSupportedException("Oracle provider is not implemented yet."),
            "sqlite" => throw new NotSupportedException("SQLite provider is not implemented yet."),
            _ => throw new ArgumentException($"Unknown provider: {providerName}", nameof(providerName))
        };
    }

    /// <summary>
    /// Returns the list of all recognized provider names (including not-yet-implemented ones).
    /// </summary>
    public static string[] SupportedProviders => ["postgres", "mssql", "oracle", "sqlite"];
}
