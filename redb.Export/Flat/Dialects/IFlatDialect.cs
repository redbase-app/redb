using System.Data;

namespace redb.Export.Flat.Dialects;

/// <summary>
/// What differs between the three providers when flat tables are created and filled: identifier
/// quoting, column types, the value form the provider's bulk insert accepts, and how a consistent
/// read of the source is obtained.
/// </summary>
internal interface IFlatDialect
{
    /// <summary>Quotes an identifier, doubling the closing quote character inside it.</summary>
    string Quote(string identifier);

    /// <summary>Column type for <paramref name="type"/>.</summary>
    string ColumnType(FlatStorageType type);

    /// <summary>
    /// True when foreign keys can only be declared inside <c>CREATE TABLE</c> (SQLite). Otherwise
    /// they are added after the data is loaded, so that load order does not matter.
    /// </summary>
    bool InlineForeignKeys { get; }

    /// <summary>Name of a column as the provider's <c>BulkInsertAsync</c> expects it in the <see cref="DataTable"/>.</summary>
    string BulkColumnName(string column);

    /// <summary>Converts a non-null CLR value of <paramref name="type"/> to the form the provider's bulk insert writes correctly.</summary>
    object ToDbValue(object value, FlatStorageType type);

    /// <summary>Isolation level under which a source of this provider is read as one snapshot.</summary>
    IsolationLevel SnapshotIsolation { get; }
}

/// <summary>Resolves the dialect of a provider by its <c>IDataProvider.Name</c>.</summary>
internal static class FlatDialects
{
    public static IFlatDialect For(string providerName) => providerName switch
    {
        "postgres" => new PostgresFlatDialect(),
        "mssql" => new MssqlFlatDialect(),
        "sqlite" => new SqliteFlatDialect(),
        _ => throw new NotSupportedException($"Flat export is not available for provider '{providerName}'.")
    };
}
