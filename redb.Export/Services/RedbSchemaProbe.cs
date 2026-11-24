using System.Data;
using System.Data.Common;
using redb.Core.Query;
using redb.Export.Providers;

namespace redb.Export.Services;

/// <summary>Whether a database holds the redb schema at all: its core table <c>_schemes</c>, as a service start asks.</summary>
internal static class RedbSchemaProbe
{
    public static async Task<bool> ExistsAsync(IDataProvider provider, CancellationToken ct)
    {
        var (dialect, table, schema) = provider.Name switch
        {
            "postgres" => ((ISqlDialect)new global::redb.Postgres.Sql.PostgreSqlDialect(), "", ""),
            "mssql" => (new global::redb.MSSql.Sql.MsSqlDialect(), "@p0", "@p1"),
            "sqlite" => (new global::redb.SQLite.Sql.SqliteDialect(), "$1", "$2"),
            _ => throw new ArgumentException($"Unknown provider: {provider.Name}", nameof(provider)),
        };

        await using var command = provider.Connection.CreateCommand();
        command.CommandText = dialect.Maintenance_SelectTableExists();
        command.Parameters.Add(Parameter(command, table, "_schemes"));
        command.Parameters.Add(Parameter(command, schema, null));
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct)) > 0;
    }

    /// <summary>A parameter by the name the dialect's statement uses; an empty name is a positional one (PostgreSQL).</summary>
    private static DbParameter Parameter(DbCommand command, string name, string? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = DbType.String;
        parameter.Value = (object?)value ?? DBNull.Value;
        return parameter;
    }
}
