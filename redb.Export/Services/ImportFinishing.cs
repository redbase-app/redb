using System.Data;
using System.Data.Common;
using redb.Core.Query;
using redb.Export.Providers;

namespace redb.Export.Services;

/// <summary>
/// What an import does around its load, outside the load's transaction: which type rows the target already holds,
/// and after the load the scheme metadata cache and the note about time values stored as text by an older version.
/// </summary>
internal static class ImportFinishing
{
    /// <summary>_types ids of TimeOnly and TimeSpan, the same on every provider (RedbTypeIds).</summary>
    private const long TimeOnlyTypeId = -9223372036854775685;
    private const long TimeSpanTypeId = -9223372036854775684;

    /// <summary>The _types ids the target holds: its built-in types, kept by the import whatever the file says.</summary>
    public static async Task<HashSet<long>> TargetTypeIdsAsync(IDataProvider provider, CancellationToken ct)
    {
        var ids = new HashSet<long>();
        await using var command = provider.Connection.CreateCommand();
        command.CommandText = "SELECT _id FROM _types";
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            ids.Add(reader.GetInt64(0));
        return ids;
    }

    /// <summary>
    /// Builds the scheme metadata cache of every imported scheme through the dialect's per-scheme statement, as the
    /// scheme sync of a service does. Without it the first query of a Pro application rebuilt it, which is slow on a
    /// large database and needs write rights.
    /// </summary>
    public static async Task<int> SyncMetadataCacheAsync(IDataProvider provider, CancellationToken ct)
    {
        var schemes = new List<long>();
        await using (var read = provider.Connection.CreateCommand())
        {
            read.CommandText = "SELECT _id FROM _schemes";
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                schemes.Add(reader.GetInt64(0));
        }

        var (dialect, parameter) = provider.Name switch
        {
            "postgres" => ((ISqlDialect)new global::redb.Postgres.Sql.PostgreSqlDialect(), ""),
            "mssql" => (new global::redb.MSSql.Sql.MsSqlDialect(), "@p0"),
            "sqlite" => (new global::redb.SQLite.Sql.SqliteDialect(), "$1"),
            _ => throw new ArgumentException($"Unknown provider: {provider.Name}", nameof(provider)),
        };

        foreach (var scheme in schemes)
        {
            await using var sync = provider.Connection.CreateCommand();
            sync.CommandText = dialect.Schemes_SyncMetadataCache();
            sync.CommandTimeout = 0;
            var value = sync.CreateParameter();
            value.ParameterName = parameter;
            value.DbType = DbType.Int64;
            value.Value = scheme;
            sync.Parameters.Add(value);
            await sync.ExecuteNonQueryAsync(ct);
        }
        return schemes.Count;
    }

    /// <summary>
    /// How many imported values of TimeSpan and TimeOnly fields are stored as text: a file of a version before they
    /// were stored as ticks. The import leaves them as the file has them; the upgrade notes convert them.
    /// </summary>
    public static async Task<long> TimeValuesAsTextAsync(IDataProvider provider, CancellationToken ct)
    {
        await using var command = provider.Connection.CreateCommand();
        command.CommandText =
            "SELECT COUNT(*) FROM _values v JOIN _structures s ON s._id = v._id_structure " +
            $"WHERE s._id_type IN ({TimeOnlyTypeId}, {TimeSpanTypeId}) AND v._String IS NOT NULL";
        command.CommandTimeout = 0;
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }
}
