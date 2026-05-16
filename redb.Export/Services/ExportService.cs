using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using redb.Export.Models;
using redb.Export.Providers;

namespace redb.Export.Services;

/// <summary>
/// Exports an entire REDB database (or a filtered subset of schemes) to a <c>.redb</c> file.
/// <para>
/// The output is a JSONL (line-delimited JSON) stream, optionally wrapped in a ZIP archive.
/// Records are written in foreign-key-safe order so the file can be imported with a single pass.
/// </para>
/// </summary>
public sealed class ExportService
{
    private readonly IDataProvider _provider;
    private readonly bool _verbose;
    private readonly int _batchSize;

    private long _typesCount;
    private long _rolesCount;
    private long _usersCount;
    private long _userRolesCount;
    private long _listsCount;
    private long _listItemsCount;
    private long _schemesCount;
    private long _structuresCount;
    private long _objectsCount;
    private long _permissionsCount;
    private long _valuesCount;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    /// <summary>
    /// Initializes a new instance of <see cref="ExportService"/>.
    /// </summary>
    /// <param name="provider">An opened <see cref="IDataProvider"/>.</param>
    /// <param name="verbose">When <c>true</c>, progress is written to <see cref="Console"/>.</param>
    /// <param name="batchSize">Number of records per batch (affects memory usage).</param>
    public ExportService(IDataProvider provider, bool verbose, int batchSize)
    {
        _provider = provider;
        _verbose = verbose;
        _batchSize = batchSize;
    }

    /// <summary>
    /// Exports the database to the specified file path.
    /// </summary>
    /// <param name="outputPath">
    /// Destination file path. The <c>.redb</c> extension is appended automatically if missing.
    /// </param>
    /// <param name="schemeIds">
    /// Optional array of scheme identifiers to export.
    /// When <c>null</c> or empty, all schemes are included.
    /// </param>
    /// <param name="compress">When <c>true</c>, the JSONL stream is wrapped in a ZIP archive.</param>
    /// <param name="dryRun">When <c>true</c>, only prints statistics without writing a file.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task ExportAsync(
        string outputPath,
        long[]? schemeIds,
        bool compress,
        bool dryRun,
        CancellationToken ct = default)
    {
        var startTime = DateTime.UtcNow;

        if (dryRun)
        {
            await PrintDryRunInfoAsync(schemeIds, ct);
            return;
        }

        var actualPath = outputPath;
        if (!actualPath.EndsWith(".redb", StringComparison.OrdinalIgnoreCase))
        {
            actualPath += ".redb";
        }

        Log($"Exporting to: {actualPath}");

        long sequenceValue;
        TimeSpan duration;

        FileStream? fileStream = null;
        ZipArchive? archive = null;
        Stream writeStream;

        try
        {
            fileStream = File.Create(actualPath);

            if (compress)
            {
                archive = new ZipArchive(fileStream, ZipArchiveMode.Create, leaveOpen: false);
                var entry = archive.CreateEntry("data.jsonl", CompressionLevel.Optimal);
                writeStream = entry.Open();
            }
            else
            {
                writeStream = fileStream;
            }

            await using (var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false))
            {
                // Header
                var header = new ExportHeader
                {
                    Provider = _provider.Name,
                    ExportedAt = startTime,
                    SchemeIds = schemeIds ?? []
                };
                await writer.WriteLineAsync(JsonSerializer.Serialize(header, JsonOptions));

                // Data in FK-safe order
                await ExportTypesAsync(writer, ct);
                await ExportListsAsync(writer, ct);
                await ExportSchemesAsync(writer, schemeIds, ct);
                await ExportStructuresAsync(writer, schemeIds, ct);
                await ExportRolesAsync(writer, ct);
                await ExportUsersAsync(writer, ct);
                await ExportUserRolesAsync(writer, ct);
                await ExportObjectsAsync(writer, schemeIds, ct);
                await ExportListItemsAsync(writer, ct);
                await ExportPermissionsAsync(writer, ct);
                await ExportValuesAsync(writer, schemeIds, ct);

                // Footer
                sequenceValue = await _provider.GetSequenceValueAsync(ct);
                Log($"Sequence value: {sequenceValue}");

                duration = DateTime.UtcNow - startTime;
                var footer = new ExportFooter
                {
                    SequenceValue = sequenceValue,
                    TotalTypes = _typesCount,
                    TotalRoles = _rolesCount,
                    TotalUsers = _usersCount,
                    TotalUserRoles = _userRolesCount,
                    TotalLists = _listsCount,
                    TotalListItems = _listItemsCount,
                    TotalSchemes = _schemesCount,
                    TotalStructures = _structuresCount,
                    TotalObjects = _objectsCount,
                    TotalPermissions = _permissionsCount,
                    TotalValues = _valuesCount,
                    Duration = duration
                };
                await writer.WriteLineAsync(JsonSerializer.Serialize(footer, JsonOptions));
                await writer.FlushAsync(ct);
            }

            archive?.Dispose();
            archive = null;
            fileStream = null;

            var checksum = await ComputeChecksumAsync(actualPath, ct);
            var fileSize = new FileInfo(actualPath).Length;

            Console.WriteLine();
            Console.WriteLine("Export completed successfully!");
            Console.WriteLine($"  File: {actualPath}");
            Console.WriteLine($"  Size: {FormatSize(fileSize)}{(compress ? " (compressed)" : "")}");
            Console.WriteLine($"  Duration: {duration:hh\\:mm\\:ss}");
            Console.WriteLine($"  Checksum: SHA256:{checksum[..16]}...");
            Console.WriteLine();
            Console.WriteLine("Statistics:");
            Console.WriteLine($"  Types:       {_typesCount:N0}");
            Console.WriteLine($"  Roles:       {_rolesCount:N0}");
            Console.WriteLine($"  Users:       {_usersCount:N0}");
            Console.WriteLine($"  UserRoles:   {_userRolesCount:N0}");
            Console.WriteLine($"  Lists:       {_listsCount:N0}");
            Console.WriteLine($"  ListItems:   {_listItemsCount:N0}");
            Console.WriteLine($"  Schemes:     {_schemesCount:N0}");
            Console.WriteLine($"  Structures:  {_structuresCount:N0}");
            Console.WriteLine($"  Objects:     {_objectsCount:N0}");
            Console.WriteLine($"  Permissions: {_permissionsCount:N0}");
            Console.WriteLine($"  Values:      {_valuesCount:N0}");
        }
        finally
        {
            archive?.Dispose();
            fileStream?.Dispose();
        }
    }

    private async Task PrintDryRunInfoAsync(long[]? schemeIds, CancellationToken ct)
    {
        Console.WriteLine("DRY RUN - No changes will be made");
        Console.WriteLine();

        var counts = await GetCountsAsync(schemeIds, ct);

        Console.WriteLine("Would export:");
        Console.WriteLine($"  Types:      {counts.Types:N0}");
        Console.WriteLine($"  Lists:      {counts.Lists:N0}");
        Console.WriteLine($"  ListItems:  {counts.ListItems:N0}");
        Console.WriteLine($"  Schemes:    {counts.Schemes:N0}");
        Console.WriteLine($"  Structures: {counts.Structures:N0}");
        Console.WriteLine($"  Objects:    {counts.Objects:N0}");
        Console.WriteLine($"  Values:     {counts.Values:N0}");

        if (schemeIds?.Length > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"Filtered by schemes: {string.Join(", ", schemeIds)}");
        }
    }

    private async Task<(long Types, long Lists, long ListItems, long Schemes, long Structures, long Objects, long Values)>
        GetCountsAsync(long[]? schemeIds, CancellationToken ct)
    {
        var conn = _provider.Connection;

        async Task<long> CountAsync(string sql)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            var result = await cmd.ExecuteScalarAsync(ct);
            return Convert.ToInt64(result);
        }

        var schemeFilter = schemeIds?.Length > 0
            ? $"WHERE _id_scheme IN ({string.Join(",", schemeIds)})"
            : "";
        var schemeFilter2 = schemeIds?.Length > 0
            ? $"WHERE _id IN ({string.Join(",", schemeIds)})"
            : "";

        return (
            await CountAsync("SELECT COUNT(*) FROM _types"),
            await CountAsync("SELECT COUNT(*) FROM _lists"),
            await CountAsync("SELECT COUNT(*) FROM _list_items"),
            await CountAsync($"SELECT COUNT(*) FROM _schemes {schemeFilter2}"),
            await CountAsync($"SELECT COUNT(*) FROM _structures {schemeFilter}"),
            await CountAsync($"SELECT COUNT(*) FROM _objects {schemeFilter}"),
            await CountAsync($"SELECT COUNT(*) FROM _values WHERE _id_object IN (SELECT _id FROM _objects {schemeFilter})")
        );
    }

    private async Task ExportTypesAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting types...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = "SELECT _id, _name, _db_type, _type FROM _types ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new TypeRecord
            {
                Id = reader.GetInt64(0),
                Name = reader.GetString(1),
                DbType = reader.IsDBNull(2) ? null : reader.GetString(2),
                DotnetType = reader.IsDBNull(3) ? null : reader.GetString(3)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _typesCount++;
        }

        LogCount("Types", _typesCount);
    }

    private async Task ExportUsersAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting users...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = @"
            SELECT _id, _login, _password, _name, _phone, _email,
                   _date_register, _date_dismiss, _enabled, _key,
                   _code_int, _code_string, _code_guid, _note, _hash, _id_configuration
            FROM _users ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new UserRecord
            {
                Id = reader.GetInt64(0),
                Login = reader.GetString(1),
                Password = reader.GetString(2),
                Name = reader.GetString(3),
                Phone = reader.IsDBNull(4) ? null : reader.GetString(4),
                Email = reader.IsDBNull(5) ? null : reader.GetString(5),
                DateRegister = reader.GetFieldValue<DateTimeOffset>(6),
                DateDismiss = reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                Enabled = reader.GetBoolean(8),
                Key = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                CodeInt = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                CodeString = reader.IsDBNull(11) ? null : reader.GetString(11),
                CodeGuid = reader.IsDBNull(12) ? null : reader.GetGuid(12),
                Note = reader.IsDBNull(13) ? null : reader.GetString(13),
                Hash = reader.IsDBNull(14) ? null : reader.GetGuid(14),
                IdConfiguration = reader.IsDBNull(15) ? null : reader.GetInt64(15)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _usersCount++;
        }

        LogCount("Users", _usersCount);
    }

    private async Task ExportRolesAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting roles...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = "SELECT _id, _name, _id_configuration FROM _roles ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new RoleRecord
            {
                Id = reader.GetInt64(0),
                Name = reader.GetString(1),
                IdConfiguration = reader.IsDBNull(2) ? null : reader.GetInt64(2)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _rolesCount++;
        }

        LogCount("Roles", _rolesCount);
    }

    private async Task ExportUserRolesAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting user-roles...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = "SELECT _id, _id_role, _id_user FROM _users_roles ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new UserRoleRecord
            {
                Id = reader.GetInt64(0),
                IdRole = reader.GetInt64(1),
                IdUser = reader.GetInt64(2)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _userRolesCount++;
        }

        LogCount("UserRoles", _userRolesCount);
    }

    private async Task ExportPermissionsAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting permissions...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = @"
            SELECT _id, _id_role, _id_user, _id_ref, _select, _insert, _update, _delete
            FROM _permissions ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new PermissionRecord
            {
                Id = reader.GetInt64(0),
                IdRole = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                IdUser = reader.IsDBNull(2) ? null : reader.GetInt64(2),
                IdRef = reader.GetInt64(3),
                Select = reader.IsDBNull(4) ? null : reader.GetBoolean(4),
                Insert = reader.IsDBNull(5) ? null : reader.GetBoolean(5),
                Update = reader.IsDBNull(6) ? null : reader.GetBoolean(6),
                Delete = reader.IsDBNull(7) ? null : reader.GetBoolean(7)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _permissionsCount++;
        }

        LogCount("Permissions", _permissionsCount);
    }

    private async Task ExportListsAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting lists...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = "SELECT _id, _name, _alias FROM _lists ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new ListRecord
            {
                Id = reader.GetInt64(0),
                Name = reader.GetString(1),
                Alias = reader.IsDBNull(2) ? null : reader.GetString(2)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _listsCount++;
        }

        LogCount("Lists", _listsCount);
    }

    private async Task ExportListItemsAsync(StreamWriter writer, CancellationToken ct)
    {
        Log("Exporting list items...");

        await using var cmd = _provider.Connection.CreateCommand();
        cmd.CommandText = "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new ListItemRecord
            {
                Id = reader.GetInt64(0),
                IdList = reader.GetInt64(1),
                Value = reader.IsDBNull(2) ? null : reader.GetString(2),
                Alias = reader.IsDBNull(3) ? null : reader.GetString(3),
                IdObject = reader.IsDBNull(4) ? null : reader.GetInt64(4)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _listItemsCount++;
        }

        LogCount("ListItems", _listItemsCount);
    }

    private async Task ExportSchemesAsync(StreamWriter writer, long[]? schemeIds, CancellationToken ct)
    {
        Log("Exporting schemes...");

        await using var cmd = _provider.Connection.CreateCommand();
        var filter = schemeIds?.Length > 0
            ? $"WHERE _id IN ({string.Join(",", schemeIds)})"
            : "";
        cmd.CommandText = $"SELECT _id, _id_parent, _name, _alias, _name_space, _structure_hash, _type FROM _schemes {filter} ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new SchemeRecord
            {
                Id = reader.GetInt64(0),
                IdParent = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                Name = reader.GetString(2),
                Alias = reader.IsDBNull(3) ? null : reader.GetString(3),
                NameSpace = reader.IsDBNull(4) ? null : reader.GetString(4),
                StructureHash = reader.IsDBNull(5) ? null : reader.GetGuid(5),
                SchemeType = reader.GetInt64(6)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _schemesCount++;
        }

        LogCount("Schemes", _schemesCount);
    }

    private async Task ExportStructuresAsync(StreamWriter writer, long[]? schemeIds, CancellationToken ct)
    {
        Log("Exporting structures...");

        await using var cmd = _provider.Connection.CreateCommand();
        var filter = schemeIds?.Length > 0
            ? $"WHERE _id_scheme IN ({string.Join(",", schemeIds)})"
            : "";
        cmd.CommandText = $@"
            SELECT _id, _id_parent, _id_scheme, _id_override, _id_type, _id_list,
                   _name, _alias, _order, _readonly, _allow_not_null,
                   _collection_type, _key_type, _is_compress, _store_null,
                   _default_value, _default_editor
            FROM _structures {filter} ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new StructureRecord
            {
                Id = reader.GetInt64(0),
                IdParent = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                IdScheme = reader.GetInt64(2),
                IdOverride = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                IdType = reader.GetInt64(4),
                IdList = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                Name = reader.GetString(6),
                Alias = reader.IsDBNull(7) ? null : reader.GetString(7),
                Order = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                Readonly = reader.IsDBNull(9) ? null : reader.GetBoolean(9),
                AllowNotNull = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                CollectionType = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                KeyType = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                IsCompress = reader.IsDBNull(13) ? null : reader.GetBoolean(13),
                StoreNull = reader.IsDBNull(14) ? null : reader.GetBoolean(14),
                DefaultValue = reader.IsDBNull(15) ? null : (byte[])reader.GetValue(15),
                DefaultEditor = reader.IsDBNull(16) ? null : reader.GetString(16)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _structuresCount++;
        }

        LogCount("Structures", _structuresCount);
    }

    private async Task ExportObjectsAsync(StreamWriter writer, long[]? schemeIds, CancellationToken ct)
    {
        Log("Exporting objects...");

        await using var cmd = _provider.Connection.CreateCommand();
        var filter = schemeIds?.Length > 0
            ? $"WHERE _id_scheme IN ({string.Join(",", schemeIds)})"
            : "";
        cmd.CommandText = $@"
            SELECT _id, _id_parent, _id_scheme, _id_owner, _id_who_change,
                   _date_create, _date_modify, _date_begin, _date_complete,
                   _key, _name, _note, _hash,
                   _value_long, _value_string, _value_guid, _value_bool,
                   _value_double, _value_numeric, _value_datetime, _value_bytes
            FROM _objects {filter} ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new ObjectRecord
            {
                Id = reader.GetInt64(0),
                IdParent = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                IdScheme = reader.GetInt64(2),
                IdOwner = reader.GetInt64(3),
                IdWhoChange = reader.GetInt64(4),
                DateCreate = reader.GetFieldValue<DateTimeOffset>(5),
                DateModify = reader.GetFieldValue<DateTimeOffset>(6),
                DateBegin = reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                DateComplete = reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
                Key = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                Name = reader.IsDBNull(10) ? null : reader.GetString(10),
                Note = reader.IsDBNull(11) ? null : reader.GetString(11),
                Hash = reader.IsDBNull(12) ? null : reader.GetGuid(12),
                ValueLong = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                ValueString = reader.IsDBNull(14) ? null : reader.GetString(14),
                ValueGuid = reader.IsDBNull(15) ? null : reader.GetGuid(15),
                ValueBool = reader.IsDBNull(16) ? null : reader.GetBoolean(16),
                ValueDouble = reader.IsDBNull(17) ? null : reader.GetDouble(17),
                ValueNumeric = reader.IsDBNull(18) ? null : reader.GetDecimal(18),
                ValueDatetime = reader.IsDBNull(19) ? null : reader.GetFieldValue<DateTimeOffset>(19),
                ValueBytes = reader.IsDBNull(20) ? null : (byte[])reader.GetValue(20)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _objectsCount++;

            if (_verbose && _objectsCount % 10000 == 0)
            {
                Console.Write($"\r  Objects: {_objectsCount:N0}");
            }
        }

        LogCount("Objects", _objectsCount);
    }

    private async Task ExportValuesAsync(StreamWriter writer, long[]? schemeIds, CancellationToken ct)
    {
        Log("Exporting values...");

        await using var cmd = _provider.Connection.CreateCommand();
        var filter = schemeIds?.Length > 0
            ? $"WHERE _id_object IN (SELECT _id FROM _objects WHERE _id_scheme IN ({string.Join(",", schemeIds)}))"
            : "";
        cmd.CommandText = $@"
            SELECT _id, _id_structure, _id_object,
                   _String, _Long, _Guid, _Double, _DateTimeOffset,
                   _Boolean, _ByteArray, _Numeric, _ListItem, _Object,
                   _array_parent_id, _array_index
            FROM _values {filter} ORDER BY _id";

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var record = new ValueRecord
            {
                Id = reader.GetInt64(0),
                IdStructure = reader.GetInt64(1),
                IdObject = reader.GetInt64(2),
                String = reader.IsDBNull(3) ? null : reader.GetString(3),
                Long = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                Guid = reader.IsDBNull(5) ? null : reader.GetGuid(5),
                Double = reader.IsDBNull(6) ? null : reader.GetDouble(6),
                DateTimeOffset = reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                Boolean = reader.IsDBNull(8) ? null : reader.GetBoolean(8),
                ByteArray = reader.IsDBNull(9) ? null : (byte[])reader.GetValue(9),
                Numeric = reader.IsDBNull(10) ? null : reader.GetDecimal(10),
                ListItem = reader.IsDBNull(11) ? null : reader.GetInt64(11),
                Object = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                ArrayParentId = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                ArrayIndex = reader.IsDBNull(14) ? null : reader.GetString(14)
            };
            await writer.WriteLineAsync(JsonSerializer.Serialize<ExportRecord>(record, JsonOptions));
            _valuesCount++;

            if (_verbose && _valuesCount % 50000 == 0)
            {
                Console.Write($"\r  Values: {_valuesCount:N0}");
            }
        }

        LogCount("Values", _valuesCount);
    }

    private static async Task<string> ComputeChecksumAsync(string filePath, CancellationToken ct)
    {
        await using var stream = File.OpenRead(filePath);
        var hash = await SHA256.HashDataAsync(stream, ct);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static string FormatSize(long bytes)
    {
        string[] sizes = ["B", "KB", "MB", "GB", "TB"];
        int order = 0;
        double size = bytes;
        while (size >= 1024 && order < sizes.Length - 1)
        {
            order++;
            size /= 1024;
        }
        return $"{size:0.##} {sizes[order]}";
    }

    private void Log(string message)
    {
        if (_verbose)
        {
            Console.WriteLine(message);
        }
    }

    private void LogCount(string entity, long count)
    {
        if (_verbose)
        {
            Console.WriteLine($"\r  {entity}: {count:N0}          ");
        }
    }
}
