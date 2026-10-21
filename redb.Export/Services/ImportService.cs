using System.Data;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using redb.Export.Models;
using redb.Export.Providers;

namespace redb.Export.Services;

/// <summary>
/// Imports data from a <c>.redb</c> file into a REDB database.
/// <para>
/// Uses a streaming approach: records are read line-by-line, accumulated into
/// <see cref="DataTable"/> batches, and bulk-inserted via <see cref="IDataProvider"/>.
/// The file may be plain JSONL or ZIP-compressed (auto-detected).
/// </para>
/// </summary>
public sealed class ImportService
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
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    private readonly int _parallelism;
    private ParallelImportWriter? _writer;
    private CountingStream? _progress;
    private long _totalBytes;
    private DateTime _loadStarted;

    /// <summary>
    /// Initializes a new instance of <see cref="ImportService"/>.
    /// </summary>
    /// <param name="provider">Connection the import runs on.</param>
    /// <param name="verbose">Print progress.</param>
    /// <param name="batchSize">Rows per bulk insert.</param>
    /// <param name="parallelism">
    /// 1 (the default) keeps the whole import in one transaction on one connection. More than 1 writes
    /// over that many connections and rebuilds the secondary indexes of the large tables afterwards;
    /// see <see cref="IParallelImportProvider"/> for what is given up in exchange.
    /// </param>
    public ImportService(IDataProvider provider, bool verbose, int batchSize, int parallelism = 1)
    {
        if (parallelism < 1)
            throw new ArgumentOutOfRangeException(nameof(parallelism), "Parallelism must be at least 1.");
        if (parallelism > 1 && provider is not IParallelImportProvider)
            throw new NotSupportedException($"Parallel import is not available for the '{provider.Name}' provider.");

        _provider = provider;
        _verbose = verbose;
        _batchSize = batchSize;
        _parallelism = parallelism;
    }

    /// <summary>
    /// Imports data from the specified <c>.redb</c> file.
    /// </summary>
    /// <param name="inputPath">Path to the <c>.redb</c> file (JSONL or ZIP).</param>
    /// <param name="clean">
    /// When <c>true</c>, all existing data is truncated before import
    /// via <see cref="IDataProvider.CleanDatabaseAsync"/>.
    /// </param>
    /// <param name="dryRun">When <c>true</c>, only prints statistics without writing to the database.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task ImportAsync(
        string inputPath,
        bool clean,
        bool dryRun,
        CancellationToken ct = default)
    {
        var startTime = DateTime.UtcNow;

        Log($"Importing from: {inputPath}");

        var isZip = await IsZipFileAsync(inputPath);
        Log($"Detected: {(isZip ? "ZIP compressed" : "Plain JSONL")}");

        Stream dataStream;
        ZipArchive? archive = null;

        if (isZip)
        {
            archive = ZipFile.OpenRead(inputPath);
            var entry = archive.Entries.FirstOrDefault(e => e.Name.EndsWith(".jsonl"))
                ?? throw new InvalidOperationException("No .jsonl file found in archive");
            dataStream = entry.Open();
            _totalBytes = entry.Length;
        }
        else
        {
            dataStream = File.OpenRead(inputPath);
            _totalBytes = new FileInfo(inputPath).Length;
        }

        // Progress and the time estimate are told from how far into the file the reader is.
        _progress = new CountingStream(dataStream);
        dataStream = _progress;

        try
        {
            using var reader = new StreamReader(dataStream, Encoding.UTF8);

            var headerLine = await reader.ReadLineAsync(ct)
                ?? throw new InvalidOperationException("Empty file");

            var headerDoc = JsonDocument.Parse(headerLine);
            var provider = headerDoc.RootElement.TryGetProperty("provider", out var p) ? p.GetString() : "unknown";
            var exportedAt = headerDoc.RootElement.TryGetProperty("exported_at", out var e)
                ? e.GetDateTime() : DateTime.MinValue;

            Log($"Source: {provider}");
            Log($"Exported: {exportedAt:yyyy-MM-dd HH:mm:ss} UTC");
            Console.WriteLine();

            if (dryRun)
            {
                await PrintDryRunInfoAsync(reader, ct);
                return;
            }

            // One transaction for the whole import, cleaning included: a run that stops half-way - a broken or
            // truncated file, a cancellation, a server error - leaves the database exactly as it found it, with
            // its constraints on. Before, rows went in batch by batch as read, the constraints came back only on
            // success, and an import with cleaning that failed left an emptied, half-filled database with its
            // integrity switched off (review 2026-09-24).
            await _provider.BeginImportAsync(ct);
            var committed = false;
            var parallelFinished = false;
            try
            {
                if (clean)
                {
                    Log("Cleaning database...");
                    await _provider.CleanDatabaseAsync(ct);
                    Log("Database cleaned.");
                }

                IReadOnlyList<DroppedIndex> droppedIndexes = [];
                if (_parallelism > 1)
                {
                    // From here on several connections write and commit on their own, so the cleaning is
                    // committed first: there is no single transaction to roll back any more.
                    var parallel = (IParallelImportProvider)_provider;
                    await _provider.CommitImportAsync(ct);
                    committed = true;

                    droppedIndexes = await parallel.DropLoadIndexesAsync(ct);
                    Log($"Took {droppedIndexes.Count} secondary indexes out for the load; they are rebuilt at the end.");

                    await parallel.RelaxConstraintsForLoadAsync(ct);

                    _writer = await ParallelImportWriter.StartAsync(parallel, _parallelism, ct);
                    Log($"Writing over {_parallelism} connections.");
                }
                else
                {
                    await _provider.DisableConstraintsAsync(ct);
                }

                _loadStarted = DateTime.UtcNow;
                var tables = CreateDataTables();
                long sequenceValue = 0;
                ExportFooter? footer = null;

                Log("Importing data (streaming)...");

                string? line;
                while ((line = await reader.ReadLineAsync(ct)) != null)
                {
                    var doc = JsonDocument.Parse(line);

                    if (!doc.RootElement.TryGetProperty("type", out var typeProp))
                    {
                        if (doc.RootElement.TryGetProperty("sequence_value", out var seqProp))
                        {
                            sequenceValue = seqProp.GetInt64();
                        }
                        continue;
                    }

                    var type = typeProp.GetString();

                    if (_currentRecordType != null && _currentRecordType != type)
                    {
                        await FlushCurrentTableAsync(tables, ct);
                    }
                    _currentRecordType = type;

                    switch (type)
                    {
                        case "type":
                            AddTypeRow(tables.Types, JsonSerializer.Deserialize<TypeRecord>(line, JsonOptions)!);
                            tables.TypesTotal++;
                            if (tables.Types.Rows.Count >= _batchSize)
                                await FlushTableAsync("_types", tables.Types, ct);
                            break;

                        case "list":
                            AddListRow(tables.Lists, JsonSerializer.Deserialize<ListRecord>(line, JsonOptions)!);
                            tables.ListsTotal++;
                            if (tables.Lists.Rows.Count >= _batchSize)
                                await FlushTableAsync("_lists", tables.Lists, ct);
                            break;

                        case "scheme":
                            AddSchemeRow(tables.Schemes, JsonSerializer.Deserialize<SchemeRecord>(line, JsonOptions)!);
                            tables.SchemesTotal++;
                            if (tables.Schemes.Rows.Count >= _batchSize)
                                await FlushTableAsync("_schemes", tables.Schemes, ct);
                            break;

                        case "structure":
                            AddStructureRow(tables.Structures, JsonSerializer.Deserialize<StructureRecord>(line, JsonOptions)!);
                            tables.StructuresTotal++;
                            if (tables.Structures.Rows.Count >= _batchSize)
                                await FlushTableAsync("_structures", tables.Structures, ct);
                            break;

                        case "role":
                            AddRoleRow(tables.Roles, JsonSerializer.Deserialize<RoleRecord>(line, JsonOptions)!);
                            tables.RolesTotal++;
                            if (tables.Roles.Rows.Count >= _batchSize)
                                await FlushTableAsync("_roles", tables.Roles, ct);
                            break;

                        case "user":
                            AddUserRow(tables.Users, JsonSerializer.Deserialize<UserRecord>(line, JsonOptions)!);
                            tables.UsersTotal++;
                            if (tables.Users.Rows.Count >= _batchSize)
                                await FlushTableAsync("_users", tables.Users, ct);
                            break;

                        case "user_role":
                            AddUserRoleRow(tables.UserRoles, JsonSerializer.Deserialize<UserRoleRecord>(line, JsonOptions)!);
                            tables.UserRolesTotal++;
                            if (tables.UserRoles.Rows.Count >= _batchSize)
                                await FlushTableAsync("_users_roles", tables.UserRoles, ct);
                            break;

                        case "object":
                            AddObjectRow(tables.Objects, JsonSerializer.Deserialize<ObjectRecord>(line, JsonOptions)!);
                            _objectsCount++;
                            if (tables.Objects.Rows.Count >= _batchSize)
                                await FlushTableAsync("_objects", tables.Objects, ct);
                            break;

                        case "list_item":
                            AddListItemRow(tables.ListItems, JsonSerializer.Deserialize<ListItemRecord>(line, JsonOptions)!);
                            tables.ListItemsTotal++;
                            if (tables.ListItems.Rows.Count >= _batchSize)
                                await FlushTableAsync("_list_items", tables.ListItems, ct);
                            break;

                        case "permission":
                            AddPermissionRow(tables.Permissions, JsonSerializer.Deserialize<PermissionRecord>(line, JsonOptions)!);
                            tables.PermissionsTotal++;
                            if (tables.Permissions.Rows.Count >= _batchSize)
                                await FlushTableAsync("_permissions", tables.Permissions, ct);
                            break;

                        case "value":
                            AddValueRow(tables.Values, JsonSerializer.Deserialize<ValueRecord>(line, JsonOptions)!);
                            _valuesCount++;
                            if (tables.Values.Rows.Count >= _batchSize)
                                await FlushTableAsync("_values", tables.Values, ct);
                            break;

                        case "footer":
                            footer = JsonSerializer.Deserialize<ExportFooter>(line, JsonOptions);
                            if (footer?.SequenceValue > 0)
                            {
                                sequenceValue = footer.SequenceValue;
                            }
                            break;
                    }
                }

                await FlushCurrentTableAsync(tables, ct);

                if (_verbose && _lastTable != null)
                {
                    _tableRowCounts.TryGetValue(_lastTable, out var lastCount);
                    WriteProgressLine($"  {_lastTable}: {lastCount:N0} rows", final: true);
                }

                if (_writer != null)
                    Console.WriteLine($"Data read in {(DateTime.UtcNow - _loadStarted):hh\\:mm\\:ss}; waiting for the writers, then the indexes are rebuilt.");

                // The footer is the last line of every export: without it the file was cut short, and with it every
                // table must have arrived in full. Checked before the commit, so a short file changes nothing.
                if (_writer != null)
                {
                    await _writer.CompleteAsync();
                    try
                    {
                        VerifyAgainstFooter(footer, tables);
                    }
                    catch (InvalidOperationException ex)
                    {
                        // The serial import says "nothing was imported" here; a parallel one has already written.
                        throw new InvalidOperationException(
                            ex.Message.Replace(" Nothing was imported; the database is unchanged.", "") +
                            " The parallel import has already written its rows: run it again with --clean.", ex);
                    }
                }
                else
                {
                    VerifyAgainstFooter(footer, tables);
                    await _provider.EnableConstraintsAsync(ct);
                }

                if (sequenceValue > 0)
                {
                    Log($"Setting sequence to: {sequenceValue}");
                    await _provider.SetSequenceValueAsync(sequenceValue, ct);
                }

                if (_writer != null)
                {
                    await RebuildIndexesAsync(droppedIndexes, ct);

                    // After the indexes: checking foreign keys over the loaded rows wants them in place.
                    var constraintsStarted = DateTime.UtcNow;
                    await ((IParallelImportProvider)_provider).RestoreConstraintsAfterLoadAsync(ct);
                    Log($"Constraints restored in {(DateTime.UtcNow - constraintsStarted):hh\\:mm\\:ss}.");
                    parallelFinished = true;
                }
                else
                {
                    await _provider.CommitImportAsync(ct);
                    committed = true;
                }

                _typesCount = tables.TypesTotal;
                _listsCount = tables.ListsTotal;
                _schemesCount = tables.SchemesTotal;
                _structuresCount = tables.StructuresTotal;
                _rolesCount = tables.RolesTotal;
                _usersCount = tables.UsersTotal;
                _userRolesCount = tables.UserRolesTotal;
                _listItemsCount = tables.ListItemsTotal;
                _permissionsCount = tables.PermissionsTotal;
            }
            finally
            {
                if (!committed)
                    await _provider.AbortImportAsync();

                if (_writer != null)
                {
                    await _writer.DisposeAsync();
                    _writer = null;
                    if (!parallelFinished)
                    {
                        Console.WriteLine();
                        Console.WriteLine("The parallel import did not finish. The database holds part of the data, the secondary indexes");
                        Console.WriteLine("of the large tables are out (dropped with their definitions kept in _import_dropped_indexes, or");
                        Console.WriteLine("disabled, depending on the database), and foreign key checks may still be off.");
                        Console.WriteLine("Run the import again with --clean and --parallel: it reloads the data and puts all of that back.");
                    }
                }
            }

            var duration = DateTime.UtcNow - startTime;

            Console.WriteLine();
            Console.WriteLine("Import completed successfully!");
            Console.WriteLine($"  Duration: {duration:hh\\:mm\\:ss}");
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
            await dataStream.DisposeAsync();
            archive?.Dispose();
        }
    }

    /// <summary>
    /// Refuses an import whose file ended before its footer, or whose tables did not all arrive in full. The
    /// counts in the footer are written by the exporter as it goes; a mismatch means a cut or damaged file.
    /// </summary>
    private void VerifyAgainstFooter(ExportFooter? footer, ImportTables tables)
    {
        if (footer is null)
            throw new InvalidOperationException(
                "The file ends before its footer: it was cut short, or it is not a complete redb export. " +
                "Nothing was imported; the database is unchanged.");

        var mismatches = new List<string>();
        void Check(string table, long expected, long actual)
        {
            if (expected != actual)
                mismatches.Add($"{table}: expected {expected:N0}, read {actual:N0}");
        }

        Check("types", footer.TotalTypes, tables.TypesTotal);
        Check("roles", footer.TotalRoles, tables.RolesTotal);
        Check("users", footer.TotalUsers, tables.UsersTotal);
        Check("user roles", footer.TotalUserRoles, tables.UserRolesTotal);
        Check("lists", footer.TotalLists, tables.ListsTotal);
        Check("list items", footer.TotalListItems, tables.ListItemsTotal);
        Check("schemes", footer.TotalSchemes, tables.SchemesTotal);
        Check("structures", footer.TotalStructures, tables.StructuresTotal);
        Check("objects", footer.TotalObjects, _objectsCount);
        Check("permissions", footer.TotalPermissions, tables.PermissionsTotal);
        Check("values", footer.TotalValues, _valuesCount);

        if (mismatches.Count > 0)
            throw new InvalidOperationException(
                "The file does not hold what its footer lists (" + string.Join("; ", mismatches) + "). " +
                "Nothing was imported; the database is unchanged.");
    }

    private readonly Dictionary<string, long> _tableRowCounts = new();
    private string? _lastTable;
    private string? _currentRecordType;

    private async Task FlushTableAsync(string tableName, DataTable table, CancellationToken ct)
    {
        if (table.Rows.Count == 0) return;

        _tableRowCounts.TryGetValue(tableName, out var currentCount);
        currentCount += table.Rows.Count;
        _tableRowCounts[tableName] = currentCount;

        if (_verbose)
        {
            if (_lastTable != null && _lastTable != tableName)
            {
                _tableRowCounts.TryGetValue(_lastTable, out var prevCount);
                WriteProgressLine($"  {_lastTable}: {prevCount:N0} rows", final: true);
            }
            _lastTable = tableName;
            WriteProgressLine($"  {tableName}: {currentCount:N0} rows... {ProgressText()}", final: false);
        }

        if (_writer != null)
        {
            // The batch is handed over as a copy: the reader keeps filling this table while a worker sends that one.
            await _writer.EnqueueAsync(tableName, table.Copy());
        }
        else
        {
            await _provider.BulkInsertAsync(tableName, table, ct);
        }
        table.Clear();
    }

    private async Task FlushCurrentTableAsync(ImportTables tables, CancellationToken ct)
    {
        switch (_currentRecordType)
        {
            case "type": await FlushTableAsync("_types", tables.Types, ct); break;
            case "list": await FlushTableAsync("_lists", tables.Lists, ct); break;
            case "scheme": await FlushTableAsync("_schemes", tables.Schemes, ct); break;
            case "structure": await FlushTableAsync("_structures", tables.Structures, ct); break;
            case "role": await FlushTableAsync("_roles", tables.Roles, ct); break;
            case "user": await FlushTableAsync("_users", tables.Users, ct); break;
            case "user_role": await FlushTableAsync("_users_roles", tables.UserRoles, ct); break;
            case "object": await FlushTableAsync("_objects", tables.Objects, ct); break;
            case "list_item": await FlushTableAsync("_list_items", tables.ListItems, ct); break;
            case "permission": await FlushTableAsync("_permissions", tables.Permissions, ct); break;
            case "value": await FlushTableAsync("_values", tables.Values, ct); break;
        }
    }

    private async Task PrintDryRunInfoAsync(StreamReader reader, CancellationToken ct)
    {
        Console.WriteLine("DRY RUN - No changes will be made");
        Console.WriteLine();

        long types = 0, lists = 0, schemes = 0, structures = 0;
        long roles = 0, users = 0, userRoles = 0;
        long objects = 0, listItems = 0, permissions = 0, values = 0;

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) != null)
        {
            var doc = JsonDocument.Parse(line);
            if (!doc.RootElement.TryGetProperty("type", out var typeProp)) continue;

            switch (typeProp.GetString())
            {
                case "type": types++; break;
                case "list": lists++; break;
                case "scheme": schemes++; break;
                case "structure": structures++; break;
                case "role": roles++; break;
                case "user": users++; break;
                case "user_role": userRoles++; break;
                case "object": objects++; break;
                case "list_item": listItems++; break;
                case "permission": permissions++; break;
                case "value": values++; break;
            }
        }

        Console.WriteLine("Would import:");
        Console.WriteLine($"  Types:       {types:N0}");
        Console.WriteLine($"  Roles:       {roles:N0}");
        Console.WriteLine($"  Users:       {users:N0}");
        Console.WriteLine($"  UserRoles:   {userRoles:N0}");
        Console.WriteLine($"  Lists:       {lists:N0}");
        Console.WriteLine($"  ListItems:   {listItems:N0}");
        Console.WriteLine($"  Schemes:     {schemes:N0}");
        Console.WriteLine($"  Structures:  {structures:N0}");
        Console.WriteLine($"  Objects:     {objects:N0}");
        Console.WriteLine($"  Permissions: {permissions:N0}");
        Console.WriteLine($"  Values:      {values:N0}");
    }

    /// <summary>
    /// Index builds running at the same time, whatever the number of writer connections. Each build
    /// sorts the whole table; more of them at once compete for the same disk and memory and finish
    /// no sooner.
    /// </summary>
    private const int MaxConcurrentIndexBuilds = 5;

    private int _progressLineLength;

    /// <summary>
    /// Writes the progress line over the previous one and blanks whatever the previous one left
    /// beyond it. A final line ends with a newline; a running one is overwritten by the next.
    /// </summary>
    private void WriteProgressLine(string text, bool final)
    {
        var padding = Math.Max(0, _progressLineLength - text.Length);
        Console.Write("\r" + text + new string(' ', padding));
        if (final)
        {
            Console.WriteLine();
            _progressLineLength = 0;
        }
        else
        {
            _progressLineLength = text.Length;
        }
    }

    /// <summary>
    /// How far into the file the reader is and how long the rest should take, from the bytes read so
    /// far. It measures reading; in a parallel import the index rebuild comes on top and is timed
    /// separately.
    /// </summary>
    private string ProgressText()
    {
        if (_progress == null || _totalBytes <= 0) return "";

        var fraction = Math.Clamp((double)_progress.BytesRead / _totalBytes, 0d, 1d);
        var elapsed = DateTime.UtcNow - _loadStarted;
        if (fraction < 0.005 || elapsed.TotalSeconds < 2)
            return $"data {fraction:P0}";

        var remaining = TimeSpan.FromSeconds(elapsed.TotalSeconds * (1 - fraction) / fraction);
        var indexes = _parallelism > 1 ? "; indexes follow" : "";
        return $"data {fraction:P0}, {elapsed:hh\\:mm\\:ss} elapsed, about {remaining:hh\\:mm\\:ss} left{indexes}";
    }

    /// <summary>
    /// Builds the indexes again, several at a time: one statement per connection. Where the
    /// database lets index builds on one table run side by side, any free connection takes the next
    /// index; where it does not, each table gets one connection and its indexes go in turn.
    /// </summary>
    private async Task RebuildIndexesAsync(IReadOnlyList<DroppedIndex> indexes, CancellationToken ct)
    {
        var parallel = (IParallelImportProvider)_provider;
        if (indexes.Count == 0)
        {
            await parallel.ForgetDroppedIndexesAsync(ct);
            return;
        }

        Console.WriteLine();
        // One queue for everything, or one queue per table.
        List<System.Collections.Concurrent.ConcurrentQueue<DroppedIndex>> queues = parallel.BuildsIndexesOfOneTableConcurrently
            ? [new System.Collections.Concurrent.ConcurrentQueue<DroppedIndex>(indexes)]
            : indexes.GroupBy(i => i.Table)
                .Select(g => new System.Collections.Concurrent.ConcurrentQueue<DroppedIndex>(g))
                .ToList();

        var connections = _writer!.Connections.Take(MaxConcurrentIndexBuilds).ToList();
        var builders = parallel.BuildsIndexesOfOneTableConcurrently
            ? connections.Select(c => (Connection: c, Queue: queues[0])).ToList()
            : connections.Zip(queues, (c, q) => (Connection: c, Queue: q)).ToList();
        if (builders.Count < queues.Count)
            throw new InvalidOperationException(
                $"{queues.Count} tables have indexes to rebuild and only {builders.Count} connections are open; raise --parallel.");

        Console.WriteLine($"Rebuilding {indexes.Count} indexes, {builders.Count} at a time...");

        var started = DateTime.UtcNow;
        var done = 0;

        async Task BuildAsync((IDataProvider Connection, System.Collections.Concurrent.ConcurrentQueue<DroppedIndex> Queue) work)
        {
            var builder = (IParallelImportProvider)work.Connection;
            var pending = work.Queue;
            while (pending.TryDequeue(out var index))
            {
                var indexStarted = DateTime.UtcNow;
                await builder.CreateIndexAsync(index, ct);
                var finished = Interlocked.Increment(ref done);
                Console.WriteLine(
                    $"  [{finished}/{indexes.Count}] {index.Table}.{index.Name}  {(DateTime.UtcNow - indexStarted):hh\\:mm\\:ss}");
            }
        }

        await Task.WhenAll(builders.Select(BuildAsync));
        await parallel.ForgetDroppedIndexesAsync(ct);

        Console.WriteLine($"Indexes rebuilt in {(DateTime.UtcNow - started):hh\\:mm\\:ss}.");
    }

    private void Log(string message)

    {
        if (_verbose) Console.WriteLine(message);
    }

    #region DataTable Creation and Row Mapping

    private sealed class ImportTables
    {
        public DataTable Types { get; } = CreateTypesTable();
        public DataTable Lists { get; } = CreateListsTable();
        public DataTable Schemes { get; } = CreateSchemesTable();
        public DataTable Structures { get; } = CreateStructuresTable();
        public DataTable Roles { get; } = CreateRolesTable();
        public DataTable Users { get; } = CreateUsersTable();
        public DataTable UserRoles { get; } = CreateUserRolesTable();
        public DataTable Objects { get; } = CreateObjectsTable();
        public DataTable ListItems { get; } = CreateListItemsTable();
        public DataTable Permissions { get; } = CreatePermissionsTable();
        public DataTable Values { get; } = CreateValuesTable();

        public long TypesTotal { get; set; }
        public long ListsTotal { get; set; }
        public long SchemesTotal { get; set; }
        public long StructuresTotal { get; set; }
        public long RolesTotal { get; set; }
        public long UsersTotal { get; set; }
        public long UserRolesTotal { get; set; }
        public long ListItemsTotal { get; set; }
        public long PermissionsTotal { get; set; }
    }

    private static ImportTables CreateDataTables() => new();

    private static DataTable CreateTypesTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_db_type", typeof(string));
        dt.Columns.Add("_type", typeof(string));
        return dt;
    }

    private static void AddTypeRow(DataTable dt, TypeRecord r)
    {
        dt.Rows.Add(r.Id, r.Name, r.DbType ?? (object)DBNull.Value, r.DotnetType ?? (object)DBNull.Value);
    }

    private static DataTable CreateListsTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_alias", typeof(string));
        return dt;
    }

    private static void AddListRow(DataTable dt, ListRecord r)
    {
        dt.Rows.Add(r.Id, r.Name, r.Alias ?? (object)DBNull.Value);
    }

    private static DataTable CreateSchemesTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_parent", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_alias", typeof(string));
        dt.Columns.Add("_name_space", typeof(string));
        // uuid-semantic: object, not Guid - SQLite stores it as an RFC-ordered BLOB (GuidToDb).
        dt.Columns.Add("_structure_hash", typeof(object));
        dt.Columns.Add("_type", typeof(long));
        dt.Columns.Add("_tags", typeof(string));
        return dt;
    }

    private void AddSchemeRow(DataTable dt, SchemeRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.IdParent ?? (object)DBNull.Value,
            r.Name,
            r.Alias ?? (object)DBNull.Value,
            r.NameSpace ?? (object)DBNull.Value,
            GuidDb(r.StructureHash),
            r.SchemeType,
            r.Tags ?? (object)DBNull.Value
        );
    }

    private static DataTable CreateStructuresTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_parent", typeof(long));
        dt.Columns.Add("_id_scheme", typeof(long));
        dt.Columns.Add("_id_override", typeof(long));
        dt.Columns.Add("_id_type", typeof(long));
        dt.Columns.Add("_id_list", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_alias", typeof(string));
        dt.Columns.Add("_order", typeof(int));
        dt.Columns.Add("_readonly", typeof(bool));
        dt.Columns.Add("_allow_not_null", typeof(bool));
        dt.Columns.Add("_collection_type", typeof(long));
        dt.Columns.Add("_key_type", typeof(long));
        dt.Columns.Add("_is_compress", typeof(bool));
        dt.Columns.Add("_store_null", typeof(bool));
        dt.Columns.Add("_default_value", typeof(byte[]));
        dt.Columns.Add("_default_editor", typeof(string));
        dt.Columns.Add("_unique", typeof(bool));
        dt.Columns.Add("_unique_version", typeof(long));
        dt.Columns.Add("_lazy", typeof(bool));
        dt.Columns.Add("_unique_scope", typeof(long));
        dt.Columns.Add("_tags", typeof(string));
        return dt;
    }

    private static void AddStructureRow(DataTable dt, StructureRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.IdParent ?? (object)DBNull.Value,
            r.IdScheme,
            r.IdOverride ?? (object)DBNull.Value,
            r.IdType,
            r.IdList ?? (object)DBNull.Value,
            r.Name,
            r.Alias ?? (object)DBNull.Value,
            r.Order ?? (object)DBNull.Value,
            r.Readonly ?? (object)DBNull.Value,
            r.AllowNotNull ?? (object)DBNull.Value,
            r.CollectionType ?? (object)DBNull.Value,
            r.KeyType ?? (object)DBNull.Value,
            r.IsCompress ?? (object)DBNull.Value,
            r.StoreNull ?? (object)DBNull.Value,
            r.DefaultValue ?? (object)DBNull.Value,
            r.DefaultEditor ?? (object)DBNull.Value,
            r.Unique ?? (object)DBNull.Value,
            r.UniqueVersion ?? (object)DBNull.Value,
            r.Lazy ?? (object)DBNull.Value,
            r.UniqueScope ?? (object)DBNull.Value,
            r.Tags ?? (object)DBNull.Value
        );
    }

    private static DataTable CreateRolesTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_id_configuration", typeof(long));
        return dt;
    }

    private static void AddRoleRow(DataTable dt, RoleRecord r)
    {
        dt.Rows.Add(r.Id, r.Name, r.IdConfiguration ?? (object)DBNull.Value);
    }

    private static DataTable CreateUsersTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_login", typeof(string));
        dt.Columns.Add("_password", typeof(string));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_phone", typeof(string));
        dt.Columns.Add("_email", typeof(string));
        dt.Columns.Add("_date_register", typeof(DateTimeOffset));
        dt.Columns.Add("_date_dismiss", typeof(DateTimeOffset));
        dt.Columns.Add("_enabled", typeof(bool));
        dt.Columns.Add("_key", typeof(long));
        dt.Columns.Add("_code_int", typeof(long));
        dt.Columns.Add("_code_string", typeof(string));
        dt.Columns.Add("_code_guid", typeof(Guid));
        dt.Columns.Add("_note", typeof(string));
        // uuid-semantic: object, not Guid - SQLite stores it as an RFC-ordered BLOB (GuidToDb).
        dt.Columns.Add("_hash", typeof(object));
        dt.Columns.Add("_id_configuration", typeof(long));
        return dt;
    }

    private void AddUserRow(DataTable dt, UserRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.Login,
            r.Password,
            r.Name,
            r.Phone ?? (object)DBNull.Value,
            r.Email ?? (object)DBNull.Value,
            (object)r.DateRegister,
            r.DateDismiss.HasValue ? (object)r.DateDismiss.Value : DBNull.Value,
            (object)r.Enabled,
            r.Key ?? (object)DBNull.Value,
            r.CodeInt ?? (object)DBNull.Value,
            r.CodeString ?? (object)DBNull.Value,
            r.CodeGuid ?? (object)DBNull.Value,
            r.Note ?? (object)DBNull.Value,
            GuidDb(r.Hash),
            r.IdConfiguration ?? (object)DBNull.Value
        );
    }

    private static DataTable CreateUserRolesTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_role", typeof(long));
        dt.Columns.Add("_id_user", typeof(long));
        return dt;
    }

    private static void AddUserRoleRow(DataTable dt, UserRoleRecord r)
    {
        dt.Rows.Add(r.Id, r.IdRole, r.IdUser);
    }

    private static DataTable CreateObjectsTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_parent", typeof(long));
        dt.Columns.Add("_id_scheme", typeof(long));
        dt.Columns.Add("_id_owner", typeof(long));
        dt.Columns.Add("_id_who_change", typeof(long));
        dt.Columns.Add("_date_create", typeof(DateTimeOffset));
        dt.Columns.Add("_date_modify", typeof(DateTimeOffset));
        dt.Columns.Add("_date_begin", typeof(DateTimeOffset));
        dt.Columns.Add("_date_complete", typeof(DateTimeOffset));
        dt.Columns.Add("_key", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_note", typeof(string));
        // uuid-semantic: object, not Guid - SQLite stores it as an RFC-ordered BLOB (GuidToDb).
        dt.Columns.Add("_hash", typeof(object));
        dt.Columns.Add("_value_long", typeof(long));
        dt.Columns.Add("_value_string", typeof(string));
        dt.Columns.Add("_value_guid", typeof(Guid));
        dt.Columns.Add("_value_bool", typeof(bool));
        dt.Columns.Add("_value_double", typeof(double));
        dt.Columns.Add("_value_numeric", typeof(decimal));
        dt.Columns.Add("_value_datetime", typeof(DateTimeOffset));
        dt.Columns.Add("_value_bytes", typeof(byte[]));
        dt.Columns.Add("_value_unique", typeof(string));
        return dt;
    }

    private void AddObjectRow(DataTable dt, ObjectRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.IdParent ?? (object)DBNull.Value,
            r.IdScheme,
            (object)r.IdOwner,
            (object)r.IdWhoChange,
            (object)r.DateCreate,
            (object)r.DateModify,
            r.DateBegin.HasValue ? (object)r.DateBegin.Value : DBNull.Value,
            r.DateComplete.HasValue ? (object)r.DateComplete.Value : DBNull.Value,
            r.Key ?? (object)DBNull.Value,
            r.Name ?? (object)DBNull.Value,
            r.Note ?? (object)DBNull.Value,
            GuidDb(r.Hash),
            r.ValueLong ?? (object)DBNull.Value,
            r.ValueString ?? (object)DBNull.Value,
            r.ValueGuid ?? (object)DBNull.Value,
            r.ValueBool ?? (object)DBNull.Value,
            r.ValueDouble ?? (object)DBNull.Value,
            r.ValueNumeric ?? (object)DBNull.Value,
            r.ValueDatetime.HasValue ? (object)r.ValueDatetime.Value : DBNull.Value,
            r.ValueBytes ?? (object)DBNull.Value,
            r.ValueUnique ?? (object)DBNull.Value
        );
    }

    private static DataTable CreateListItemsTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_list", typeof(long));
        dt.Columns.Add("_value", typeof(string));
        dt.Columns.Add("_alias", typeof(string));
        dt.Columns.Add("_id_object", typeof(long));
        return dt;
    }

    private static void AddListItemRow(DataTable dt, ListItemRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.IdList,
            r.Value ?? (object)DBNull.Value,
            r.Alias ?? (object)DBNull.Value,
            r.IdObject ?? (object)DBNull.Value
        );
    }

    private static DataTable CreatePermissionsTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_role", typeof(long));
        dt.Columns.Add("_id_user", typeof(long));
        dt.Columns.Add("_id_ref", typeof(long));
        dt.Columns.Add("_select", typeof(bool));
        dt.Columns.Add("_insert", typeof(bool));
        dt.Columns.Add("_update", typeof(bool));
        dt.Columns.Add("_delete", typeof(bool));
        return dt;
    }

    private static void AddPermissionRow(DataTable dt, PermissionRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.IdRole ?? (object)DBNull.Value,
            r.IdUser ?? (object)DBNull.Value,
            (object)r.IdRef,
            r.Select ?? (object)DBNull.Value,
            r.Insert ?? (object)DBNull.Value,
            r.Update ?? (object)DBNull.Value,
            r.Delete ?? (object)DBNull.Value
        );
    }

    private static DataTable CreateValuesTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_structure", typeof(long));
        dt.Columns.Add("_id_object", typeof(long));
        dt.Columns.Add("_String", typeof(string));
        dt.Columns.Add("_Long", typeof(long));
        dt.Columns.Add("_Guid", typeof(Guid));
        dt.Columns.Add("_Double", typeof(double));
        dt.Columns.Add("_DateTimeOffset", typeof(DateTimeOffset));
        dt.Columns.Add("_Boolean", typeof(bool));
        dt.Columns.Add("_ByteArray", typeof(byte[]));
        dt.Columns.Add("_Numeric", typeof(decimal));
        dt.Columns.Add("_ListItem", typeof(long));
        dt.Columns.Add("_Object", typeof(long));
        dt.Columns.Add("_array_parent_id", typeof(long));
        dt.Columns.Add("_array_index", typeof(string));
        // uuid-semantic: object, not Guid - SQLite stores it as an RFC-ordered BLOB (GuidToDb).
        dt.Columns.Add("_unique", typeof(object));
        return dt;
    }

    private void AddValueRow(DataTable dt, ValueRecord r)
    {
        dt.Rows.Add(
            r.Id,
            r.IdStructure,
            r.IdObject,
            r.String ?? (object)DBNull.Value,
            r.Long ?? (object)DBNull.Value,
            r.Guid ?? (object)DBNull.Value,
            r.Double ?? (object)DBNull.Value,
            r.DateTimeOffset ?? (object)DBNull.Value,
            r.Boolean ?? (object)DBNull.Value,
            r.ByteArray ?? (object)DBNull.Value,
            r.Numeric ?? (object)DBNull.Value,
            r.ListItem ?? (object)DBNull.Value,
            r.Object ?? (object)DBNull.Value,
            r.ArrayParentId ?? (object)DBNull.Value,
            r.ArrayIndex ?? (object)DBNull.Value,
            GuidDb(r.Unique)
        );
    }

    /// <summary>
    /// Provider form of a uuid-semantic column value (<c>_hash</c>, <c>_structure_hash</c>,
    /// <c>_values._unique</c>): the Guid itself for PostgreSQL/MSSQL, an RFC 4122-ordered
    /// BLOB for SQLite. Genuine binary columns never go through this.
    /// </summary>
    private object GuidDb(Guid? value) => value is { } v ? _provider.GuidToDb(v) : DBNull.Value;

    #endregion

    private static async Task<bool> IsZipFileAsync(string filePath)
    {
        await using var stream = File.OpenRead(filePath);
        var buffer = new byte[4];
        var read = await stream.ReadAsync(buffer);

        // ZIP magic number: PK\x03\x04
        return read >= 4 && buffer[0] == 0x50 && buffer[1] == 0x4B &&
               buffer[2] == 0x03 && buffer[3] == 0x04;
    }
}
