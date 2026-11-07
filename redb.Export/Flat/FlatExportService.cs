using System.Reflection;
using redb.Core.Attributes;
using redb.Core.Models.Entities;
using redb.Export.Flat.Dialects;
using redb.Export.Flat.Model;
using redb.Export.Providers;

namespace redb.Export.Flat;

/// <summary>
/// Unfolds redb objects into ordinary relational tables and copies the data into them.
/// <para>
/// Source and target are two independent connections, of the same provider or of different ones:
/// the source is read with its own dialect inside one snapshot transaction, the target receives DDL
/// and bulk inserts in its own dialect inside one transaction. Either everything lands in the target
/// or nothing does. The transfer is one-way and expects a target without the flat tables.
/// </para>
/// </summary>
public sealed class FlatExportService
{
    // Objects of a scheme read by one statement.
    private const int ObjectsPerRead = 1000;

    // Largest key copied so far, in any table.
    private long _maxKey;

    private readonly IDataProvider _source;
    private readonly IDataProvider _target;
    private readonly bool _verbose;
    private readonly int _batchSize;

    /// <summary>
    /// Initializes a new instance of <see cref="FlatExportService"/>.
    /// </summary>
    /// <param name="source">An opened provider over the redb database to read.</param>
    /// <param name="target">An opened provider over the database that receives the flat tables.</param>
    /// <param name="verbose">When <c>true</c>, progress is written to <see cref="Console"/>.</param>
    /// <param name="batchSize">Rows buffered per table before a bulk insert.</param>
    public FlatExportService(IDataProvider source, IDataProvider target, bool verbose, int batchSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        _source = source;
        _target = target;
        _verbose = verbose;
        _batchSize = batchSize;
    }

    /// <summary>
    /// Name of the scheme a Props type is stored under: the explicit name of <see cref="RedbSchemeAttribute"/>
    /// when set, otherwise the full type name. Same rule as scheme synchronisation.
    /// </summary>
    public static string SchemeNameOf(Type propsType) =>
        propsType.GetCustomAttribute<RedbSchemeAttribute>()?.GetSchemeName(propsType)
        ?? propsType.FullName
        ?? propsType.Name;

    /// <summary>
    /// Exports the given Props types.
    /// </summary>
    /// <param name="propsTypes">
    /// Props types to unfold. A type referenced through <c>RedbObject&lt;T&gt;</c> must be in the set:
    /// the reference becomes a foreign key to its table.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    public async Task ExportAsync(IReadOnlyCollection<Type> propsTypes, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(propsTypes);

        var sourceDialect = FlatDialects.For(_source.Name);
        var targetDialect = FlatDialects.For(_target.Name);

        await using var snapshot = await _source.Connection.BeginTransactionAsync(sourceDialect.SnapshotIsolation, ct);
        var reader = new FlatSourceReader(_source, snapshot);

        // Bind every type to its scheme and structure tree before anything is written.
        var schemeIds = new Dictionary<Type, long>();
        var trees = new Dictionary<Type, IReadOnlyList<StructureTreeNode>>();
        foreach (var propsType in propsTypes)
        {
            var schemeName = SchemeNameOf(propsType);
            var schemeId = await reader.FindSchemeIdAsync(schemeName, ct)
                ?? throw new InvalidOperationException(
                    $"Scheme '{schemeName}' of type '{propsType.FullName}' is not in the source database.");

            schemeIds[propsType] = schemeId;
            trees[propsType] = StructureTreeBuilder.BuildFromFlat(await reader.ReadStructuresAsync(schemeId, ct));
        }

        var model = new FlatModelBuilder().Build(propsTypes, trees);
        Validate(model);

        var ddl = new FlatDdlGenerator(targetDialect);
        var buffers = model.Tables.ToDictionary(t => t.Name, t => new FlatTableBuffer(t, targetDialect));

        await _target.BeginImportAsync(ct);
        try
        {
            var createSequence = ddl.CreateSequence();
            if (createSequence != null)
                await _target.ExecuteAsync(createSequence, ct);

            Log($"Creating {model.Tables.Count} tables...");
            foreach (var table in model.Tables)
                await _target.ExecuteAsync(ddl.CreateTable(table), ct);

            await CopyServiceTableAsync(reader, "_objects", Single(model, FlatTableKind.Objects), buffers, targetDialect, ct);
            await CopyServiceTableAsync(reader, "_lists", Single(model, FlatTableKind.Lists), buffers, targetDialect, ct);
            await CopyServiceTableAsync(reader, "_list_items", Single(model, FlatTableKind.ListItems), buffers, targetDialect, ct);

            foreach (var propsType in propsTypes)
            {
                var schemeTables = model.Tables.Where(t => t.PropsType == propsType).ToList();
                await CopySchemeAsync(reader, schemeIds[propsType], schemeTables, buffers, targetDialect, ct);
            }

            // New rows written later, by EF or by hand, take their keys from the sequence: it has to
            // start above every key copied from redb.
            var restartSequence = ddl.RestartSequence(_maxKey + 1);
            if (restartSequence != null)
                await _target.ExecuteAsync(restartSequence, ct);

            Log("Adding foreign keys...");
            foreach (var statement in model.Tables.SelectMany(table => ddl.AddForeignKeys(table)))
                await _target.ExecuteAsync(statement, ct);

            Log("Creating foreign key indexes...");
            foreach (var statement in model.Tables.SelectMany(table => ddl.CreateIndexes(table)))
                await _target.ExecuteAsync(statement, ct);

            await _target.CommitImportAsync(ct);
        }
        catch
        {
            await _target.AbortImportAsync();
            throw;
        }

        Log("Flat export completed.");
    }

    private async Task CopyServiceTableAsync(
        FlatSourceReader reader,
        string sourceTable,
        FlatTable table,
        IReadOnlyDictionary<string, FlatTableBuffer> buffers,
        IFlatDialect dialect,
        CancellationToken ct)
    {
        Log($"Copying {sourceTable} -> {table.Name}...");

        var buffer = buffers[table.Name];
        long count = 0;

        await foreach (var values in reader.ReadServiceRowsAsync(sourceTable, table, ct))
        {
            // The first column of a service table is its key.
            if (values[0] is long key && key > _maxKey)
                _maxKey = key;

            var row = buffer.Data.NewRow();
            for (var i = 0; i < values.Length; i++)
                row[i] = values[i] == null ? DBNull.Value : dialect.ToDbValue(values[i]!, buffer.StorageTypes[i]);
            buffer.Data.Rows.Add(row);
            count++;

            if (buffer.Data.Rows.Count >= _batchSize)
                await FlushAsync(buffer, dialect, ct);
        }

        await FlushAsync(buffer, dialect, ct);
        LogCount(table.Name, count);
    }

    private async Task CopySchemeAsync(
        FlatSourceReader reader,
        long schemeId,
        IReadOnlyList<FlatTable> schemeTables,
        IReadOnlyDictionary<string, FlatTableBuffer> buffers,
        IFlatDialect dialect,
        CancellationToken ct)
    {
        var schemeTable = schemeTables.Single(t => t.Kind == FlatTableKind.Scheme);
        Log($"Unfolding {schemeTable.FullName} ({schemeTables.Count} tables)...");

        var assembler = new FlatRowAssembler(schemeTables, buffers, dialect);
        var objectIds = await reader.ReadSchemeObjectIdsAsync(schemeId, ct);
        var started = DateTime.UtcNow;
        long count = 0;

        // The scheme is read in slices of ObjectsPerRead objects, so the first rows arrive at once
        // and progress is visible from the start.
        for (var offset = 0; offset < objectIds.Count; offset += ObjectsPerRead)
        {
            var fromId = objectIds[offset];
            var toId = objectIds[Math.Min(offset + ObjectsPerRead, objectIds.Count) - 1];

            await foreach (var (objectId, values) in reader.ReadSchemeObjectsAsync(schemeId, fromId, toId, ct))
            {
                assembler.Append(schemeTable, objectId, values);
                count++;

                // Owners are flushed before the tables they own: the list is in creation order.
                if (schemeTables.Any(t => buffers[t.Name].Data.Rows.Count >= _batchSize))
                {
                    foreach (var table in schemeTables)
                        await FlushAsync(buffers[table.Name], dialect, ct);
                }
            }

            if (_verbose)
                Console.Write($"\r  {schemeTable.FullName}: {count:N0} / {objectIds.Count:N0}, {DateTime.UtcNow - started:hh\\:mm\\:ss}");
        }

        foreach (var table in schemeTables)
            await FlushAsync(buffers[table.Name], dialect, ct);

        _maxKey = Math.Max(_maxKey, assembler.MaxRowId);
        LogCount(schemeTable.FullName, count);
    }

    private async Task FlushAsync(FlatTableBuffer buffer, IFlatDialect dialect, CancellationToken ct)
    {
        if (buffer.Data.Rows.Count == 0)
            return;

        await _target.BulkInsertAsync(dialect.Quote(buffer.Table.Name), buffer.Data, ct);
        buffer.Data.Clear();
    }

    /// <summary>
    /// Refuses a model that cannot be filled: a field with no structure in the source (the scheme
    /// was not synchronised with this version of the class) or a reference to a Props type that is
    /// not exported.
    /// </summary>
    private static void Validate(FlatDatabaseModel model)
    {
        var tableNames = model.Tables.Select(t => t.Name).ToHashSet();

        foreach (var table in model.Tables)
        {
            foreach (var foreignKey in table.ForeignKeys)
            {
                if (!tableNames.Contains(foreignKey.TargetTable))
                    throw new InvalidOperationException(
                        $"Column '{foreignKey.Column}' of table '{table.FullName}' references " +
                        $"'{foreignKey.TargetPropsType?.FullName ?? foreignKey.TargetTable}', which is not in the " +
                        "export set. Add that Props type to the exported types.");
            }

            if (table.PropsType == null)
                continue;

            if (table.Kind != FlatTableKind.Scheme && table.StructureId == null)
                throw MissingStructure(table, table.PropertyPath!);

            foreach (var column in table.Columns)
            {
                if (column.ValueSource != FlatValueSource.None && column.StructureId == null)
                    throw MissingStructure(table, table.PropertyPath == null ? column.Name : $"{table.PropertyPath}.{column.Name}");
            }
        }
    }

    private static InvalidOperationException MissingStructure(FlatTable table, string path) => new(
        $"Property '{table.PropsType!.Name}.{path}' has no structure in the source database. " +
        "Synchronise the scheme with this version of the class before exporting.");

    private static FlatTable Single(FlatDatabaseModel model, FlatTableKind kind) =>
        model.Tables.Single(t => t.Kind == kind);

    private void Log(string message)
    {
        if (_verbose)
            Console.WriteLine(message);
    }

    private void LogCount(string entity, long count)
    {
        if (_verbose)
            // Padded so that it wipes the longer progress line it overwrites.
            Console.WriteLine($"\r  {entity}: {count:N0}".PadRight(79));
    }
}
