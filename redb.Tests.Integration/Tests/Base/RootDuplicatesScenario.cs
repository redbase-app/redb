using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A database written before the schema lock: root fields of one scheme twice (RootStructureKey). Planted by hand on
/// the probe scheme, with the unique key of root structures dropped: an empty duplicate of a scalar, an empty duplicate
/// of a nested class with its child, and a duplicate of Count holding a value of an object next to the original's.
/// </summary>
public sealed class RootDuplicatesScenario
{
    public long TitleDuplicate { get; private set; }
    public long PartDuplicate { get; private set; }
    public long PartDuplicateChild { get; private set; }
    public long CountDuplicate { get; private set; }
    public long ObjectId { get; private set; }

    private const string SchemeName = "schema_lock_sync_probe";

    public static async Task<RootDuplicatesScenario> PlantAsync(IRedbService redb, IRedbContext context)
    {
        var scenario = new RootDuplicatesScenario();
        await context.ExecuteAsync($"DROP INDEX IF EXISTS \"{RootStructureKey.IndexName}\"");
        await redb.SyncSchemeAsync<SchemaLockSyncProbeProps>();
        scenario.ObjectId = await redb.SaveAsync(new RedbObject<SchemaLockSyncProbeProps>
        {
            name = "root-duplicates",
            Props = new SchemaLockSyncProbeProps { Title = "t", Count = 5, Part = new SchemaLockSyncProbePart { City = "c" } }
        });

        var scheme = await context.ExecuteScalarAsync<long>($"SELECT _id FROM _schemes WHERE _name = '{SchemeName}'");
        // Ids well above anything the sequence handed out in these test databases.
        var next = 9_000_000_000L + await context.ExecuteScalarAsync<long>("SELECT CAST(COUNT(*) AS BIGINT) FROM _structures") * 10;

        async Task<long> CopyAsync(string name, long? parentOfOriginal, long? newParent)
        {
            var id = next++;
            var originalParent = parentOfOriginal == null ? "_id_parent IS NULL" : $"_id_parent = {parentOfOriginal}";
            await context.ExecuteAsync(
                "INSERT INTO _structures (_id, _id_parent, _id_scheme, _id_type, _name, _order, _collection_type, _key_type, _allow_not_null) " +
                $"SELECT {id}, {(newParent?.ToString() ?? "NULL")}, _id_scheme, _id_type, _name, _order, _collection_type, _key_type, _allow_not_null " +
                $"FROM _structures WHERE _id_scheme = {scheme} AND _name = '{name}' AND {originalParent}");
            return id;
        }

        scenario.TitleDuplicate = await CopyAsync("Title", null, null);
        scenario.PartDuplicate = await CopyAsync("Part", null, null);
        var part = await context.ExecuteScalarAsync<long>(
            $"SELECT _id FROM _structures WHERE _id_scheme = {scheme} AND _name = 'Part' AND _id_parent IS NULL AND _id <> {scenario.PartDuplicate}");
        scenario.PartDuplicateChild = await CopyAsync("City", part, scenario.PartDuplicate);
        scenario.CountDuplicate = await CopyAsync("Count", null, null);

        var count = await context.ExecuteScalarAsync<long>(
            $"SELECT _id FROM _structures WHERE _id_scheme = {scheme} AND _name = 'Count' AND _id_parent IS NULL AND _id <> {scenario.CountDuplicate}");
        await context.ExecuteAsync(
            "INSERT INTO _values (_id, _id_structure, _id_object, _Long) " +
            $"SELECT {next++}, {scenario.CountDuplicate}, _id_object, _Long FROM _values WHERE _id_structure = {count} AND _id_object = {scenario.ObjectId}");
        return scenario;
    }

    /// <summary>One more copy of a root field of the probe scheme, empty.</summary>
    public async Task<long> CopyRootAsync(IRedbContext context, string name)
    {
        var id = 9_100_000_000L + await context.ExecuteScalarAsync<long>("SELECT CAST(COUNT(*) AS BIGINT) FROM _structures");
        await context.ExecuteAsync(
            "INSERT INTO _structures (_id, _id_parent, _id_scheme, _id_type, _name, _order, _collection_type, _key_type, _allow_not_null) " +
            $"SELECT {id}, NULL, s._id_scheme, s._id_type, s._name, s._order, s._collection_type, s._key_type, s._allow_not_null " +
            $"FROM _structures s JOIN _schemes sc ON sc._id = s._id_scheme " +
            $"WHERE sc._name = '{SchemeName}' AND s._name = '{name}' AND s._id_parent IS NULL AND s._id = " +
            $"(SELECT MIN(x._id) FROM _structures x WHERE x._id_scheme = s._id_scheme AND x._name = '{name}' AND x._id_parent IS NULL)");
        return id;
    }

    public static async Task<bool> StructureExistsAsync(IRedbContext context, long id)
        => await context.ExecuteScalarAsync<long>($"SELECT CAST(COUNT(*) AS BIGINT) FROM _structures WHERE _id = {id}") == 1;

    /// <summary>
    /// After a start over the planted database: Count, the one field whose copies both hold values, is listed by
    /// SchemaProblems with both ids and logged as an error once.
    /// </summary>
    public void ShouldReportCountOnly(IRedbService started, CollectingLoggerProvider logs)
    {
        var problem = started.SchemaProblems.Should().ContainSingle().Which;
        problem.Kind.Should().Be(redb.Core.Models.RedbSchemaProblemKind.DuplicateRootStructure);
        problem.SchemeName.Should().Be(SchemeName);
        problem.Field.Should().Be("Count");
        problem.StructureIds.Should().Contain(CountDuplicate).And.HaveCount(2);
        logs.Errors.Should().ContainSingle(e => e.Contains("'Count'") && e.Contains("Duplicate root structures"));
    }

    /// <summary>The hand fix of UPGRADING.md: the duplicate with the value goes, its value with it.</summary>
    public Task ResolveByHandAsync(IRedbContext context)
        => context.ExecuteAsync($"DELETE FROM _structures WHERE _id = {CountDuplicate}");
}

/// <summary>Keeps the warnings a service logs, for the tests that expect one.</summary>
public sealed class CollectingLoggerProvider : ILoggerProvider
{
    public List<string> Warnings { get; } = [];

    public List<string> Errors { get; } = [];

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(CollectingLoggerProvider owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel < LogLevel.Warning)
                return;
            lock (owner)
                (logLevel >= LogLevel.Error ? owner.Errors : owner.Warnings).Add(formatter(state, exception));
        }
    }
}
