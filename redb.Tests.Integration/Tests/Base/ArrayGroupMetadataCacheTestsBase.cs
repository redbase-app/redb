using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Pro GroupByArray reads the metadata of the array path and of the element fields once: a repeated call runs the data
/// statement alone, and its SQL preview runs nothing. The commands are counted through the redb.Core ActivitySource,
/// one span per database command (CommandCounter).
/// </summary>
public abstract class ArrayGroupMetadataCacheTestsBase
{
    protected readonly IRedbService Redb;

    protected ArrayGroupMetadataCacheTestsBase(IRedbService redb) => Redb = redb;

    private static Task<int> CommandsAsync(Func<Task> action) => CommandCounter.CountAsync(action);

    private static Task<List<Row>> GroupAsync(IRedbQueryable<EmployeeProps> source)
        => source.GroupByArray(e => e.Contacts!.SelectMany(c => c.Metadata!), m => m.Label)
            .SelectAsync(g => new Row { Label = g.Key, Count = Agg.Count(g), Values = Agg.Count(g, m => m.Value == "d") });

    private static Task<string> PreviewAsync(IRedbQueryable<EmployeeProps> source)
        => source.GroupByArray(e => e.Contacts!.SelectMany(c => c.Metadata!), m => m.Label)
            .ToSqlStringAsync(g => new Row { Label = g.Key, Count = Agg.Count(g), Values = Agg.Count(g, m => m.Value == "d") });

    private sealed class Row
    {
        public string? Label { get; set; }
        public int Count { get; set; }
        public int Values { get; set; }
    }

    private static EmployeeProps Employee(int i, string marker)
    {
        var p = TestDataFactory.CreateEmployee(i, firstName: $"{marker}-{i:D2}").Props;
        p.Contacts![0].Metadata = [new TestContactDetail { Label = "work", Value = $"v{i}" }];
        p.Contacts[1].Metadata = i % 2 == 0 ? [new TestContactDetail { Label = "night", Value = "d" }] : null;
        return p;
    }

    [Fact]
    public async Task FlatGrouping_RepeatedCall_RunsTheDataStatementAlone()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var marker = "mc" + Guid.NewGuid().ToString("N")[..10];
        for (var i = 0; i < 6; i++)
            await Redb.SaveAsync(new RedbObject<EmployeeProps> { name = $"{marker}-{i}", Props = Employee(i, marker) });
        IRedbQueryable<EmployeeProps> Source() => Redb.Query<EmployeeProps>().Where(e => e.FirstName.StartsWith(marker));

        var first = await GroupAsync(Source());
        first.Should().HaveCount(2, "work and night");

        (await CommandsAsync(() => GroupAsync(Source()))).Should().Be(1, "the metadata of the path is cached after the first call");
        (await CommandsAsync(() => PreviewAsync(Source()))).Should().Be(0, "the preview builds the statement from cached metadata");
    }

    [Fact]
    public async Task TreeGrouping_RepeatedCall_RunsTheDataStatementAlone()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var marker = "mt" + Guid.NewGuid().ToString("N")[..10];
        var root = new TreeRedbObject<EmployeeProps> { name = marker, Props = Employee(0, marker) };
        root.id = await Redb.SaveAsync(root);
        for (var i = 1; i < 6; i++)
            await Redb.CreateChildAsync(new TreeRedbObject<EmployeeProps> { name = $"{marker}-{i}", Props = Employee(i, marker) }, root);
        IRedbQueryable<EmployeeProps> Source() => Redb.TreeQuery<EmployeeProps>(root.id);

        var first = await GroupAsync(Source());
        first.Should().HaveCount(2, "work and night under the root");

        (await CommandsAsync(() => GroupAsync(Source()))).Should().Be(1, "the metadata of the path is cached after the first call");
        (await CommandsAsync(() => PreviewAsync(Source()))).Should().Be(0, "the preview builds the statement from cached metadata");
    }
}
