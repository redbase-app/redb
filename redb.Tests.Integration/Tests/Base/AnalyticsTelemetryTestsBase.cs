using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The grouping, window and aggregate statements appear in telemetry as commands on every provider: a repeated call (the
/// field metadata already cached) opens one redb.Core span per command. SQLite read them on the raw connection and
/// opened no span for the statement itself.
/// </summary>
public abstract class AnalyticsTelemetryTestsBase
{
    protected readonly IRedbService Redb;

    protected AnalyticsTelemetryTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Commands of one call: Pro runs the statement alone; Free first asks the module for the statement text.</summary>
    protected abstract int CommandsPerCall { get; }

    private async Task<IRedbQueryable<EmployeeProps>> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var marker = "tl" + Guid.NewGuid().ToString("N")[..10];
        for (var i = 0; i < 6; i++)
        {
            var p = TestDataFactory.CreateEmployee(i, firstName: $"{marker}-{i:D2}", department: marker,
                salary: 1000m * (i + 1), position: i % 2 == 0 ? "Even" : "Odd").Props;
            await Redb.SaveAsync(new RedbObject<EmployeeProps> { name = $"{marker}-{i}", Props = p });
        }
        return Redb.Query<EmployeeProps>().Where(e => e.Department == marker);
    }

    /// <summary>Runs <paramref name="action"/> once to warm the metadata, then counts the commands of a second run.</summary>
    private static async Task<int> SecondRunAsync(Func<Task> action)
    {
        await action();
        return await CommandCounter.CountAsync(action);
    }

    [Fact]
    public async Task GroupBy_AppearsAsItsCommands()
    {
        var source = await SeedAsync();
        (await SecondRunAsync(() => source.GroupBy(e => e.Position)
            .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g) }))).Should().Be(CommandsPerCall);
    }

    [Fact]
    public async Task Window_AppearsAsItsCommands()
    {
        var source = await SeedAsync();
        (await SecondRunAsync(() => source.WithWindow(w => w.OrderBy(e => e.Salary))
            .SelectAsync(e => new { e.Props.Salary, Row = Win.RowNumber() }))).Should().Be(CommandsPerCall);
    }

    [Fact]
    public async Task Aggregate_AppearsAsItsCommands()
    {
        var source = await SeedAsync();
        (await SecondRunAsync(() => source.AggregateAsync(x => new { Count = Agg.Count(), Sum = Agg.Sum(x.Props.Salary) })))
            .Should().Be(CommandsPerCall);
    }
}
