using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Concurrency stress integration tests — drives the query pipeline with
/// 50 parallel tasks against shared seed data.
/// Catches parameter-list leaks between concurrent queries (a classical bug
/// in inline-parameterised SQL builders that re-use a global counter), and
/// asserts that no exception leaks out.
///
/// IMPORTANT: a single <see cref="IRedbService"/> instance shares one
/// <see cref="Core.Data.IRedbContext"/> (= one Npgsql connection in PG, one
/// EF DbContext in MSSql). Neither can multiplex concurrent commands, so
/// each parallel worker must resolve its OWN <see cref="IRedbService"/> from
/// a fresh DI scope.
///
/// Free MSSql is excluded because some query paths (array aggregation, HAVING)
/// are unsupported.
/// </summary>
public abstract class ConcurrencyStressTestsBase
{
    private const int Parallelism = 50;

    protected readonly IRedbService Redb;
    protected readonly IServiceProvider Services;

    protected ConcurrencyStressTestsBase(IRedbService redb, IServiceProvider services)
    {
        Redb = redb;
        Services = services;
    }

    private async Task SeedAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    /// <summary>
    /// Runs <paramref name="work"/> in parallel with each task using its own
    /// freshly-scoped <see cref="IRedbService"/> instance (= own connection).
    /// </summary>
    private Task<T[]> RunParallel<T>(int count, Func<IRedbService, Task<T>> work)
    {
        var tasks = Enumerable.Range(0, count).Select(async _ =>
        {
            using var scope = Services.CreateScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            return await work(redb);
        }).ToArray();
        return Task.WhenAll(tasks);
    }

    [Fact]
    public async Task Stress_50Parallel_GroupByArray_NoExceptionsAndStableShape()
    {
        await SeedAsync();

        var shapes = await RunParallel(Parallelism, async redb =>
        {
            var res = await redb.Query<EmployeeProps>()
                .GroupByArray(e => e.Contacts!, c => c.Type)
                .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });
            return res.OrderBy(r => r.Type).Select(r => (r.Type, r.C)).ToArray();
        });

        shapes.Should().HaveCount(Parallelism);
        var reference = shapes[0];
        reference.Should().NotBeEmpty();
        shapes.Should().AllSatisfy(s => s.Should().Equal(reference,
            "every parallel GroupByArray must yield identical aggregation shape"));
    }

    [Fact]
    public async Task Stress_50Parallel_AggregateOverArray_NoParamLeak()
    {
        await SeedAsync();

        var results = await RunParallel(Parallelism, redb => redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                S = Agg.Sum(x.Props.SkillLevels.Select(s => s)),
                A = Agg.Average(x.Props.SkillLevels.Select(s => s))
            }));

        results.Should().HaveCount(Parallelism);
        var first = results[0];
        results.Should().AllSatisfy(r =>
        {
            r.S.Should().Be(first.S, "Sum must be deterministic under concurrency");
            r.A.Should().Be(first.A, "Average must be deterministic under concurrency");
        });
    }

    [Fact]
    public async Task Stress_50Parallel_HavingQueries_DistinctThresholds_DoNotInterfere()
    {
        await SeedAsync();

        var tasks = Enumerable.Range(0, Parallelism).Select(async i =>
        {
            var threshold = i % 5;
            using var scope = Services.CreateScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var res = await redb.Query<EmployeeProps>()
                .GroupByArray(e => e.Contacts!, c => c.Type)
                .Having(g => Agg.Count(g) > threshold)
                .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });
            return (threshold, count: res.Count);
        }).ToArray();

        var results = await Task.WhenAll(tasks);
        results.Should().HaveCount(Parallelism);
        // Group results by threshold; results sharing the same threshold MUST have identical count.
        foreach (var grp in results.GroupBy(r => r.threshold))
        {
            grp.Select(r => r.count).Distinct().Should().HaveCount(1,
                $"all queries with threshold={grp.Key} must produce the same row count");
        }
    }
}
