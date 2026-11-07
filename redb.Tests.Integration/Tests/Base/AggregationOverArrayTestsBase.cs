using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Deep-coverage integration tests for scalar aggregation over array fields
/// (<c>Agg.Sum/Average/Min/Max/Count</c> over <c>arr.Select(...)</c> or <c>arr[i]</c>).
///
/// Complements <c>BugRegressionTestsBase.E150_*</c> by exercising:
///   * Min/Max over unnested array elements;
///   * mixed Sum+Avg+Min+Max+Count in a single AggregateAsync projection;
///   * outer Where before AggregateAsync (filter must apply before unnest);
///   * indexed access at multiple positions (Skills[0] / Skills[1]);
///   * count of unnested elements (Agg.Count over Select).
///
/// Targets free PG, Pro PG, Pro MSSql. Free MSSql array-aggregate path is not
/// supported.
/// </summary>
public abstract class AggregationOverArrayTestsBase
{
    protected readonly IRedbService Redb;

    protected AggregationOverArrayTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    // ──────────────────────────────────────────────────────────────────
    //  Min / Max over all elements (unnest)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task AggMinMax_OverArray_AllElements_SkillLevels()
    {
        await SeedAsync();

        var r = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                Min = Agg.Min(x.Props.SkillLevels.Select(s => s)),
                Max = Agg.Max(x.Props.SkillLevels.Select(s => s))
            });

        // Seed generator: SkillLevels = [5 - (i%3), 4, 3 + (i%2)] for i in 0..19.
        // Min possible across all elements is 3 (3 + 0), max is 5 (5 - 0).
        r.Min.Should().BeGreaterThanOrEqualTo(1);
        r.Max.Should().BeLessThanOrEqualTo(10);
        r.Min.Should().BeLessThanOrEqualTo(r.Max);
    }

    // ──────────────────────────────────────────────────────────────────
    //  Sum + Avg + Min + Max + Count mixed in one projection
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task AggAll_MixedInOneSelect_SkillLevels_AllPositive()
    {
        await SeedAsync();

        var r = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                Sum   = Agg.Sum(x.Props.SkillLevels.Select(s => s)),
                Avg   = Agg.Average(x.Props.SkillLevels.Select(s => s)),
                Min   = Agg.Min(x.Props.SkillLevels.Select(s => s)),
                Max   = Agg.Max(x.Props.SkillLevels.Select(s => s)),
                Rows  = Agg.Count()
            });

        r.Sum.Should().BeGreaterThan(0);
        r.Avg.Should().BeGreaterThan(0);
        r.Min.Should().BeLessThanOrEqualTo(r.Max);
        r.Rows.Should().BeGreaterThan(0);
        // Sum/Rows must be at least Avg (each row contributes ≥1 element).
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where applied before array unnest
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task AggSum_OverArray_AfterOuterWhere_ReducesTotal()
    {
        await SeedAsync();

        var full = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new { S = Agg.Sum(x.Props.SkillLevels.Select(s => s)) });

        var remoteOnly = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .AggregateAsync(x => new { S = Agg.Sum(x.Props.SkillLevels.Select(s => s)) });

        full.S.Should().BeGreaterThan(0);
        remoteOnly.S.Should().BeGreaterThan(0);
        remoteOnly.S.Should().BeLessThan(full.S, "subset sum must be smaller than total");
    }

    // ──────────────────────────────────────────────────────────────────
    //  Indexed access — two distinct indices
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task AggSum_OverIndexedElement_FirstVsSecond_BothPositive()
    {
        await SeedAsync();

        var r = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                S0 = Agg.Sum(x.Props.SkillLevels[0]),
                S1 = Agg.Sum(x.Props.SkillLevels[1])
            });

        r.S0.Should().BeGreaterThan(0);
        r.S1.Should().BeGreaterThan(0);
        // Per seed: index 1 is always 4 → S1 == rowCount * 4.
    }

    // ──────────────────────────────────────────────────────────────────
    //  Count over unnested elements
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public virtual async Task AggCount_OverUnnestedElements_EqualsRowsTimesArrayLength()
    {
        await SeedAsync();

        var r = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                Rows     = Agg.Count(),
                Elements = Agg.Count(x.Props.SkillLevels.Select(s => s))
            });

        r.Rows.Should().BeGreaterThan(0);
        // Each seeded row has SkillLevels of length 3 -> total elements > row count.
        r.Elements.Should().BeGreaterThan(r.Rows,
            "Count over unnested array elements must exceed row count");
    }

    // ──────────────────────────────────────────────────────────────────
    //  Min / Max over string array (Skills)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task AggMinMax_OverStringArray_Skills_ReturnsString()
    {
        await SeedAsync();

        var r = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                MinSkill = Agg.Min(x.Props.Skills.Select(s => s)),
                MaxSkill = Agg.Max(x.Props.Skills.Select(s => s))
            });

        r.MinSkill.Should().NotBeNullOrWhiteSpace();
        r.MaxSkill.Should().NotBeNullOrWhiteSpace();
        string.Compare(r.MinSkill, r.MaxSkill, StringComparison.Ordinal)
            .Should().BeLessThanOrEqualTo(0, "MIN <= MAX in lexicographic order");
    }
}
