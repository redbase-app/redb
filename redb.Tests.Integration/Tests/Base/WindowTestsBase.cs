using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

public abstract class WindowTestsBase
{
    protected readonly IRedbService Redb;

    protected WindowTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 15);
    }

    public class WindowRowDto
    {
        public string? FirstName { get; set; }
        public long RowNum { get; set; }
    }

    [Fact]
    public async Task Window_SelectAsync_IntoDtoMemberInit()
    {
        // G-1 доехал до оконных (решение владельца 2026-09-04): раньше DTO давал громкий отказ,
        // а до ревью - молча пустой результат.
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary))
            .SelectAsync(e => new WindowRowDto
            {
                FirstName = e.Props.FirstName,
                RowNum = Win.RowNumber(),
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.FirstName.Should().NotBeNullOrEmpty();
            r.RowNum.Should().BeGreaterThan(0);
        });
    }

        [Fact]
    public async Task Window_RowNumber_PartitionByDept()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Props.FirstName,
                e.Props.Department,
                e.Props.Salary,
                RowNum = Win.RowNumber()
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.RowNum.Should().BeGreaterThan(0));

        // Within each department, row numbers should start at 1
        var grouped = results.GroupBy(r => r.Department);
        foreach (var group in grouped)
        {
            group.Min(r => r.RowNum).Should().Be(1);
        }
    }

    [Fact]
    public async Task Window_RunningSum_OverSalary()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary)
                .Frame(Frame.Rows().UnboundedPreceding()))
            .SelectAsync(e => new
            {
                e.Props.FirstName,
                e.Props.Department,
                e.Props.Salary,
                RunSum = Win.Sum(e.Props.Salary)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.RunSum.Should().BeGreaterThanOrEqualTo(r.Salary);
        });
    }

    [Fact]
    public async Task Window_Rank_ByDepartment()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Props.FirstName,
                e.Props.Department,
                e.Props.Salary,
                RankNum = Win.Rank()
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.RankNum.Should().BeGreaterThan(0));
    }

    [Fact]
    public async Task Window_DenseRank()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Props.Department,
                e.Props.Salary,
                DRank = Win.DenseRank()
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.DRank.Should().BeGreaterThan(0));
    }

    [Fact]
    public async Task Window_Ntile_SplitsIntoBuckets()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w.OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Props.FirstName,
                e.Props.Salary,
                Bucket = Win.Ntile(3)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Bucket.Should().BeInRange(1, 3));
    }

    [Fact]
    public async Task Window_Lag_PreviousValue()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Props.FirstName,
                e.Props.Department,
                e.Props.Salary,
                PrevSalary = Win.Lag(e.Props.Salary)
            });

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Window_Lead_NextValue()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary))
            .SelectAsync(e => new
            {
                e.Props.FirstName,
                e.Props.Salary,
                NextSalary = Win.Lead(e.Props.Salary)
            });

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Window_FirstValue_LastValue()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary)
                .Frame(Frame.Rows().UnboundedPreceding().AndUnboundedFollowing()))
            .SelectAsync(e => new
            {
                e.Props.Department,
                e.Props.Salary,
                TopSalary = Win.FirstValue(e.Props.Salary),
                LowestSalary = Win.LastValue(e.Props.Salary)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.TopSalary.Should().BeGreaterThanOrEqualTo(r.Salary);
            r.LowestSalary.Should().BeLessThanOrEqualTo(r.Salary);
        });
    }

    // An explicit frame must reach SQL as written. The checks above compare with >= and <=, which a
    // one-row frame also satisfies, so a frame silently turned into CURRENT ROW passed them (review
    // 2026-09-24, FRM-1: the Pro providers read another JSON shape than the core writes). The two facts
    // below compare exact sums, computed from the very rows the query returned.

    [Fact]
    public async Task Window_FullPartitionFrame_SumsTheWholeDepartment()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary)
                .Frame(Frame.Rows().UnboundedPreceding().AndUnboundedFollowing()))
            .SelectAsync(e => new
            {
                e.Props.Department,
                e.Props.Salary,
                DeptSum = Win.Sum(e.Props.Salary)
            });

        results.Should().NotBeEmpty();
        var totals = results.GroupBy(r => r.Department).ToDictionary(g => g.Key, g => g.Sum(r => r.Salary));
        results.Should().AllSatisfy(r =>
            r.DeptSum.Should().BeApproximately(totals[r.Department], 0.01m,
                "a frame over the whole partition sums every salary of the department, not the current row alone"));
        totals.Should().Contain(t => results.Count(r => r.Department == t.Key) > 1,
            "precondition: some department has more than one employee, or a one-row frame could not be told apart");
    }

    [Fact]
    public async Task Window_SlidingFrame_SumsThePreviousRowAndThisOne()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .WithWindow(w => w
                .PartitionBy(e => e.Department)
                .OrderByDesc(e => e.Salary)
                .Frame(Frame.Rows().Preceding(1).AndCurrentRow()))
            .SelectAsync(e => new
            {
                e.Props.Department,
                e.Props.Salary,
                PairSum = Win.Sum(e.Props.Salary)
            });

        results.Should().NotBeEmpty();
        foreach (var department in results.GroupBy(r => r.Department))
        {
            // Ties in salary may swap which employee stands where, but not the sequence of salaries, so the
            // multiset of "this salary plus the one before it" is fixed.
            var ordered = department.Select(r => r.Salary).OrderByDescending(s => s).ToList();
            var expected = ordered.Select((s, i) => i == 0 ? s : s + ordered[i - 1]).OrderBy(s => s).ToList();
            var actual = department.Select(r => r.PairSum).OrderBy(s => s).ToList();

            actual.Should().HaveCount(expected.Count);
            for (var i = 0; i < expected.Count; i++)
                actual[i].Should().BeApproximately(expected[i], 0.01m,
                    $"ROWS BETWEEN 1 PRECEDING AND CURRENT ROW in department '{department.Key}'");
        }
    }
}
