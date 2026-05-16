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
}
