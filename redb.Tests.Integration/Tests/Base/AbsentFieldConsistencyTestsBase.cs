using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// An object that has no value for a field must still be seen by every kind of query: counted,
/// selected by "Field == null", grouped under the NULL key, kept in a window.
///
/// Aggregates and groupings used to range over rows of _values instead of objects, so an object
/// without a row on any field of the statement was not in their input at all. Three symptoms, all
/// silent: <c>Agg.Count()</c> depended on which other aggregates shared the batch; a null check
/// selected nothing; a nullable grouping key produced no NULL group. Pages, CountAsync and
/// AnyAsync, driven by _objects, answered correctly, so the same Where gave different sets
/// depending on the terminal operation.
///
/// Every expectation here is computed in memory from the loaded objects, and every query runs
/// with the planner off and on: both forms have to tell the truth, not merely agree.
/// </summary>
public abstract class AbsentFieldConsistencyTestsBase
{
    protected readonly IRedbService Redb;

    protected AbsentFieldConsistencyTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Two persons with a status and four without: the field under test is absent for most.</summary>
    private async Task<List<RedbObject<PersonProps>>> SeedAndLoadAsync(string listName)
    {
        var existing = await Redb.ListProvider.GetListByNameAsync(listName);
        if (existing != null)
        {
            var oldItems = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (oldItems.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(oldItems.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }

        var list = await Redb.ListProvider.SaveListAsync(RedbList.Create(listName, listName));
        var items = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked"]);

        var persons = Enumerable.Range(0, 6).Select(i => new RedbObject<PersonProps>
        {
            name = $"{listName}_{i}",
            Props = new PersonProps
            {
                Name = $"Person{i}",
                Age = 30 + i % 2,
                Email = $"p{i}@absent.test",
                Status = i < 2 ? items[i] : null
            }
        }).ToList();

        await Redb.SaveAsync(persons);

        var all = await Redb.Query<PersonProps>().ToListAsync();
        all.Count(p => p.Props.Status == null).Should().BeGreaterThan(0, "the seed leaves the status empty");
        all.Count(p => p.Props.Status != null).Should().BeGreaterThan(0, "the seed sets the status");
        return all;
    }

    private async Task EachFormAsync(Func<string, Task> check)
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            foreach (var on in new[] { false, true })
            {
                Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = on);
                await check(on ? "planner on" : "planner off");
            }
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    [Fact]
    public async Task NullCheck_SameSetForEveryTerminalOperation()
    {
        var all = await SeedAndLoadAsync("AbsentField_Terminal");
        var expected = all.Where(p => p.Props.Status == null).ToList();

        await EachFormAsync(async form =>
        {
            IRedbQueryable<PersonProps> Query() => Redb.Query<PersonProps>().Where(p => p.Status == null);

            (await Query().CountAsync()).Should().Be(expected.Count, $"CountAsync, {form}");
            (await Query().ToListAsync()).Count.Should().Be(expected.Count, $"ToListAsync, {form}");
            (await Query().AnyAsync()).Should().BeTrue($"AnyAsync, {form}");

            var counted = await Query().AggregateAsync(x => new { Count = Agg.Count() });
            counted.Count.Should().Be(expected.Count,
                $"Agg.Count() alone reads no field but the filtered one, which these objects lack, {form}");

            var batch = await Query().AggregateAsync(x => new { Count = Agg.Count(), SumAge = Agg.Sum(x.Props.Age) });
            batch.Count.Should().Be(expected.Count, $"Agg.Count() next to Agg.Sum, {form}");
            ((long)batch.SumAge).Should().Be(expected.Sum(p => (long)p.Props.Age), $"Agg.Sum(Age), {form}");

            var sum = await Query().SumAsync(p => p.Age);
            ((long)sum).Should().Be(expected.Sum(p => (long)p.Props.Age), $"SumAsync(Age), {form}");
        });
    }

    [Fact]
    public async Task AggregateCount_DoesNotDependOnOtherAggregates()
    {
        var all = await SeedAndLoadAsync("AbsentField_Batch");

        await EachFormAsync(async form =>
        {
            var alone = await Redb.Query<PersonProps>().AggregateAsync(x => new { Count = Agg.Count() });
            alone.Count.Should().Be(all.Count, $"Agg.Count() alone, {form}");

            var withNullable = await Redb.Query<PersonProps>()
                .AggregateAsync(x => new { Count = Agg.Count(), MaxStatus = Agg.Max(x.Props.Status!.Id) });
            withNullable.Count.Should().Be(all.Count,
                $"an aggregate over a field most objects lack must not shrink Agg.Count(), {form}");
        });
    }

    [Fact]
    public async Task GroupBy_NullableKey_KeepsObjectsWithoutValue()
    {
        var all = await SeedAndLoadAsync("AbsentField_GroupBy");
        var withoutStatus = all.Count(p => p.Props.Status == null);
        var distinctStatuses = all.Where(p => p.Props.Status != null).Select(p => p.Props.Status!.Id).Distinct().Count();

        await EachFormAsync(async form =>
        {
            var groups = await Redb.Query<PersonProps>()
                .GroupBy(p => p.Status!.Id)
                .SelectAsync(g => new { g.Key, Count = Agg.Count(g) });

            groups.Sum(g => g.Count).Should().Be(all.Count, $"every object belongs to a group, {form}");
            groups.Count.Should().Be(distinctStatuses + 1, $"objects without a status form the NULL group, {form}");

            var filtered = await Redb.Query<PersonProps>()
                .Where(p => p.Status == null)
                .GroupBy(p => p.Status!.Id)
                .SelectAsync(g => new { g.Key, Count = Agg.Count(g) });

            filtered.Sum(g => g.Count).Should().Be(withoutStatus, $"null check before GroupBy, {form}");
            filtered.Count.Should().Be(1, $"only the NULL group is left, {form}");
        });
    }

    [Fact]
    public async Task DistinctBy_NullableKey_KeepsOneObjectWithoutValue()
    {
        var all = await SeedAndLoadAsync("AbsentField_DistinctBy");
        var distinctStatuses = all.Where(p => p.Props.Status != null).Select(p => p.Props.Status!.Id).Distinct().Count();

        await EachFormAsync(async form =>
        {
            var flat = await Redb.Query<PersonProps>().DistinctBy(p => p.Status!.Id).ToListAsync();
            flat.Count.Should().Be(distinctStatuses + 1, $"objects without a status share the NULL key, flat query, {form}");

            var tree = await Redb.TreeQuery<PersonProps>().DistinctBy(p => p.Status!.Id).ToListAsync();
            tree.Count.Should().Be(distinctStatuses + 1, $"objects without a status share the NULL key, tree query, {form}");
        });
    }

    [Fact]
    public async Task OrderBy_NullableKey_DoesNotDropObjects()
    {
        var all = await SeedAndLoadAsync("AbsentField_OrderBy");

        await EachFormAsync(async form =>
        {
            var flat = await Redb.Query<PersonProps>().OrderBy(p => p.Status!.Id).ToListAsync();
            flat.Count.Should().Be(all.Count, $"sorting is not filtering, flat query, {form}");

            var paged = await Redb.Query<PersonProps>().OrderBy(p => p.Status!.Id).Take(all.Count + 10).ToListAsync();
            paged.Count.Should().Be(all.Count, $"sorting is not filtering, flat page, {form}");

            var tree = await Redb.TreeQuery<PersonProps>().OrderBy(p => p.Status!.Id).ToListAsync();
            tree.Count.Should().Be(all.Count, $"sorting is not filtering, tree query, {form}");
        });
    }

    [Fact]
    public async Task Window_PartitionByNullableField_KeepsObjectsWithoutValue()
    {
        var all = await SeedAndLoadAsync("AbsentField_Window");
        var withoutStatus = all.Count(p => p.Props.Status == null);

        await EachFormAsync(async form =>
        {
            var rows = await Redb.Query<PersonProps>()
                .WithWindow(w => w.PartitionBy(p => p.Status!.Id).OrderByDesc(p => p.Status!.Id))
                .SelectAsync(x => new { x.Id, Row = Win.RowNumber() });
            rows.Count.Should().Be(all.Count, $"the window reads no field but the one most objects lack, {form}");

            var filtered = await Redb.Query<PersonProps>()
                .Where(p => p.Status == null)
                .WithWindow(w => w.PartitionBy(p => p.Status!.Id).OrderByDesc(p => p.Status!.Id))
                .SelectAsync(x => new { x.Id, Row = Win.RowNumber() });
            filtered.Count.Should().Be(withoutStatus, $"null check before the window, {form}");
        });
    }
}
