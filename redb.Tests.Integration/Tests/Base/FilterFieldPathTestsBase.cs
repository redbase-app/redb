using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The field path of a filter is a chain of fields of the queried object. An array or list element by index and a method
/// call inside the path are refused with one message: the parser used to drop the part before them, so
/// <c>Items[0].Name == "top"</c> filtered by the field Name of the object itself and returned it.
/// The paths that are supported keep answering: a field, a field of a nested class, Nullable Value and HasValue,
/// Length and Count, a dictionary entry by key (Contains over an array of simple values and Any over an array of list
/// items have their own classes: ArrayTestsBase, ListTestsBase).
///
/// One object per test, read by its id: Name "top", Rank 1; Items[0] and Parts[0] have Name "inner", Rank 7.
/// </summary>
public abstract class FilterFieldPathTestsBase
{
    protected readonly IRedbService Redb;

    protected FilterFieldPathTestsBase(IRedbService redb) => Redb = redb;

    private async Task<IRedbQueryable<FieldPathProps>> SeedAsync()
    {
        await Redb.SyncSchemeAsync<FieldPathProps>();
        var obj = new RedbObject<FieldPathProps>
        {
            name = "fp-" + Guid.NewGuid().ToString("N")[..8],
            Props = new FieldPathProps
            {
                Name = "top",
                Rank = 1,
                Items = [new FieldPathItem { Name = "inner", Rank = 7 }],
                Parts = [new FieldPathItem { Name = "inner", Rank = 7 }],
                ByKey = new Dictionary<string, FieldPathItem> { ["k"] = new() { Name = "entry", Rank = 5 } },
                Main = new FieldPathItem { Name = "main", Rank = 3, Score = 4 }
            }
        };
        var id = await Redb.SaveAsync(obj);
        return Redb.Query<FieldPathProps>().WhereRedb(o => o.Id == id);
    }

    public static IEnumerable<object[]> RefusedPaths()
    {
        // Each of these compared the top-level field of the same name and returned the object.
        yield return ["array-index", (Expression<Func<FieldPathProps, bool>>)(p => p.Items![0].Name == "top"), "Items"];
        yield return ["list-index", (Expression<Func<FieldPathProps, bool>>)(p => p.Parts![0].Rank == 1), "Parts"];
        yield return ["first", (Expression<Func<FieldPathProps, bool>>)(p => p.Items!.First().Name == "top"), "First()"];
        yield return ["array-index-null", (Expression<Func<FieldPathProps, bool>>)(p => p.Items![0].Score == null), "Items"];
        yield return ["array-index-string-method", (Expression<Func<FieldPathProps, bool>>)(p => p.Items![0].Name.StartsWith("to")), "Items"];
    }

    [Theory]
    [MemberData(nameof(RefusedPaths))]
    public async Task ElementByIndexOrMethodCallInThePath_IsRefused_NamingThePathAndTheWayOut(
        string kind, Expression<Func<FieldPathProps, bool>> predicate, string named)
    {
        var q = await SeedAsync();

        var act = () => q.Where(predicate).ToListAsync();

        var ex = (await act.Should().ThrowAsync<NotSupportedException>($"case {kind}")).Which;
        ex.Message.Should().Contain(named).And.Contain("Contains(").And.Contain("Any(").And.Contain("dictionary");
    }

    public static IEnumerable<object[]> SupportedPaths()
    {
        yield return ["field", (Expression<Func<FieldPathProps, bool>>)(p => p.Name == "top"), true];
        yield return ["field-miss", (Expression<Func<FieldPathProps, bool>>)(p => p.Name == "inner"), false];
        yield return ["nested-class", (Expression<Func<FieldPathProps, bool>>)(p => p.Main!.Name == "main"), true];
        yield return ["nested-class-miss", (Expression<Func<FieldPathProps, bool>>)(p => p.Main!.Name == "top"), false];
        yield return ["nullable-value", (Expression<Func<FieldPathProps, bool>>)(p => p.Main!.Score!.Value == 4), true];
        yield return ["nullable-has-value", (Expression<Func<FieldPathProps, bool>>)(p => p.Main!.Score.HasValue), true];
        yield return ["string-length", (Expression<Func<FieldPathProps, bool>>)(p => p.Name.Length == 3), true];
        yield return ["array-length", (Expression<Func<FieldPathProps, bool>>)(p => p.Items!.Length == 1), true];
        yield return ["list-count", (Expression<Func<FieldPathProps, bool>>)(p => p.Parts!.Count == 1), true];
        yield return ["dictionary-entry", (Expression<Func<FieldPathProps, bool>>)(p => p.ByKey!["k"].Name == "entry"), true];
        yield return ["dictionary-entry-miss", (Expression<Func<FieldPathProps, bool>>)(p => p.ByKey!["k"].Name == "top"), false];
    }

    [Theory]
    [MemberData(nameof(SupportedPaths))]
    public async Task SupportedPath_KeepsAnswering(string kind, Expression<Func<FieldPathProps, bool>> predicate, bool matches)
    {
        var q = await SeedAsync();

        var rows = await q.Where(predicate).ToListAsync();

        rows.Should().HaveCount(matches ? 1 : 0, $"case {kind}");
    }
}
