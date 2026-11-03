using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Pro.Models;
using redb.Core.Pro.Query;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Grouping;
using redb.Postgres.Sql;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// GroupByArray without a database (docs/array-groupby/PLAN.md): the condition of a conditional count, the strict
/// reading of aggregates, the array path, the shared Pro builder, the strict reading of the result.
/// </summary>
public class ArrayGroupingTests
{
    // ---- condition of Agg.Count ---------------------------------------------------------------------------------

    private static string Condition<T>(Expression<Func<T, bool>> predicate, GroupItemKind kind = GroupItemKind.ArrayClassElement)
        => ItemPredicateParser.ToJson(predicate, kind, "test");

    [Fact]
    public void Condition_BooleanField_IsEqualToTrue()
        => Condition<TestContact>(c => c.IsVerified).Should().Be("{\"$eq\":[{\"$field\":\"IsVerified\"},{\"$const\":true}]}");

    [Fact]
    public void Condition_NullTests_AreIsNullAndNotNull()
    {
        Condition<TestContact>(c => c.Value == null).Should().Be("{\"$isnull\":{\"$field\":\"Value\"}}");
        Condition<TestContact>(c => null != c.Value).Should().Be("{\"$notnull\":{\"$field\":\"Value\"}}");
    }

    [Fact]
    public void Condition_LogicAndCapturedConstants()
    {
        var type = "email";
        Condition<TestContact>(c => !c.IsVerified || c.Type == type).Should().Be(
            "{\"$or\":[{\"$not\":{\"$eq\":[{\"$field\":\"IsVerified\"},{\"$const\":true}]}},{\"$eq\":[{\"$field\":\"Type\"},{\"$const\":\"email\"}]}]}");
    }

    [Fact]
    public void Condition_NestedField_And_ElementOfASimpleArray()
    {
        Condition<ArrayItem>(i => i.Place!.City == "Riga").Should().Be("{\"$eq\":[{\"$field\":\"Place.City\"},{\"$const\":\"Riga\"}]}");
        Condition<long>(x => x > 5, GroupItemKind.ArrayValueElement).Should().Be("{\"$gt\":[{\"$field\":\"$element\"},{\"$const\":5}]}");
    }

    [Fact]
    public void Condition_StringMethod_IsRefusedWithWhatIsSupported()
    {
        var act = () => Condition<TestContact>(c => c.Value.Contains("x"));
        act.Should().Throw<NotSupportedException>().WithMessage("*condition of Agg.Count may compare a field*");
    }

    [Fact]
    public void Condition_OrderingAgainstNull_IsRefused()
    {
        var act = () => Condition<ArrayItem>(i => i.Size > (int?)null);
        act.Should().Throw<NotSupportedException>();
    }

    // ---- HAVING and strict aggregates ---------------------------------------------------------------------------

    [Fact]
    public void Having_ConditionalCount_CarriesItsFilter()
    {
        Expression<Func<IRedbGrouping<string, TestContact>, bool>> having = g => Agg.Count(g, c => c.IsVerified) > 1;
        HavingPredicateParser.ToJson(having, GroupItemKind.ArrayClassElement).Should().Be(
            "{\"$gt\":[{\"$count\":\"*\",\"filter\":{\"$eq\":[{\"$field\":\"IsVerified\"},{\"$const\":true}]}},{\"$const\":1}]}");
    }

    [Fact]
    public void Having_ComputedAggregate_IsRefused()
    {
        Expression<Func<IRedbGrouping<string, EmployeeProps>, bool>> having = g => Agg.Sum(g, e => e.Salary * 2) > 1;
        var act = () => HavingPredicateParser.ToJson(having);
        act.Should().Throw<NotSupportedException>().WithMessage("*computation*");
    }

    [Fact]
    public void Aggregate_TheWholeObject_IsRefused()
    {
        Expression<Func<IRedbGrouping<string, EmployeeProps>, bool>> having = g => Agg.Count(g) > 0 && Agg.Max(g, e => e) != null;
        var act = () => HavingPredicateParser.ToJson(having);
        act.Should().Throw<NotSupportedException>();
    }

    // ---- array path -----------------------------------------------------------------------------------------------

    private static RedbArrayGroupedQueryable<TKey, TItem, TProps> Grouping<TKey, TItem, TProps>(
        Expression<Func<TProps, IEnumerable<TItem>>> array, Expression<Func<TItem, TKey>> key) where TProps : class, new()
        => new(null!, 1, null, null, array, key);

    [Fact]
    public void ArrayPath_SelectManyWithAResultSelector_IsRefused()
    {
        var act = () => Grouping<string, TestContactDetail, EmployeeProps>(e => e.Contacts!.SelectMany(c => c.Metadata!, (c, m) => m), m => m.Label);
        act.Should().Throw<NotSupportedException>().WithMessage("*result selector*");
    }

    [Fact]
    public void ArrayPath_ComputedCollection_IsRefused()
    {
        var act = () => Grouping<string, TestContact, EmployeeProps>(e => e.Contacts!.Where(c => c.IsVerified), c => c.Type);
        act.Should().Throw<NotSupportedException>().WithMessage("*SelectMany over an inner array*");
    }

    [Fact]
    public void ElementKey_OnAnArrayOfClasses_IsRefused()
    {
        var act = () => Grouping<TestContact, TestContact, EmployeeProps>(e => e.Contacts!, c => c);
        act.Should().Throw<NotSupportedException>();
    }

    // ---- shared Pro builder ----------------------------------------------------------------------------------------

    private static readonly Dictionary<(long? Parent, string Name), SchemeMetadataCacheRow> Meta = new()
    {
        [(null, "Contacts")] = Row(10, "Guid", "Class", "Array"),
        [(10, "Type")] = Row(11, "String", "String", null),
        [(10, "IsVerified")] = Row(12, "Boolean", "Boolean", null),
        [(10, "Metadata")] = Row(13, "Guid", "Class", "Array"),
        [(13, "Label")] = Row(14, "String", "String", null),
        [(null, "Skills")] = Row(20, "String", "String", "Array"),
        [(null, "Items")] = Row(30, "Guid", "Class", "Array"),
        [(30, "Place")] = Row(31, "Guid", "Class", null),
        [(31, "City")] = Row(32, "String", "String", null),
        [(null, "Directory")] = Row(40, "String", "String", "Dictionary"),
    };

    private static SchemeMetadataCacheRow Row(long sid, string dbType, string typeName, string? collection)
        => new() { StructureId = sid, DbType = dbType, TypeName = typeName, CollectionTypeName = collection };

    private static Task<string> BuildAsync(string arrayPath, string[] keys, IReadOnlyList<AggregateRequest>? aggregations = null,
        ArrayGroupSqlBuilder.IdSource? ids = null, string? havingJson = null)
    {
        var builder = new ArrayGroupSqlBuilder(new PostgreSqlDialect(),
            (scheme, name, parent, ct) => Task.FromResult(Meta.GetValueOrDefault((parent, name))), "test");
        var query = new ArrayGroupQuery
        {
            SchemeId = 7,
            ArrayPath = arrayPath,
            GroupFields = keys.Select((k, i) => new GroupFieldRequest { FieldPath = k, Alias = "k" + i }).ToList(),
            Aggregations = aggregations ?? [new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "c" }],
            HavingJson = havingJson
        };
        query.EnsureValid();
        var collector = new SqlParameterCollector(new PostgreSqlDialect());
        return builder.BuildAsync(query, collector, _ => Task.FromResult(ids), rankingNeedsOrder: false, CancellationToken.None)
            .ContinueWith(t => t.Result.Select);
    }

    [Fact]
    public async Task Builder_InnerArray_ReadsTheLastArrayByItsStructure()
    {
        var sql = await BuildAsync("Contacts.Metadata", ["Label"]);
        sql.Should().Contain("arr._id_structure = 13").And.Contain("_id_structure = 14")
            .And.Contain("._array_parent_id = arr._id").And.Contain("._array_parent_id IS NOT NULL")
            .And.NotContain("= 10");
    }

    [Fact]
    public async Task Builder_FieldOfAClassInsideTheElement_JoinsThroughTheClass()
    {
        var sql = await BuildAsync("Items", ["Place.City"]);
        sql.Should().Contain("e1._array_parent_id = arr._id AND e1._id_structure = 31")
            .And.Contain("e2._array_parent_id = e1._id AND e2._id_structure = 32")
            .And.Contain("e2._string");
    }

    [Fact]
    public async Task Builder_ElementOfASimpleArray_ReadsTheElementRow()
        => (await BuildAsync("Skills", [GroupFieldRequest.ElementPath])).Should().Contain("arr._string").And.NotContain("LEFT JOIN");

    [Fact]
    public async Task Builder_ConditionalCount_IsACaseInsideCount()
    {
        var sql = await BuildAsync("Contacts", ["Type"], [new AggregateRequest
        {
            FieldPath = "*", Function = AggregateFunction.Count, Alias = "v",
            ConditionJson = "{\"$eq\":[{\"$field\":\"IsVerified\"},{\"$const\":true}]}"
        }]);
        sql.Should().Contain("COUNT(CASE WHEN (e").And.Contain("_boolean = ");
    }

    [Fact]
    public async Task Builder_WithIds_StartsFromThem()
    {
        var sql = await BuildAsync("Contacts", ["Type"], ids: new ArrayGroupSqlBuilder.IdSource("SELECT 1 AS _id"));
        sql.Should().Contain("FROM (SELECT 1 AS _id) f").And.Contain("arr._id_object = f._id");
    }

    [Theory]
    [InlineData("Contacts", "$element", "*names the element itself*")]
    [InlineData("Contacts", "$foo", "*is not a field of the elements*")]
    [InlineData("Skills", "Name", "*simple values*")]
    [InlineData("Contacts", "Metadata", "*collection inside the element*")]
    [InlineData("Items", "Place", "*is a class*")]
    [InlineData("Directory", "$element", "*dictionary*")]
    public async Task Builder_WhatItCannotAnswer_IsRefused(string arrayPath, string key, string message)
    {
        var act = () => BuildAsync(arrayPath, [key]);
        await act.Should().ThrowAsync<NotSupportedException>().WithMessage(message);
    }

    [Fact]
    public async Task Builder_UnknownField_Throws()
    {
        var act = () => BuildAsync("Contacts", ["Missing"]);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*is not a field of the elements*");
    }

    // ---- request and result ----------------------------------------------------------------------------------------

    [Fact]
    public void Request_WithoutKeys_IsRefused()
    {
        var query = new ArrayGroupQuery { SchemeId = 1, ArrayPath = "Contacts", GroupFields = [], Aggregations = [] };
        var act = () => query.EnsureValid();
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Result_NotAnArray_OrWithoutAColumn_IsAnError()
    {
        Expression<Func<IRedbGrouping<string, TestContact>, object>> selector = g => new { Type = g.Key, C = Agg.Count(g) };
        var keys = new List<GroupFieldRequest> { new() { FieldPath = "Type", Alias = "Type" } };

        var notArray = () => ArrayGroupMaterializer.Materialize<object>(JsonDocument.Parse("{}"), selector, keys, "p", "test");
        notArray.Should().Throw<InvalidOperationException>();

        var missing = () => ArrayGroupMaterializer.Materialize<object>(JsonDocument.Parse("[{\"Type\":\"a\"}]"), selector, keys, "p", "test");
        missing.Should().Throw<InvalidOperationException>().WithMessage("*without the column 'C'*");

        var nullKey = ArrayGroupMaterializer.Materialize<object>(JsonDocument.Parse("[{\"Type\":null,\"C\":2}]"), selector, keys, "p", "test");
        nullKey.Should().ContainSingle();
    }
}
