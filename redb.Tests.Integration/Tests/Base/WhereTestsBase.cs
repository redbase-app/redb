using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

public abstract class WhereTestsBase
{
    protected readonly IRedbService Redb;
    private bool _seeded;
    private List<long> _seededIds = new();

    protected WhereTestsBase(IRedbService redb) => Redb = redb;

    protected async Task EnsureSeededAsync()
    {
        if (_seeded) return;
        _seededIds = await TestDataFactory.SeedEmployees(Redb, 20);
        _seeded = true;
    }

    [Fact]
    public async Task Where_EqualString_Filters()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Position == "Senior Developer")
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Position.Should().Be("Senior Developer"));
    }

    [Fact]
    public async Task Where_EqualInt_Filters()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age > 30)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Age.Should().BeGreaterThan(30));
    }

    [Fact]
    public async Task Where_And_CombinesConditions()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 50000m && e.IsRemote)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
        {
            r.Props.Salary.Should().BeGreaterThan(50000m);
            r.Props.IsRemote.Should().BeTrue();
        });
    }

    [Fact]
    public async Task Where_Or_CombinesConditions()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering" || e.Department == "Marketing")
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Department.Should().BeOneOf("Engineering", "Marketing"));
    }

    [Fact]
    public async Task Where_Not_InvertsCondition()
    {
        await EnsureSeededAsync();

        var all = await Redb.Query<EmployeeProps>().CountAsync();
        var notRemote = await Redb.Query<EmployeeProps>()
            .Where(e => !e.IsRemote)
            .CountAsync();
        var remote = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .CountAsync();

        (remote + notRemote).Should().Be(all);
    }

    [Fact]
    public async Task Where_GreaterThan_Decimal()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary >= 80000m)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r => r.Props.Salary.Should().BeGreaterThanOrEqualTo(80000m));
    }

    [Fact]
    public async Task Where_LessThan_Int()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age < 30)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r => r.Props.Age.Should().BeLessThan(30));
    }

    [Fact]
    public async Task Where_Range_BetweenValues()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Salary >= 50000m && e.Salary <= 90000m)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Props.Salary.Should().BeGreaterThanOrEqualTo(50000m);
            r.Props.Salary.Should().BeLessThanOrEqualTo(90000m);
        });
    }

    [Fact]
    public async Task Where_ChainedFilters_Accumulate()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age > 25)
            .Where(e => e.IsRemote)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
        {
            r.Props.Age.Should().BeGreaterThan(25);
            r.Props.IsRemote.Should().BeTrue();
        });
    }

    [Fact]
    public async Task Where_DateTime_Comparison()
    {
        await EnsureSeededAsync();

        var cutoff = new DateTime(2020, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.HireDate > cutoff)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.HireDate.Should().BeAfter(cutoff));
    }

    [Fact]
    public async Task Where_StringContains_Filters()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer"))
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Position.Should().Contain("Developer"));
    }

    [Fact]
    public async Task Where_StringStartsWith_Filters()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Position.StartsWith("Senior"))
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r => r.Props.Position.Should().StartWith("Senior"));
    }

    [Fact]
    public async Task Where_BoolField_True()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.IsRemote.Should().BeTrue());
    }

    [Fact]
    public async Task Where_NestedField_Filters()
    {
        await EnsureSeededAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.City == "Berlin")
            .ToListAsync();

        results.Should().AllSatisfy(r => r.Props.HomeAddress!.City.Should().Be("Berlin"));
    }

    [Fact]
    public async Task Where_Nullable_IsNull()
    {
        var obj = new RedbObject<EmployeeProps>
        {
            name = "NullableTest",
            Props = new EmployeeProps
            {
                FirstName = "Null",
                LastName = "Test",
                Position = "Temp",
                Department = "QA",
                Age = 20,
                Salary = 30000m,
                HireDate = DateTime.UtcNow,
                IsRemote = false,
                Rating = null
            }
        };
        obj.id = await Redb.SaveAsync(obj);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Rating == null)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().Contain(r => r.id == obj.id);
    }

    [Fact]
    public async Task CountAsync_ReturnsCorrectCount()
    {
        await EnsureSeededAsync();

        var count = await Redb.Query<EmployeeProps>().CountAsync();
        count.Should().BeGreaterThanOrEqualTo(20);
    }

    [Fact]
    public async Task AnyAsync_WithMatchingCondition_ReturnsTrue()
    {
        await EnsureSeededAsync();

        var any = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering")
            .AnyAsync();

        any.Should().BeTrue();
    }

    [Fact]
    public async Task FirstOrDefaultAsync_ReturnsFirstMatch()
    {
        await EnsureSeededAsync();

        var first = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering")
            .FirstOrDefaultAsync();

        first.Should().NotBeNull();
        first!.Props.Department.Should().Be("Engineering");
    }

    // ===== WhereRedb: base field ValueString tests =====

    private async Task<List<long>> SeedWithValueString()
    {
        var ids = new List<long>();
        var entries = new[]
        {
            ("alpha-bravo-charlie", "First"),
            ("delta-echo-foxtrot", "Second"),
            ("golf-hotel-india",   "Third"),
            ("alpha-juliet-kilo",  "Fourth"),
            ("lima-echo-mike",     "Fifth"),
        };

        foreach (var (vs, fn) in entries)
        {
            var obj = new RedbObject<EmployeeProps>
            {
                value_string = vs,
                name = $"VS_{fn}",
                Props = new EmployeeProps
                {
                    FirstName = fn,
                    LastName = "VSTest",
                    Position = "Developer",
                    Department = "Engineering",
                    Age = 30,
                    Salary = 60000m,
                    HireDate = DateTime.UtcNow,
                    IsRemote = false
                }
            };
            obj.id = await Redb.SaveAsync(obj);
            ids.Add(obj.id);
        }

        return ids;
    }

    [Fact]
    public async Task WhereRedb_ValueString_Contains_Filters()
    {
        var ids = await SeedWithValueString();

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.value_string != null && r.value_string.Contains("echo"));
        // "delta-echo-foxtrot" and "lima-echo-mike"
        results.Should().HaveCountGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task WhereRedb_ValueString_Equal_Filters()
    {
        var ids = await SeedWithValueString();

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString == "alpha-bravo-charlie")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.value_string == "alpha-bravo-charlie");
    }

    [Fact]
    public async Task WhereRedb_ValueString_StartsWith_Filters()
    {
        var ids = await SeedWithValueString();

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString!.StartsWith("alpha"))
            .ToListAsync();

        results.Should().HaveCountGreaterThanOrEqualTo(2);
        results.Should().OnlyContain(r => r.value_string != null && r.value_string.StartsWith("alpha"));
    }

    [Fact]
    public async Task WhereRedb_ValueString_CombinedWithPropsFilter()
    {
        var ids = await SeedWithValueString();

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString!.Contains("alpha"))
            .Where(e => e.LastName == "VSTest")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r =>
            r.value_string != null && r.value_string.Contains("alpha") &&
            r.Props.LastName == "VSTest");
    }

    [Fact]
    public async Task WhereRedb_ValueString_Contains_ThenMultipleWhereProps()
    {
        var ids = await SeedWithValueString();

        // Reproduces: WhereRedb(ValueString.Contains) + multiple .Where(props) + OrderBy + Count
        // Bug: Pro query provider puts _value_string in PVT CTE instead of o._value_string
        var query = Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .Where(e => e.Department == "Engineering")
            .Where(e => e.Age == 30)
            .Where(e => e.IsRemote == false)
            .Where(e => e.HireDate >= new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc))
            .OrderBy(e => e.FirstName);

        // Count must also work (separate SQL path)
        var count = await query.CountAsync();
        count.Should().BeGreaterThanOrEqualTo(0);

        // ToList
        var results = await query.ToListAsync();

        results.Should().OnlyContain(r =>
            r.value_string != null && r.value_string.Contains("echo") &&
            r.Props.Department == "Engineering" &&
            r.Props.Age == 30);
    }

    [Fact]
    public async Task WhereRedb_ValueString_Contains_ThenWhereProps_CountAsync()
    {
        var ids = await SeedWithValueString();

        // Minimal repro: WhereRedb(Contains) + one Where(props) → CountAsync
        var count = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueString!.Contains("echo"))
            .Where(e => e.Salary > 0m)
            .CountAsync();

        count.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task WhereRedb_Name_Contains_Filters()
    {
        var ids = await SeedWithValueString();

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Name.Contains("VS_"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.name != null && r.name.Contains("VS_"));
    }

    // ===== WhereRedb: nullable base field .Value / .HasValue tests =====

    private bool _nullableSeeded;
    private long _parentId, _childId, _orphanId;

    private async Task EnsureNullableSeededAsync()
    {
        if (_nullableSeeded) return;
        
        // Create parent
        var parent = new RedbObject<EmployeeProps>
        {
            name = "NullableTest_Parent",
            value_long = 42,
            key = 100,
            Props = new EmployeeProps
            {
                FirstName = "Parent",
                LastName = "NullableTest",
                Position = "Manager",
                Department = "Engineering",
                Age = 40,
                Salary = 100000m,
                HireDate = DateTime.UtcNow,
                IsRemote = false
            }
        };
        parent.id = await Redb.SaveAsync(parent);

        // Create child with ParentId set
        var child = new RedbObject<EmployeeProps>
        {
            name = "NullableTest_Child",
            parent_id = parent.id,
            value_long = 99,
            key = 200,
            Props = new EmployeeProps
            {
                FirstName = "Child",
                LastName = "NullableTest",
                Position = "Developer",
                Department = "Engineering",
                Age = 25,
                Salary = 60000m,
                HireDate = DateTime.UtcNow,
                IsRemote = true
            }
        };
        child.id = await Redb.SaveAsync(child);

        // Create orphan (no parent, no value_long, no key)
        var orphan = new RedbObject<EmployeeProps>
        {
            name = "NullableTest_Orphan",
            Props = new EmployeeProps
            {
                FirstName = "Orphan",
                LastName = "NullableTest",
                Position = "Intern",
                Department = "Engineering",
                Age = 20,
                Salary = 30000m,
                HireDate = DateTime.UtcNow,
                IsRemote = false
            }
        };
        orphan.id = await Redb.SaveAsync(orphan);

        _parentId = parent.id;
        _childId = child.id;
        _orphanId = orphan.id;
        _nullableSeeded = true;
    }

    [Fact]
    public async Task WhereRedb_ParentId_Value_Equal_Filters()
    {
        await EnsureNullableSeededAsync();

        // BUG REPRO: o.ParentId.Value used to resolve to o._id instead of o._id_parent
        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId.HasValue && o.ParentId.Value == _parentId)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.parent_id == _parentId);
        results.Should().Contain(r => r.id == _childId);
    }

    [Fact]
    public async Task WhereRedb_ParentId_HasValue_Filters()
    {
        await EnsureNullableSeededAsync();

        var withParent = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId.HasValue)
            .Where(e => e.LastName == "NullableTest")
            .ToListAsync();

        withParent.Should().NotBeEmpty();
        withParent.Should().OnlyContain(r => r.parent_id.HasValue);
        withParent.Should().Contain(r => r.id == _childId);
        withParent.Should().NotContain(r => r.id == _orphanId);
    }

    [Fact]
    public async Task WhereRedb_ParentId_IsNull_Filters()
    {
        await EnsureNullableSeededAsync();

        var withoutParent = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId == null)
            .Where(e => e.LastName == "NullableTest")
            .ToListAsync();

        withoutParent.Should().NotBeEmpty();
        withoutParent.Should().OnlyContain(r => !r.parent_id.HasValue);
        withoutParent.Should().Contain(r => r.id == _parentId);
        withoutParent.Should().Contain(r => r.id == _orphanId);
    }

    [Fact]
    public async Task WhereRedb_ValueLong_Value_GreaterThan_Filters()
    {
        await EnsureNullableSeededAsync();

        // parent.value_long = 42, child.value_long = 99, orphan = null
        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ValueLong.HasValue && o.ValueLong.Value > 50)
            .Where(e => e.LastName == "NullableTest")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.value_long.HasValue && r.value_long.Value > 50);
        results.Should().Contain(r => r.id == _childId);    // value_long = 99
        results.Should().NotContain(r => r.id == _parentId); // value_long = 42
    }

    [Fact]
    public async Task WhereRedb_Key_Value_Equal_Filters()
    {
        await EnsureNullableSeededAsync();

        // parent.key = 100, child.key = 200, orphan = null
        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Key.HasValue && o.Key.Value == 200)
            .Where(e => e.LastName == "NullableTest")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.key == 200);
        results.Should().Contain(r => r.id == _childId);
    }

    [Fact]
    public async Task WhereRedb_NullableValue_CombinedWithProps_Filters()
    {
        await EnsureNullableSeededAsync();

        // Combines nullable .Value base field with props filter
        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId.Value == _parentId)
            .Where(e => e.Department == "Engineering" && e.IsRemote)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.parent_id == _parentId);
    }

    // ===== Pushdown regression: base predicate + props filter must produce correct results =====
    // After perf fix base WhereRedb on _id_parent is pushed INTO the inner _objects subquery
    // of the PVT CTE (no longer applied as outer WHERE after PVT aggregation).
    // These tests validate functional equivalence of the new SQL shape.

    [Fact]
    public async Task WhereRedb_ParentId_In_Array_Combined_With_Props_Filter()
    {
        await EnsureNullableSeededAsync();

        // Reproduces the original 80x perf-bug scenario:
        //   WhereRedb(o => parents.Contains(o.ParentId.Value)) + Where(props)
        // Compiles to `_id_parent = ANY(@p)` (Postgres) or `_id_parent IN (...)` (MSSql).
        // Must be pushed inside the (SELECT _id FROM _objects WHERE _id_scheme = X AND ...) subquery.
        var parentIds = new[] { _parentId };

        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId.HasValue && parentIds.Contains(o.ParentId.Value))
            .Where(e => e.Department == "Engineering")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.parent_id == _parentId);
        results.Should().OnlyContain(r => r.Props.Department == "Engineering");
        results.Should().Contain(r => r.id == _childId);
    }

    [Fact]
    public async Task WhereRedb_ParentId_In_Array_Combined_With_Props_CountAsync_Matches_ToList()
    {
        await EnsureNullableSeededAsync();

        var parentIds = new[] { _parentId };

        var query = Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId.HasValue && parentIds.Contains(o.ParentId.Value))
            .Where(e => e.Department == "Engineering");

        var count = await query.CountAsync();
        var list = await query.ToListAsync();

        count.Should().Be(list.Count);
        count.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task WhereRedb_ParentId_Equal_PropsFilter_OrderByProps_TopN()
    {
        await EnsureNullableSeededAsync();

        // Validates pushdown does not break ORDER BY / TOP combined with mixed filters.
        var results = await Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.ParentId.HasValue && o.ParentId.Value == _parentId)
            .Where(e => e.Salary > 0m)
            .OrderBy(e => e.Salary)
            .Take(10)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().OnlyContain(r => r.parent_id == _parentId);
        // Ordering preserved
        for (int i = 1; i < results.Count; i++)
        {
            results[i].Props.Salary.Should().BeGreaterThanOrEqualTo(results[i - 1].Props.Salary);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  String equality and membership on values longer than the string index holds
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task Where_StringEquality_FindsValuesLongerThanTheIndexBound()
    {
        // PostgreSQL writes a string equality with the byte bound of its partial string index
        // (octet_length < 2000) so the planner can use it. The bound is added only for a short
        // value: a long one must still be found, through equality, membership and Any alike.
        // 1500 Cyrillic characters are 3000 bytes.
        var tag = Guid.NewGuid().ToString("N");
        var longText = tag + new string('ж', 1500);
        var shortText = "short-" + tag;

        var longObj = TestDataFactory.CreateSimple("long-string-" + tag);
        longObj.Props.Description = longText;
        var shortObj = TestDataFactory.CreateSimple("short-string-" + tag);
        shortObj.Props.Description = shortText;
        var ids = new[] { await Redb.SaveAsync(longObj), await Redb.SaveAsync(shortObj) };

        try
        {
            (await Redb.Query<SimpleProps>().Where(p => p.Description == longText).CountAsync())
                .Should().Be(1, "a value longer than the index bound is not covered by it and must still match");
            (await Redb.Query<SimpleProps>().AnyAsync(p => p.Description == longText)).Should().BeTrue();
            (await Redb.Query<SimpleProps>().WhereIn(p => p.Description, new[] { longText, shortText }).CountAsync())
                .Should().Be(2, "membership with a long value among short ones takes no bound");
            (await Redb.Query<SimpleProps>().Where(p => p.Description == shortText).CountAsync())
                .Should().Be(1);
            (await Redb.Query<SimpleProps>().WhereIn(p => p.Description, new[] { shortText, "absent-" + tag }).CountAsync())
                .Should().Be(1);
            (await Redb.Query<SimpleProps>().Where(p => p.Description != shortText && p.Description!.StartsWith(tag)).CountAsync())
                .Should().Be(1, "NOT of a bounded equality keeps the long value");
        }
        finally
        {
            await Redb.DeleteAsync(ids);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  AllAsync: the answer of every form, absent values included
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task AllAsync_CounterexampleSemantics_AbsentValueFails()
    {
        // PostgreSQL Pro answers AllAsync as "no counterexample" (NOT EXISTS of an object for which the predicate is
        // not TRUE), the other providers by two counts. Both must agree: an object without a value for the field
        // fails the check, an empty set passes it, and Take/Skip do not narrow it.
        var tag = "all-" + Guid.NewGuid().ToString("N");
        var objects = new[]
        {
            (Price: 10m, Active: true, Description: (string?)"x"),
            (Price: 20m, Active: true, Description: (string?)null),
            (Price: 30m, Active: false, Description: (string?)"y"),
        }.Select((d, i) =>
        {
            var o = TestDataFactory.CreateSimple($"{tag}-{i}", d.Price);
            o.Props.IsActive = d.Active;
            o.Props.Description = d.Description;
            return o;
        }).ToList();
        var ids = new List<long>();
        foreach (var o in objects)
            ids.Add(await Redb.SaveAsync(o));

        try
        {
            var set = Redb.Query<SimpleProps>().Where(p => p.Title.StartsWith(tag));

            (await set.AllAsync(p => p.Price > 5m)).Should().BeTrue();
            (await set.AllAsync(p => p.Price > 10m)).Should().BeFalse("one object has Price 10");
            (await set.AllAsync(p => p.Description != null)).Should().BeFalse("one object has no Description");
            (await set.AllAsync(p => p.Description == "x" || p.Description == "y"))
                .Should().BeFalse("the object without a Description is a counterexample of a condition on it");
            (await set.AllAsync(p => p.Price >= 10m && p.Price <= 30m)).Should().BeTrue();
            (await set.AllAsync(p => p.Price >= 10m && p.IsActive)).Should().BeFalse("one object is not active");
            (await set.Where(p => p.IsActive).AllAsync(p => p.Price < 25m)).Should().BeTrue();
            (await set.Take(1).AllAsync(p => p.Price > 10m)).Should().BeFalse("Take does not narrow the checked set");

            var noneTitle = tag + "-none";
            var empty = Redb.Query<SimpleProps>().Where(p => p.Title == noneTitle);
            (await empty.AllAsync(p => p.Price > 100m)).Should().BeTrue("every element of an empty set satisfies anything");
        }
        finally
        {
            await Redb.DeleteAsync(ids);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Soft-deleted objects stay out of counts and checks until the purge
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task CountAnyAll_ByValue_LeaveOutSoftDeletedObjectsBeforeThePurge()
    {
        // PostgreSQL Pro reads a count or an existence check filtered by Props values off the index of values and
        // subtracts the trash: the soft delete moves an object to the trash scheme and leaves its values until the
        // purge, so they still match the filter. The other providers check the scheme of every object.
        var tag = "trash-" + Guid.NewGuid().ToString("N");
        var objects = new[] { 10m, 20m, 30m }.Select((price, i) =>
        {
            var o = TestDataFactory.CreateSimple($"{tag}-{i}", price);
            o.Props.Description = tag;
            return o;
        }).ToList();
        var ids = new List<long>();
        foreach (var o in objects)
            ids.Add(await Redb.SaveAsync(o));

        try
        {
            await Redb.SoftDeleteAsync(new[] { ids[1] });
            (await Redb.Context.ExecuteScalarAsync<long>(
                    $"SELECT COUNT(*) FROM _objects WHERE _id = {ids[1]} AND _id_scheme = -10"))
                .Should().Be(1, "the object waits in the trash with its values, which is the case under test");

            (await Redb.Query<SimpleProps>().Where(p => p.Description == tag).CountAsync())
                .Should().Be(2, "the soft-deleted object is not counted");
            (await Redb.Query<SimpleProps>().AnyAsync(p => p.Description == tag && p.Price == 20m))
                .Should().BeFalse("the soft-deleted object is not found");
            (await Redb.Query<SimpleProps>().Where(p => p.Description == tag).AllAsync(p => p.Price != 20m))
                .Should().BeTrue("the soft-deleted object is not a counterexample");
            (await Redb.Query<SimpleProps>().Where(p => p.Description == tag).SumAsync(p => p.Price))
                .Should().Be(40m, "the soft-deleted object is not summed");
            var groups = await Redb.Query<SimpleProps>().Where(p => p.Description == tag)
                .GroupBy(p => p.Description)
                .SelectAsync(g => new { Key = g.Key, Count = Agg.Count(g), Total = Agg.Sum(g, p => p.Price) });
            groups.Should().ContainSingle().Which.Count.Should().Be(2, "the soft-deleted object is not grouped");
            groups[0].Total.Should().Be(40m);
            var page = await Redb.Query<SimpleProps>().Where(p => p.Description == tag).OrderBy(p => p.Price).Take(2).ToListAsync();
            page.Select(o => o.Props.Price).Should().Equal(new[] { 10m, 30m },
                "the soft-deleted object takes no place in the page: the page is still full");
        }
        finally
        {
            await Redb.DeleteAsync(ids);
        }
    }

    [Fact]
    public async Task Count_NullCheckAlone_AddsUpAndLeavesOutSoftDeletedObjects()
    {
        // PostgreSQL Pro counts Field == null alone as the objects of the scheme less the rows with a value outside
        // the trash, and Field != null off the rows of the field it drives from. Both answers must add up to the
        // objects of the scheme, and a soft-deleted object leaves the count it was in, whether it had a value or not.
        var tag = "nullcount-" + Guid.NewGuid().ToString("N");
        var withValue = TestDataFactory.CreateSimple($"{tag}-value");
        withValue.Props.OptionalNumber = 42;
        var withoutValue = TestDataFactory.CreateSimple($"{tag}-none");
        withoutValue.Props.OptionalNumber = null;
        var ids = new List<long> { await Redb.SaveAsync(withValue), await Redb.SaveAsync(withoutValue) };

        async Task<(int Nulls, int Values, int All)> CountsAsync() => (
            await Redb.Query<SimpleProps>().Where(p => p.OptionalNumber == null).CountAsync(),
            await Redb.Query<SimpleProps>().Where(p => p.OptionalNumber != null).CountAsync(),
            await Redb.Query<SimpleProps>().CountAsync());

        try
        {
            var before = await CountsAsync();
            (before.Nulls + before.Values).Should().Be(before.All, "every object either has the value or has not");

            await Redb.SoftDeleteAsync(new[] { ids[1] });
            var afterNone = await CountsAsync();
            afterNone.Nulls.Should().Be(before.Nulls - 1, "the soft-deleted object without a value is not counted");
            afterNone.Values.Should().Be(before.Values);

            await Redb.SoftDeleteAsync(new[] { ids[0] });
            var afterValue = await CountsAsync();
            afterValue.Nulls.Should().Be(afterNone.Nulls, "a soft-deleted object with a value never was among the nulls");
            afterValue.Values.Should().Be(afterNone.Values - 1, "the soft-deleted object with a value is not counted");
            (afterValue.Nulls + afterValue.Values).Should().Be(afterValue.All);
        }
        finally
        {
            await Redb.DeleteAsync(ids);
        }
    }

    [Fact]
    public async Task Page_NullCheckInTheTopLevelAnd_ReturnsOnlyTheObjectsWithoutAValue()
    {
        // SQL Server Pro checks Field == null in the top-level AND with NOT EXISTS instead of joining the field and
        // testing its column; the page must hold exactly the objects without a value, a field of a nested class too.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var full = TestDataFactory.CreateEmployee(9101, firstName: $"absent-{tag}-full");
        full.Props.EmployeeCode = "EMP-FULL";
        var empty = TestDataFactory.CreateEmployee(9102, firstName: $"absent-{tag}-empty");
        empty.Props.EmployeeCode = null;
        empty.Props.HomeAddress = null;
        var ids = new List<long> { await Redb.SaveAsync(full), await Redb.SaveAsync(empty) };
        var prefix = $"absent-{tag}-";

        try
        {
            var byCode = await Redb.Query<EmployeeProps>()
                .Where(e => e.FirstName.StartsWith(prefix) && e.EmployeeCode == null)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            byCode.Select(o => o.Id).Should().Equal(new[] { ids[1] });
            byCode[0].Props.EmployeeCode.Should().BeNull();

            var byCity = await Redb.Query<EmployeeProps>()
                .Where(e => e.FirstName.StartsWith(prefix) && e.HomeAddress!.City == null)
                .OrderByRedb(o => o.Id).Take(10).ToListAsync();
            byCity.Select(o => o.Id).Should().Equal(new[] { ids[1] });

            (await Redb.Query<EmployeeProps>().Where(e => e.FirstName.StartsWith(prefix) && e.EmployeeCode == null).CountAsync())
                .Should().Be(1);
            (await Redb.Query<EmployeeProps>().Where(e => e.FirstName.StartsWith(prefix) && e.EmployeeCode != null).CountAsync())
                .Should().Be(1);
        }
        finally
        {
            await Redb.DeleteAsync(ids);
        }
    }

    [Fact]
    public async Task Count_NullCheckAlone_FieldOfANestedClass()
    {
        // A field of a nested class hangs off its parent row (_array_parent_id set): counting Field == null alone must
        // see those rows as values. An employee without a home address has no City; one with an address has it.
        var tag = Guid.NewGuid().ToString("N")[..8];
        var withAddress = TestDataFactory.CreateEmployee(9001, firstName: $"nested-{tag}-a");
        var withoutAddress = TestDataFactory.CreateEmployee(9002, firstName: $"nested-{tag}-b");
        withoutAddress.Props.HomeAddress = null;
        var ids = new List<long> { await Redb.SaveAsync(withAddress), await Redb.SaveAsync(withoutAddress) };

        try
        {
            var nulls = await Redb.Query<EmployeeProps>().Where(e => e.HomeAddress!.City == null).CountAsync();
            var values = await Redb.Query<EmployeeProps>().Where(e => e.HomeAddress!.City != null).CountAsync();
            var all = await Redb.Query<EmployeeProps>().CountAsync();
            var nullsTheLongWay = await Redb.Query<EmployeeProps>().Where(e => e.HomeAddress!.City == null && e.Age >= 0).CountAsync();

            nulls.Should().Be(nullsTheLongWay, "the count of the null check alone and the count with a second condition agree");
            nulls.Should().BeGreaterThan(0, "the employee without an address has no City");
            (nulls + values).Should().Be(all);
        }
        finally
        {
            await Redb.DeleteAsync(ids);
        }
    }
}
