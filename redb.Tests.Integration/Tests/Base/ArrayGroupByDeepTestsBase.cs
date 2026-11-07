using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Deep-coverage integration tests for <c>GroupByArray</c>.
///
/// Complements <see cref="GroupByHavingTestsBase"/> (HAVING basics) and the
/// E142* slice of <see cref="BugRegressionTestsBase"/> by exercising:
///   * composite array keys (anonymous { Type, IsVerified });
///   * outer Where combined with nested AND/OR;
///   * outer Where with string operations (Contains / StartsWith) on parent props;
///   * outer WhereIn on base.id;
///   * boolean array keys.
///
/// Targets the same dialect set as <see cref="GroupByHavingTestsBase"/>:
/// all six classes (PostgreSQL, SQL Server, SQLite; free and Pro).
/// </summary>
public abstract class ArrayGroupByDeepTestsBase
{
    protected readonly IRedbService Redb;

    protected ArrayGroupByDeepTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    // What a group of an array unnest counts: one row per contact of that type, over the employees the outer Where
    // matched. The database is shared by the whole collection, and other tests store employees of the same
    // departments with no contacts at all, so "one row per matched employee" held only while they ran later.
    private static int ContactsOfType(IEnumerable<RedbObject<EmployeeProps>> employees, string? type)
        => employees.Sum(e => e.Props.Contacts?.Count(c => c.Type == type) ?? 0);

    // ──────────────────────────────────────────────────────────────────
    //  Composite array key (anonymous { Type, IsVerified })
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_CompositeKey_TypeAndVerified_FourGroupsMax()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .SelectAsync(g => new
            {
                g.Key.Type,
                g.Key.IsVerified,
                Count = Agg.Count(g)
            });

        // Seed: every employee has exactly two contacts —
        //   { Type=email, IsVerified=true } and { Type=phone, IsVerified=false }.
        // So there must be exactly 2 distinct composite groups, each non-empty.
        results.Should().HaveCount(2);
        results.Should().Contain(r => r.Type == "email" && r.IsVerified);
        results.Should().Contain(r => r.Type == "phone" && !r.IsVerified);
        results.Should().AllSatisfy(r => r.Count.Should().BeGreaterThan(0));
    }

    [Fact]
    public async Task ArrayGroupBy_CompositeKey_Having_PositiveThreshold_Survives()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .Having(g => Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                g.Key.Type,
                g.Key.IsVerified,
                C = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Distinct().Should().BeEquivalentTo(new[] { "email", "phone" });
    }

    [Fact]
    public async Task ArrayGroupBy_CompositeKey_Having_ImpossibleThreshold_DropsAll()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .Having(g => Agg.Count(g) > 1_000_000)
            .SelectAsync(g => new { g.Key.Type, C = Agg.Count(g) });

        results.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where with nested AND/OR over parent props
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_NestedAndOr_Combined()
    {
        await SeedAsync();

        // Seed has 20 employees, departments cycled through 5 deps, IsRemote = (i%2==0).
        // (Department == "Engineering" && IsRemote) OR (Department == "Sales" && !IsRemote)
        // produces a non-empty subset; assert non-empty groups + each group count > 0.
        var matched = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Department == "Engineering" && e.IsRemote)
                     || (e.Department == "Sales" && !e.IsRemote))
            .ToListAsync();
        matched.Should().NotBeEmpty("seed must produce some matches for the compound filter");

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Department == "Engineering" && e.IsRemote)
                     || (e.Department == "Sales" && !e.IsRemote))
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.C.Should().Be(ContactsOfType(matched, r.Type),
            "every contact type must produce exactly one row per such contact of a matched employee after array unnest"));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where with string ops (Contains / StartsWith) on parent props
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_StringContains_OnDepartment()
    {
        await SeedAsync();

        var matched = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.Contains("ing"))   // Engineering, Marketing
            .ToListAsync();
        matched.Should().NotBeEmpty();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.Contains("ing"))
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Should().BeEquivalentTo(new[] { "email", "phone" });
        results.Should().AllSatisfy(r => r.C.Should().Be(ContactsOfType(matched, r.Type)));
    }

    [Fact]
    public async Task ArrayGroupBy_OuterWhere_StringStartsWith_OnFirstName()
    {
        await SeedAsync();

        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.StartsWith("Employee00"))
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.StartsWith("Employee00"))
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.C.Should().Be(matched));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where narrows to a single employee — exact contact-type cardinality
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_UniqueFirstName_ExactCardinality()
    {
        await SeedAsync();

        // Seeders are cumulative across tests in the suite, so "Employee001"
        // may exist N≥1 times. Compute expected cardinality from data.
        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "Employee001")
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "Employee001")
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().HaveCount(2, "each matched employee has exactly 2 distinct contact types (email + phone)");
        results.Should().AllSatisfy(r => r.C.Should().Be(matched));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Boolean array key (IsVerified on its own)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_BoolKey_IsVerified_TwoGroups()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.IsVerified)
            .SelectAsync(g => new { Verified = g.Key, C = Agg.Count(g) });

        // Seed has both verified=true (email) and verified=false (phone) — 2 distinct.
        results.Should().HaveCount(2);
        results.Select(r => r.Verified).Should().BeEquivalentTo(new[] { true, false });
        results.Should().AllSatisfy(r => r.C.Should().BeGreaterThan(0));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Empty / null arrays — outer filter must exclude employees w/o array
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_ContactsNotNull_StillProducesGroups()
    {
        await SeedAsync();

        // All seeded employees have Contacts; this is essentially a smoke regression
        // for the IS NOT NULL outer filter, which used to be rejected by free PG.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Contacts != null)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Should().BeEquivalentTo(new[] { "email", "phone" });
    }

    // ══════════════════════════════════════════════════════════════════
    //  Forms of docs/array-groupby/PLAN.md. Each test writes its own objects under a fresh marker and compares
    //  the grouping with the same LINQ over those objects in memory.
    // ══════════════════════════════════════════════════════════════════

    private static string NewMarker() => "ag" + Guid.NewGuid().ToString("N")[..10];

    /// <summary>Employees under <paramref name="marker"/> with varied inner arrays: Metadata on both contacts or one,
    /// amenities of two shapes, floors 1..23.</summary>
    private async Task<List<RedbObject<EmployeeProps>>> SeedVariedAsync(string marker, int count = 12)
    {
        var list = new List<RedbObject<EmployeeProps>>();
        for (var i = 0; i < count; i++)
        {
            var e = TestDataFactory.CreateEmployee(i, firstName: $"{marker}-{i:D2}");
            e.Props.Contacts![0].Metadata = i % 3 == 0
                ? [new TestContactDetail { Label = "work", Value = "a" }, new TestContactDetail { Label = "home", Value = "b" }]
                : [new TestContactDetail { Label = "work", Value = "c" }];
            e.Props.Contacts[1].Metadata = i % 2 == 0 ? [new TestContactDetail { Label = "night", Value = "d" }] : null;
            e.Props.HomeAddress!.Building!.Amenities = i % 2 == 0 ? ["Parking", "Gym"] : ["Gym", "Pool", "Cafe"];
            e.Props.HomeAddress.Building.Floor = 1 + i * 2;
            await Redb.SaveAsync(e);
            list.Add(e);
        }
        return list;
    }

    private redb.Core.Query.IRedbQueryable<EmployeeProps> Own(string marker)
        => Redb.Query<EmployeeProps>().Where(e => e.FirstName.StartsWith(marker));

    // ---- F1: inner array through SelectMany --------------------------------------------------------------------

    [Fact]
    public async Task Nested_SelectMany_GroupsTheElementsOfTheInnerArray()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.Contacts!.SelectMany(c => c.Metadata!), m => m.Label)
            .SelectAsync(g => new { Label = g.Key, Count = Agg.Count(g) });

        var expected = seeded.SelectMany(e => e.Props.Contacts!).SelectMany(c => c.Metadata ?? [])
            .GroupBy(m => m.Label).ToDictionary(g => g.Key, g => g.Count());
        rows.ToDictionary(r => r.Label, r => r.Count).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void Nested_SelectManyWithAResultSelector_IsRefused()
    {
        var build = () => Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!.SelectMany(c => c.Metadata!, (c, m) => m), m => m.Label);

        build.Should().Throw<NotSupportedException>().WithMessage("*result selector*");
    }

    [Fact]
    public void ArrayPath_ThroughADictionary_IsRefused()
    {
        var build = () => Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.OfficeLocations!.Values, a => a.City);

        build.Should().Throw<NotSupportedException>();
    }

    // ---- F2: arrays of simple values ---------------------------------------------------------------------------

    [Fact]
    public async Task ScalarArray_GroupsByTheElementItself()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.Skills!, s => s)
            .SelectAsync(g => new { Skill = g.Key, Count = Agg.Count(g) });

        var expected = seeded.SelectMany(e => e.Props.Skills ?? []).GroupBy(s => s).ToDictionary(g => g.Key, g => g.Count());
        rows.ToDictionary(r => r.Skill, r => r.Count).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task ScalarArray_ThroughClasses_GroupsTheInnerArray()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.HomeAddress!.Building!.Amenities!, a => a)
            .Having(g => Agg.Count(g) > 3)
            .SelectAsync(g => new { Amenity = g.Key, Count = Agg.Count(g) });

        var expected = seeded.SelectMany(e => e.Props.HomeAddress!.Building!.Amenities!)
            .GroupBy(a => a).Where(g => g.Count() > 3).ToDictionary(g => g.Key, g => g.Count());
        rows.ToDictionary(r => r.Amenity, r => r.Count).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task ScalarArray_AggregatesOverTheElement()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.SkillLevels!, l => l)
            .SelectAsync(g => new { Level = g.Key, Count = Agg.Count(g), Sum = Agg.Sum(g, x => x) });

        var expected = seeded.SelectMany(e => e.Props.SkillLevels ?? []).GroupBy(l => l)
            .ToDictionary(g => g.Key, g => (Count: g.Count(), Sum: g.Sum()));
        rows.Should().HaveCount(expected.Count);
        foreach (var r in rows)
        {
            r.Count.Should().Be(expected[r.Level].Count);
            r.Sum.Should().Be(expected[r.Level].Sum);
        }
    }

    [Fact]
    public void ArrayOfListItems_IsRefusedByName()
    {
        var build = () => Redb.Query<ArrayRefusedElementsProps>().GroupByArray(e => e.Roles!, r => r.Id);
        build.Should().Throw<NotSupportedException>().WithMessage("*RedbListItem*");
    }

    [Fact]
    public void ArrayOfObjectReferences_IsRefusedByName()
    {
        var build = () => Redb.Query<ArrayRefusedElementsProps>().GroupByArray(e => e.Links!, r => r.Id);
        build.Should().Throw<NotSupportedException>().WithMessage("*object references*");
    }

    // ---- A15: one test per value column -----------------------------------------------------------------------

    private async Task<(string Marker, List<ArrayValuesProps> Seeded)> SeedValuesAsync()
    {
        await Redb.SyncSchemeAsync<ArrayValuesProps>();
        var marker = NewMarker();
        var seeded = new List<ArrayValuesProps>();
        var guids = new[] { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222") };
        for (var i = 0; i < 6; i++)
        {
            var p = new ArrayValuesProps
            {
                Marker = marker,
                Words = i % 2 == 0 ? ["alpha", "beta"] : ["beta", "gamma", "beta"],
                Longs = [i % 3, 10, 10L * (i % 2)],
                Doubles = [1.5, i % 2 == 0 ? 2.25 : 1.5],
                Decimals = [10.10m, i % 3 == 0 ? 20.20m : 10.10m],
                Flags = [true, i % 2 == 0],
                Dates = [new DateTime(2026, 1, 1 + i % 2, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc)],
                Ids = [guids[i % 2], guids[0]],
                Items =
                [
                    new ArrayItem { Kind = i % 3 == 0 ? null : "box", Size = i, Active = i % 2 == 0, Place = i % 2 == 0 ? new ArrayItemPlace { City = "Riga" } : null },
                    new ArrayItem { Kind = "bag", Size = 10 + i, Active = true, Place = new ArrayItemPlace { City = i % 3 == 0 ? "Oslo" : "Riga" } }
                ]
            };
            await Redb.SaveAsync(new RedbObject<ArrayValuesProps> { name = $"{marker}-{i}", Props = p });
            seeded.Add(p);
        }
        return (marker, seeded);
    }

    private redb.Core.Query.IRedbQueryable<ArrayValuesProps> OwnValues(string marker)
        => Redb.Query<ArrayValuesProps>().Where(v => v.Marker == marker);

    [Fact]
    public async Task ValueColumn_String()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Words!, w => w).SelectAsync(g => new { K = g.Key, C = Agg.Count(g) });
        rows.ToDictionary(r => r.K, r => r.C).Should().BeEquivalentTo(
            seeded.SelectMany(s => s.Words!).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()));
    }

    [Fact]
    public async Task ValueColumn_Long()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Longs!, w => w)
            .SelectAsync(g => new { K = g.Key, C = Agg.Count(g), S = Agg.Sum(g, x => x) });
        var expected = seeded.SelectMany(s => s.Longs!).GroupBy(x => x).ToDictionary(g => g.Key, g => (g.Count(), g.Sum()));
        rows.ToDictionary(r => r.K, r => (r.C, r.S)).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task ValueColumn_Double()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Doubles!, w => w).SelectAsync(g => new { K = g.Key, C = Agg.Count(g) });
        rows.ToDictionary(r => r.K, r => r.C).Should().BeEquivalentTo(
            seeded.SelectMany(s => s.Doubles!).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()));
    }

    [Fact]
    public async Task ValueColumn_Numeric()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Decimals!, w => w)
            .SelectAsync(g => new { K = g.Key, C = Agg.Count(g), S = Agg.Sum(g, x => x) });
        var expected = seeded.SelectMany(s => s.Decimals!).GroupBy(x => x).ToDictionary(g => g.Key, g => (g.Count(), g.Sum()));
        rows.ToDictionary(r => r.K, r => (r.C, r.S)).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task ValueColumn_Boolean()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Flags!, w => w).SelectAsync(g => new { K = g.Key, C = Agg.Count(g) });
        rows.ToDictionary(r => r.K, r => r.C).Should().BeEquivalentTo(
            seeded.SelectMany(s => s.Flags!).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()));
    }

    [Fact]
    public async Task ValueColumn_DateTime()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Dates!, w => w).SelectAsync(g => new { K = g.Key, C = Agg.Count(g) });
        rows.ToDictionary(r => r.K, r => r.C).Should().BeEquivalentTo(
            seeded.SelectMany(s => s.Dates!).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()));
    }

    [Fact]
    public async Task ValueColumn_Guid()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Ids!, w => w).SelectAsync(g => new { K = g.Key, C = Agg.Count(g) });
        rows.ToDictionary(r => r.K, r => r.C).Should().BeEquivalentTo(
            seeded.SelectMany(s => s.Ids!).GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()));
    }

    // ---- A17: a null key, A9: a field of a class inside the element --------------------------------------------

    [Fact]
    public async Task NullKey_ComesBackAsItsOwnGroup()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Items!, i => i.Kind).SelectAsync(g => new { Kind = g.Key, C = Agg.Count(g) });

        var expected = seeded.SelectMany(s => s.Items!).GroupBy(i => i.Kind).ToList();
        rows.Should().HaveCount(expected.Count);
        rows.Should().ContainSingle(r => r.Kind == null).Which.C.Should().Be(expected.Single(g => g.Key == null).Count());
        foreach (var g in expected.Where(g => g.Key != null))
            rows.Single(r => r.Kind == g.Key).C.Should().Be(g.Count());
    }

    [Fact]
    public async Task KeyOnAFieldOfAClassInsideTheElement()
    {
        var (marker, seeded) = await SeedValuesAsync();
        var rows = await OwnValues(marker).GroupByArray(v => v.Items!, i => i.Place!.City)
            .SelectAsync(g => new { City = g.Key, C = Agg.Count(g), Size = Agg.Sum(g, i => i.Size) });

        var expected = seeded.SelectMany(s => s.Items!).GroupBy(i => i.Place?.City).ToList();
        rows.Should().HaveCount(expected.Count);
        foreach (var g in expected)
        {
            var row = rows.Single(r => r.City == g.Key);
            row.C.Should().Be(g.Count());
            row.Size.Should().Be(g.Sum(i => i.Size));
        }
    }

    [Fact]
    public void KeyOnAClass_IsRefused()
    {
        var build = () => Redb.Query<ArrayValuesProps>().GroupByArray(v => v.Items!, i => i.Place);
        build.Should().Throw<NotSupportedException>().WithMessage("*class*");
    }

    // ---- F4: conditional count -------------------------------------------------------------------------------

    [Fact]
    public async Task ConditionalCount_CountsTheElementsThatSatisfyIt()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                Type = g.Key,
                All = Agg.Count(g),
                Verified = Agg.Count(g, c => c.IsVerified),
                VerifiedEmail = Agg.Count(g, c => c.IsVerified && c.Type == "email"),
                WithValue = Agg.Count(g, c => c.Value != null)
            });

        var contacts = seeded.SelectMany(e => e.Props.Contacts!).ToList();
        rows.Should().HaveCount(contacts.Select(c => c.Type).Distinct().Count());
        foreach (var r in rows)
        {
            var group = contacts.Where(c => c.Type == r.Type).ToList();
            r.All.Should().Be(group.Count);
            r.Verified.Should().Be(group.Count(c => c.IsVerified));
            r.VerifiedEmail.Should().Be(group.Count(c => c.IsVerified && c.Type == "email"));
            r.WithValue.Should().Be(group.Count(c => c.Value != null));
        }
    }

    [Fact]
    public async Task ConditionalCount_InHaving()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g, c => !c.IsVerified) > 0)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        var expected = seeded.SelectMany(e => e.Props.Contacts!).GroupBy(c => c.Type)
            .Where(g => g.Count(c => !c.IsVerified) > 0).Select(g => g.Key);
        rows.Select(r => r.Type).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task ConditionalCount_WithAStringMethod_IsRefused()
    {
        var act = async () => await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g, c => c.Value.Contains("x")) });

        await act.Should().ThrowAsync<NotSupportedException>().WithMessage("*condition of Agg.Count*");
    }

    [Fact]
    public async Task ComputedAggregate_IsRefused()
    {
        var act = async () => await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, L = Agg.Sum(g, c => c.Value.Length) });

        await act.Should().ThrowAsync<NotSupportedException>();
    }

    // ---- F5: window over the groups, F3: SQL preview ------------------------------------------------------------

    [Fact]
    public async Task Window_OverTheGroups()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .GroupByArray(e => e.Contacts!.SelectMany(c => c.Metadata!), m => m.Label)
            .WithWindow(w => w.OrderBy(g => g.Key))
            .SelectAsync(g => new { Label = g.Key, Count = Agg.Count(g), Row = Win.RowNumber() });

        var expected = seeded.SelectMany(e => e.Props.Contacts!).SelectMany(c => c.Metadata ?? [])
            .GroupBy(m => m.Label).OrderBy(g => g.Key, StringComparer.Ordinal).Select((g, i) => (g.Key, Count: g.Count(), Row: i + 1L)).ToList();
        rows.OrderBy(r => r.Row).Select(r => (r.Label, r.Count, r.Row)).Should().Equal(expected);
    }

    [Fact]
    public async Task ToSqlString_ReturnsTheSqlThatRuns()
    {
        var sql = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Sales")
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .ToSqlStringAsync(g => new { Type = g.Key, C = Agg.Count(g, c => c.IsVerified) });

        sql.Should().Contain("GROUP BY").And.NotContain("not supported");
    }

    // ---- T4: the E142 shape --------------------------------------------------------------------------------------

    [Fact]
    public async Task E142Shape_DeepOuterWhere_CompositeKey_Count()
    {
        var marker = NewMarker();
        var seeded = await SeedVariedAsync(marker);

        var rows = await Own(marker)
            .Where(e => e.HomeAddress!.Building!.Floor > 10)
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .SelectAsync(g => new { g.Key.Type, g.Key.IsVerified, Count = Agg.Count(g) });

        var expected = seeded.Where(e => e.Props.HomeAddress!.Building!.Floor > 10)
            .SelectMany(e => e.Props.Contacts!)
            .GroupBy(c => new { c.Type, c.IsVerified })
            .ToDictionary(g => (g.Key.Type, g.Key.IsVerified), g => g.Count());
        rows.ToDictionary(r => (r.Type, r.IsVerified), r => r.Count).Should().BeEquivalentTo(expected);
    }
}
