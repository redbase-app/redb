using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// E2E-контракт серверных проекций `Query&lt;T&gt;().Select(...)` (ревью 2026-09-03, волна 1.1:
/// до этого набора у Select не было НИ ОДНОГО интеграционного теста). Контракт: проекция обязана
/// вернуть те же значения, что вернула бы полная загрузка с последующим вычислением лямбды в
/// памяти, — независимо от того, урезал ли провайдер Props на сервере (PG) или грузил целиком и
/// резал на клиенте (MSSQL/SQLite). Часть тестов написана red-before под известные дефекты ревью
/// (S-2 OrderBy-null, S-3 Distinct+Count, S-5 частичный разбор лямбды, S-6 референс в пути,
/// S-7 Agg внутри Select) и зеленеет по мере волн 2-3.
/// </summary>
public abstract class ProjectionTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected ProjectionTestsBase(IRedbService redb) => Redb = redb;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
        => await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name IN ('ProjectionProbe', 'ProjectionProbeChild'))");

    private async Task SyncAsync()
    {
        await Redb.SyncSchemeAsync<ProjectionProbeChildProps>();
        await Redb.SyncSchemeAsync<ProjectionProbeProps>();
    }

    private async Task<long> SeedAsync(ProjectionProbeProps props, string name = "probe")
        => await Redb.SaveAsync(new RedbObject<ProjectionProbeProps> { name = name, Props = props });

    // ============================================================
    // === База: значения доезжают как при полной загрузке ===
    // ============================================================

    [Fact]
    public async Task SimpleFields_AnonymousType()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "alpha", N = 7, D = 1.5m, Flag = true });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S, x.Props.N, x.Props.D })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].S.Should().Be("alpha");
        rs[0].N.Should().Be(7);
        rs[0].D.Should().Be(1.5m);
    }

    [Fact]
    public async Task BaseField_Id_RidesAlongProps()
    {
        await SyncAsync();
        var id = await SeedAsync(new ProjectionProbeProps { S = "with-id" });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Id, x.Props.S })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].Id.Should().Be(id, "у каждой спроецированной строки должен остаться id её объекта");
        rs[0].S.Should().Be("with-id");
    }

    [Fact]
    public async Task NestedClassField()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps
            { S = "n", Nested = new ProjectionProbeNested { City = "Kazan", Zip = 420000 } });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { City = x.Props.Nested!.City, x.Props.Nested!.Zip })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].City.Should().Be("Kazan");
        rs[0].Zip.Should().Be(420000);
    }

    [Fact]
    public async Task ArrayOfClass_InnerSelectToList()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps
        {
            S = "arr",
            Items =
            [
                new ProjectionProbeItem { Name = "a", Price = 10m },
                new ProjectionProbeItem { Name = "b", Price = 20m },
            ]
        });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { Names = x.Props.Items!.Select(i => i.Name).ToList() })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].Names.Should().Equal("a", "b");
    }

    [Fact]
    public async Task StringArrayField()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "tags", Tags = ["red", "green"] });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.Tags })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].Tags.Should().Equal("red", "green");
    }

    [Fact]
    public async Task ComputedMembers_BinaryAndConcat()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "x", N = 41 });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { Total = x.Props.N + 1, Label = x.Props.S + "!" })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].Total.Should().Be(42);
        rs[0].Label.Should().Be("x!");
    }

    [Fact]
    public async Task Conditional_And_Coalesce()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "yes", N = 1, Flag = true, D = null });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new
            {
                K = x.Props.Flag ? x.Props.S : "no",
                Dd = x.Props.D ?? -1m,
            })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].K.Should().Be("yes");
        rs[0].Dd.Should().Be(-1m);
    }

    [Fact]
    public async Task Dto_MemberInit_PlainAssignments()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "dto", N = 5 });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new ProjectionDto { A = x.Props.S, N = x.Props.N })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].A.Should().Be("dto");
        rs[0].N.Should().Be(5);
    }

    [Fact]
    public async Task WhereAfterSelect_FiltersProjectedRows()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "keep", N = 10 });
        await SeedAsync(new ProjectionProbeProps { S = "drop", N = 1 });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S, x.Props.N })
            .Where(r => r.N > 5)
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].S.Should().Be("keep");
    }

    [Fact]
    public async Task CountAsync_Plain_MatchesSource()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "c1" });
        await SeedAsync(new ProjectionProbeProps { S = "c2" });

        var count = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S })
            .CountAsync();

        count.Should().Be(2);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_ReturnsProjectedShape()
    {
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "first", N = 3 });

        var r = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S, x.Props.N })
            .FirstOrDefaultAsync();

        r.Should().NotBeNull();
        r!.S.Should().Be("first");
    }

    // ============================================================
    // === Red-before дефектов ревью (зеленеют волнами 2-3) ===
    // ============================================================


    [Fact]
    public async Task TakeAfterWhereAfterSelect_PaginatesTheFilteredRows()
    {
        // S-4 (решение владельца 2026-09-04): Take после in-memory Where обязан резать
        // ОТФИЛЬТРОВАННЫЕ строки. Раньше Take уходил в SQL до фильтра - сервер отдавал первую
        // попавшуюся страницу, фильтр выкидывал из неё, и строки молча терялись.
        await SyncAsync();
        for (var i = 1; i <= 30; i++)
            await SeedAsync(new ProjectionProbeProps { S = $"p{i}", N = i });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.N })
            .Where(r => r.N > 10)
            .Take(5)
            .ToListAsync();

        rs.Should().HaveCount(5, "кандидатов с N>10 двадцать - страница обязана быть полной");
        rs.Should().OnlyContain(r => r.N > 10);
    }

    [Fact]
    public async Task SkipTake_AfterWhereAndOrderBy_PagesTheFilteredOrderedRows()
    {
        await SyncAsync();
        for (var i = 1; i <= 30; i++)
            await SeedAsync(new ProjectionProbeProps { S = $"p{i}", N = i });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.N })
            .Where(r => r.N > 10)
            .OrderBy(r => r.N)
            .Skip(5)
            .Take(5)
            .ToListAsync();

        rs.Select(r => r.N).Should().Equal(new[] { 16, 17, 18, 19, 20 },
            "вторая страница отфильтрованного и отсортированного набора");
    }

    [Fact]
    public async Task TakeBeforeWhere_KeepsCallOrderSemantics()
    {
        // Пин порядка вызовов: Take ДО пост-Select Where режет источник (как в LINQ),
        // а Where потом фильтрует уже отрезанную страницу.
        await SyncAsync();
        for (var i = 1; i <= 20; i++)
            await SeedAsync(new ProjectionProbeProps { S = $"p{i}", N = i });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .OrderBy(x => x.N)
            .Select(x => new { x.Props.N })
            .Take(10)
            .Where(r => r.N > 5)
            .ToListAsync();

        rs.Select(r => r.N).Should().Equal(6, 7, 8, 9, 10);
    }


    [Fact]
    public async Task WhereAfterSelect_OnNestedProjectedMember_Filters()
    {
        // S-4 шаг 2: предикат по спроецированному вложенному члену транслируется в источник.
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "k", Nested = new ProjectionProbeNested { City = "Kazan" } });
        await SeedAsync(new ProjectionProbeProps { S = "m", Nested = new ProjectionProbeNested { City = "Moscow" } });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S, City = x.Props.Nested!.City })
            .Where(r => r.City == "Kazan")
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].S.Should().Be("k");
    }

    [Fact]
    public async Task WhereAfterSelect_OnComputedMember_StillFiltersCorrectly()
    {
        // Вычисляемый член не транслируется - фильтр остаётся в памяти и обязан быть корректен.
        await SyncAsync();
        for (var i = 1; i <= 20; i++)
            await SeedAsync(new ProjectionProbeProps { S = $"p{i}", N = i });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { Total = x.Props.N + 1 })
            .Where(r => r.Total > 11)
            .Take(5)
            .ToListAsync();

        rs.Should().HaveCount(5);
        rs.Should().OnlyContain(r => r.Total > 11);
    }

    [Fact]
    public async Task WhereAfterSelect_MixedWithBaseFieldMember_StillFiltersCorrectly()
    {
        // Член из базового поля (x.Id) не транслируется - корректная фильтрация в памяти.
        await SyncAsync();
        var id1 = await SeedAsync(new ProjectionProbeProps { S = "one", N = 1 });
        await SeedAsync(new ProjectionProbeProps { S = "two", N = 2 });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Id, x.Props.S })
            .Where(r => r.Id == id1)
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].S.Should().Be("one");
    }

    [Fact]
    public async Task OrderByAfterSelect_NullKey_DoesNotCrash()
    {
        // S-2: null-ключ подменялся new object() и Comparer<object> валил запрос.
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = null, N = 1 });
        await SeedAsync(new ProjectionProbeProps { S = "b", N = 2 });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S })
            .OrderBy(r => r.S)
            .ToListAsync();

        rs.Should().HaveCount(2, "null-ключ - штатная сортировка, а не крушение");
        rs[0].S.Should().BeNull("LINQ-контракт: null сортируется первым по возрастанию");
        rs[1].S.Should().Be("b");
    }

    [Fact]
    public async Task CountAsync_AfterDistinct_MatchesToList()
    {
        // S-3: Distinct применялся в памяти, а Count уходил в источник мимо дедупа.
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "dup", N = 1 });
        await SeedAsync(new ProjectionProbeProps { S = "dup", N = 2 });
        await SeedAsync(new ProjectionProbeProps { S = "uniq", N = 3 });

        var q = Redb.Query<ProjectionProbeProps>()
            .Select(x => new { x.Props.S })
            .Distinct();

        var list = await q.ToListAsync();
        var count = await q.CountAsync();

        list.Should().HaveCount(2);
        count.Should().Be(list.Count, "CountAsync обязан считать то же, что вернёт ToListAsync");
    }

    [Fact]
    public async Task Dto_NestedMemberInit_IsNotSilentlyPartial()
    {
        // S-5: MemberMemberBinding (Sub = { X = ... }) не разбирался экстрактором путей - поле
        // молча не загружалось, и лямбда писала default. Контракт: либо путь извлечён, либо
        // полная загрузка; тихое обнуление запрещено.
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "mm", N = 77 });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new ProjectionDto { A = x.Props.S, Sub = { X = x.Props.N } })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].A.Should().Be("mm");
        rs[0].Sub.X.Should().Be(77, "нераспознанный узел лямбды обязан откатывать на полную загрузку, а не молча обнулять поле");
    }

    [Fact]
    public async Task Projection_ThroughReference_IsNotSilentlyNull()
    {
        // S-6: путь через референс (Child.Props.Label) давал мусорный сегмент "Props" - сервер
        // его не разрешал, lazy-загрузчик занулён, поле молча null.
        await SyncAsync();
        var child = new RedbObject<ProjectionProbeChildProps>
            { name = "child", Props = new ProjectionProbeChildProps { Label = "child-label" } };
        await Redb.SaveAsync(child);
        await SeedAsync(new ProjectionProbeProps { S = "parent", Child = child });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { Label = x.Props.Child!.Props!.Label })
            .ToListAsync();

        rs.Should().ContainSingle();
        rs[0].Label.Should().Be("child-label",
            "проекция через референс обязана либо работать, либо внятно отказывать - но не возвращать тихий null");
    }

    [Fact]
    public async Task AggInsideSelect_RoutesToAggregatePath()
    {
        // S-7 (решение владельца 2026-09-03: маршрутизируем): пример из XML-доки Agg
        // `.Select(x => new { Total = Agg.Sum(...) })` кидал NotSupportedException в рантайме.
        await SyncAsync();
        await SeedAsync(new ProjectionProbeProps { S = "a1", D = 10m });
        await SeedAsync(new ProjectionProbeProps { S = "a2", D = 32m });

        var rs = await Redb.Query<ProjectionProbeProps>()
            .Select(x => new { Total = Agg.Sum(x.Props.D) })
            .ToListAsync();

        rs.Should().ContainSingle("Select с агрегатами - одна строка агрегатов");
        rs[0].Total.Should().Be(42m);
    }
}
