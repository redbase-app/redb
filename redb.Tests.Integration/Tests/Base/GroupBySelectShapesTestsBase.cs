using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Формы селектора GroupBy(...).SelectAsync(...) (ревью 2026-09-03, G-1/G-2/G-3; решения
/// владельца: DTO/MemberInit поддерживаем, всё нераспознанное - громко). До этого набора:
/// DTO-класс в SelectAsync молча давал строки из default-значений, вычисление вокруг Agg
/// молча возвращало null, а вычисляемый ключ группировки молча давал пустой GROUP BY.
/// </summary>
public abstract class GroupBySelectShapesTestsBase
{
    protected readonly IRedbService Redb;

    protected GroupBySelectShapesTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    public class DeptRowDto
    {
        public string? Dept { get; set; }
        public long Cnt { get; set; }
        public decimal Total { get; set; }
    }

    [Fact]
    public async Task SelectAsync_IntoDtoMemberInit_MaterializesEveryMember()
    {
        // G-1: MemberInit раньше не разбирался - каждая строка приходила как default!.
        await SeedAsync();

        var rows = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .SelectAsync(g => new DeptRowDto
            {
                Dept = g.Key,
                Cnt = Agg.Count(g),
                Total = Agg.Sum(g, x => x.Salary),
            });

        rows.Should().NotBeEmpty();
        rows.Should().AllSatisfy(r =>
        {
            r.Should().NotBeNull("DTO-строка обязана материализоваться, а не приходить null");
            r.Dept.Should().NotBeNullOrEmpty();
            r.Cnt.Should().BeGreaterThan(0);
            r.Total.Should().BeGreaterThan(0);
        });
    }

    [Fact]
    public async Task SelectAsync_ComputedAroundAgg_IsLoud()
    {
        // G-2: вычисление вокруг агрегата не заказывается у сервера - раньше член молча
        // оставался null/0. Контракт: громкий отказ с именем члена.
        await SeedAsync();

        var act = async () => await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .SelectAsync(g => new { Doubled = Agg.Count(g) * 2 });

        (await act.Should().ThrowAsync<NotSupportedException>(
                "молча вернуть null вместо вычисления - худший исход"))
            .WithMessage("*Doubled*");
    }

    [Fact]
    public async Task GroupBy_ComputedKey_IsLoud()
    {
        // G-3: вычисляемый ключ группировки раньше молча давал пустой GROUP BY.
        await SeedAsync();

        var act = async () => await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Age + 1)
            .SelectAsync(g => new { Cnt = Agg.Count(g) });

        await act.Should().ThrowAsync<NotSupportedException>(
            "группировка по вычисляемому ключу не поддерживается и обязана отказывать громко");
    }


    public class ArrayGroupRowDto
    {
        public string? Type { get; set; }
        public long Cnt { get; set; }
    }

    [Fact]
    public async Task ArrayGroupBy_SelectAsync_IntoDtoMemberInit()
    {
        // G-1 доехал до GroupByArray (решение владельца 2026-09-04): раньше здесь стоял
        // громкий отказ на DTO.
        await SeedAsync();

        var rows = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new ArrayGroupRowDto { Type = g.Key, Cnt = Agg.Count(g) });

        rows.Should().NotBeEmpty();
        rows.Should().AllSatisfy(r =>
        {
            r.Type.Should().NotBeNullOrEmpty();
            r.Cnt.Should().BeGreaterThan(0);
        });
    }

    [Fact]
    public async Task SelectAsync_ClientConstantMember_IsEvaluated()
    {
        // Член, не ссылающийся на группу, - клиентская константа: вычисляется на клиенте,
        // а не молча null (раньше материализатор искал его в JSON и не находил).
        await SeedAsync();
        var tag = "fixed-tag";

        var rows = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .SelectAsync(g => new { Dept = g.Key, Tag = tag, Cnt = Agg.Count(g) });

        rows.Should().NotBeEmpty();
        rows.Should().AllSatisfy(r => r.Tag.Should().Be("fixed-tag"));
    }
}
