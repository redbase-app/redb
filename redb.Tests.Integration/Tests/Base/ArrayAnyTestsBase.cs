using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Any() over an array or a list of simple values: true for an object with elements, false for an empty one and for
/// one that stores none. It was translated as the length of a text: PostgreSQL refused length(text[]), SQLite counted
/// an empty or missing array as non-empty (it measured '[]'), and SQL Server answered !Any() with no object.
/// </summary>
public abstract class ArrayAnyTestsBase
{
    protected readonly IRedbService Redb;

    protected ArrayAnyTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task AnyOverArraysOfEveryValueType_TrueOnlyWithElements()
    {
        await Redb.SyncSchemeAsync<ArrayValuesProps>();
        var marker = "any-" + Guid.NewGuid().ToString("N")[..8];
        var full = new ArrayValuesProps
        {
            Marker = marker, Words = ["w"], Longs = [1], Doubles = [1.5], Decimals = [2.5m], Flags = [true],
            Dates = [new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc)], Ids = [Guid.NewGuid()]
        };
        var empty = new ArrayValuesProps { Marker = marker, Words = [], Longs = [], Doubles = [], Decimals = [], Flags = [], Dates = [], Ids = [] };
        var none = new ArrayValuesProps { Marker = marker };
        await Redb.SaveAsync(new[] { full, empty, none }
            .Select((p, i) => (IRedbObject)new RedbObject<ArrayValuesProps> { name = $"{marker}-{i}", Props = p }).ToList());

        IRedbQueryable<ArrayValuesProps> Q() => Redb.Query<ArrayValuesProps>().Where(p => p.Marker == marker);
        async Task Check(string name, Expression<Func<ArrayValuesProps, bool>> any, Expression<Func<ArrayValuesProps, bool>> notAny)
        {
            (await Q().Where(any).CountAsync()).Should().Be(1, $"{name}.Any(): the object with elements");
            (await Q().Where(notAny).CountAsync()).Should().Be(2, $"!{name}.Any(): the empty array and the missing one");
        }

        await Check("Words", p => p.Words!.Any(), p => !p.Words!.Any());
        await Check("Longs", p => p.Longs!.Any(), p => !p.Longs!.Any());
        await Check("Doubles", p => p.Doubles!.Any(), p => !p.Doubles!.Any());
        await Check("Decimals", p => p.Decimals!.Any(), p => !p.Decimals!.Any());
        await Check("Flags", p => p.Flags!.Any(), p => !p.Flags!.Any());
        await Check("Dates", p => p.Dates!.Any(), p => !p.Dates!.Any());
        await Check("Ids", p => p.Ids!.Any(), p => !p.Ids!.Any());
    }

    [Fact]
    public async Task AnyOverListsOfValues_TrueOnlyWithElements()
    {
        await Redb.SyncSchemeAsync<ArrayAnyProps>();
        var marker = "anyl-" + Guid.NewGuid().ToString("N")[..8];
        var rows = new[]
        {
            new ArrayAnyProps { Marker = marker, Labels = ["a", "b"], Numbers = [7] },
            new ArrayAnyProps { Marker = marker, Labels = [], Numbers = [] },
            new ArrayAnyProps { Marker = marker },
        };
        await Redb.SaveAsync(rows.Select((p, i) => (IRedbObject)new RedbObject<ArrayAnyProps> { name = $"{marker}-{i}", Props = p }).ToList());

        IRedbQueryable<ArrayAnyProps> Q() => Redb.Query<ArrayAnyProps>().Where(p => p.Marker == marker);
        (await Q().Where(p => p.Labels!.Any()).CountAsync()).Should().Be(1, "List<string>.Any()");
        (await Q().Where(p => !p.Labels!.Any()).CountAsync()).Should().Be(2, "!List<string>.Any()");
        (await Q().Where(p => p.Numbers!.Any()).CountAsync()).Should().Be(1, "List<long>.Any()");
        (await Q().Where(p => p.Numbers!.Any() && p.Labels!.Any()).CountAsync()).Should().Be(1, "two Any() together");
    }
}
