using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A decimal saved and read back is the decimal that was saved: by a load, by a query, by Min and Max. SQLite keeps a
/// decimal in a REAL column by default, so its reader turns a double into a decimal; that conversion must give the
/// number that was stored on every runtime (the explicit conversion rounded to 15 digits up to .NET 10 and gives the
/// exact binary value from .NET 11 on: 123.45 came back as 123.45000000000000284217094304).
///
/// Each test reads only the objects it seeded (by a title of its own), so the shared database stays out.
/// </summary>
public abstract class DecimalRoundTripTestsBase
{
    protected readonly IRedbService Redb;

    protected DecimalRoundTripTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>The provider keeps a decimal in a binary floating-point column (SQLite's default REAL mode).</summary>
    protected virtual bool StoresDecimalAsDouble => false;

    // The kinds a user has: an integer, a price, a long fraction (15 significant digits), a negative value, a small one.
    public static IEnumerable<object[]> Values() =>
    [
        [42m], [123.45m], [0.1m], [0.333333333333333m], [-98765.4321m], [0.000001234m], [999999999.99m]
    ];

    [Theory]
    [MemberData(nameof(Values))]
    public async Task Decimal_LoadedQueriedAndAggregated_IsTheSavedValue(decimal value)
    {
        var tag = "dec-" + Guid.NewGuid().ToString("N")[..8];
        var id = await Redb.SaveAsync(new RedbObject<SimpleProps> { name = tag, Props = new SimpleProps { Title = tag, Price = value } });

        (await Redb.LoadAsync<SimpleProps>(id))!.Props.Price.Should().Be(value, "a load gives the saved decimal");

        var queried = await Redb.Query<SimpleProps>().Where(p => p.Title == tag).ToListAsync();
        queried.Should().ContainSingle().Which.Props.Price.Should().Be(value, "a query gives the saved decimal");

        (await Redb.Query<SimpleProps>().Where(p => p.Title == tag).MaxAsync(p => p.Price)).Should().Be(value, "Max of one value is that value");
        (await Redb.Query<SimpleProps>().Where(p => p.Title == tag).MinAsync(p => p.Price)).Should().Be(value, "Min of one value is that value");
    }

    [Fact]
    public async Task Decimal_WithMoreDigitsThanADoubleHolds_KeepsWhatTheColumnHolds()
    {
        // 17 significant digits: a numeric column keeps them; a REAL column holds the nearest double, and the read
        // gives the shortest decimal of that double, the same on every runtime.
        const decimal value = 12345678901234.567m;
        var tag = "dec-" + Guid.NewGuid().ToString("N")[..8];
        var id = await Redb.SaveAsync(new RedbObject<SimpleProps> { name = tag, Props = new SimpleProps { Title = tag, Price = value } });

        var expected = StoresDecimalAsDouble
            ? decimal.Parse(((double)value).ToString("R", System.Globalization.CultureInfo.InvariantCulture),
                System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture)
            : value;
        (await Redb.LoadAsync<SimpleProps>(id))!.Props.Price.Should().Be(expected);
        (await Redb.Query<SimpleProps>().Where(p => p.Title == tag).MaxAsync(p => p.Price)).Should().Be(expected);
    }
}
