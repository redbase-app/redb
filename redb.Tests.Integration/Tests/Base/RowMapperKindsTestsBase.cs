using System.Text.Json.Serialization;
using redb.Core;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The row mapper of each provider (QueryAsync&lt;T&gt; of redb.Context) over a type with a property of every kind: the
/// column names it accepts (the property name in any case, its snake_case name, JsonPropertyName, a leading
/// underscore), a column no property takes (ignored), NULL into a nullable property, and the conversions it makes
/// (a wider integer into a narrower one, numeric and double into decimal, the database's boolean, guid, timestamp
/// and binary types). The mapper now binds the columns once per result set and sets the properties through compiled
/// setters; what it reads must not change.
/// </summary>
public abstract class RowMapperKindsTestsBase
{
    protected readonly IRedbService Redb;

    protected RowMapperKindsTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>
    /// One row: _id 1 (bigint), count 7 (bigint), maybe_long NULL, amount 12.50 (numeric), from_double 1.5 (double),
    /// ratio 0.25 (double), flag true, key_value 6f1d0c3e-2b4a-4c8e-9a51-3d2f7e8b9c01, when_at 2026-01-02 03:04:05 UTC,
    /// day NULL, bytes 0x0102, NAME 'abc', display_name 'shown', first_seen 2026-03-04 00:00:00 UTC, zzz 'x'.
    /// </summary>
    protected abstract string KindsSql { get; }

    /// <summary>One row: kind 2 (an integer column into an enum property).</summary>
    protected abstract string EnumSql { get; }

    public enum MapperKind { None = 0, First = 1, Second = 2 }

    public class MapperKindsRow
    {
        public long Id { get; set; }
        public int Count { get; set; }
        public long? MaybeLong { get; set; }
        public decimal Amount { get; set; }
        public decimal FromDouble { get; set; }
        public double Ratio { get; set; }
        public bool Flag { get; set; }
        public Guid KeyValue { get; set; }
        public DateTimeOffset WhenAt { get; set; }
        public DateTime? Day { get; set; }
        public byte[]? Bytes { get; set; }
        public string? Name { get; set; }
        [JsonPropertyName("display_name")]
        public string? Shown { get; set; }
        public DateTime FirstSeen { get; set; }
    }

    public class MapperEnumRow
    {
        public MapperKind Kind { get; set; }
    }

    [Fact]
    public async Task EveryKind_IsReadAsBefore()
    {
        var rows = await Redb.Context.QueryAsync<MapperKindsRow>(KindsSql);

        rows.Should().HaveCount(1);
        var row = rows[0];
        row.Id.Should().Be(1);
        row.Count.Should().Be(7);
        row.MaybeLong.Should().BeNull();
        row.Amount.Should().Be(12.50m);
        row.FromDouble.Should().Be(1.5m);
        row.Ratio.Should().Be(0.25);
        row.Flag.Should().BeTrue();
        row.KeyValue.Should().Be(Guid.Parse("6f1d0c3e-2b4a-4c8e-9a51-3d2f7e8b9c01"));
        row.WhenAt.Should().BeCloseTo(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), TimeSpan.FromMilliseconds(1));
        row.WhenAt.Offset.Should().Be(TimeSpan.Zero);
        row.Day.Should().BeNull();
        row.Bytes.Should().Equal(1, 2);
        row.Name.Should().Be("abc", "a column name in other case reaches the property");
        row.Shown.Should().Be("shown", "JsonPropertyName names the column");
        row.FirstSeen.Should().BeCloseTo(new DateTime(2026, 3, 4, 0, 0, 0, DateTimeKind.Utc), TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public async Task ManyRows_EachRowItsOwnValues()
    {
        // The columns are bound on the first row and reused: every row must still get its own values.
        var rows = await Redb.Context.QueryAsync<MapperKindsRow>(
            $"SELECT * FROM ({KindsSql}) a UNION ALL SELECT * FROM ({KindsSql.Replace("'abc'", "'def'")}) b");

        rows.Select(r => r.Name).Should().BeEquivalentTo(new[] { "abc", "def" });
    }

    /// <summary>An integer column into an enum property, as the provider read it before.</summary>
    protected virtual async Task AssertEnumAsync()
    {
        var rows = await Redb.Context.QueryAsync<MapperEnumRow>(EnumSql);
        rows.Single().Kind.Should().Be(MapperKind.Second);
    }

    [Fact]
    public Task IntegerIntoEnum_AsBefore() => AssertEnumAsync();
}
