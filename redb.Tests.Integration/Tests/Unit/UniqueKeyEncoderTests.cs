using System.Text;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The canonical-form rules of <see cref="UniqueKeyEncoder"/>, one test per trap from
/// UNIQUE_STRING_KEY_PLAN §3.1. The requirement is two-sided: one logical value — one key,
/// different values — different keys. Every rule change must bump <see cref="UniqueKeyEncoder.Version"/>;
/// these tests pin the rules of version 1.
/// </summary>
public class UniqueKeyEncoderTests
{
    // Non-null helper: every value case has a key; the null case is asserted directly below.
    private static Guid KeyOf(RedbValue v) => UniqueKeyEncoder.Compute(v)!.Value;

    [Fact]
    public void Decimal_TrailingZeros_AreOneKey()
    {
        KeyOf(new RedbValue { Numeric = 1.50m }).Should().Be(KeyOf(new RedbValue { Numeric = 1.5m }));
        KeyOf(new RedbValue { Numeric = 1.5m }).Should().NotBe(KeyOf(new RedbValue { Numeric = 1.51m }));
        KeyOf(new RedbValue { Numeric = 0.0m }).Should().Be(KeyOf(new RedbValue { Numeric = decimal.Negate(0.0m) }),
            "there is one zero");
    }

    [Fact]
    public void Timestamps_SameInstant_IsOneKey()
    {
        var moscow = new DateTimeOffset(2026, 8, 31, 12, 0, 0, TimeSpan.FromHours(3));
        var utc = new DateTimeOffset(2026, 8, 31, 9, 0, 0, TimeSpan.Zero);
        KeyOf(new RedbValue { DateTimeOffset = moscow }).Should().Be(KeyOf(new RedbValue { DateTimeOffset = utc }));

        // Precision is pinned to milliseconds - the coarsest representation every provider keeps.
        var aTickMore = utc.AddTicks(1);
        KeyOf(new RedbValue { DateTimeOffset = aTickMore }).Should().Be(KeyOf(new RedbValue { DateTimeOffset = utc }));
        KeyOf(new RedbValue { DateTimeOffset = utc.AddMilliseconds(1) })
            .Should().NotBe(KeyOf(new RedbValue { DateTimeOffset = utc }));
    }

    [Fact]
    public void Strings_NfcAndNfd_AreOneKey_CaseIsNot()
    {
        var nfc = "café";          // é as one code point
        var nfd = "café";          // e + combining acute
        nfc.Should().NotBe(nfd, "the raw strings differ");
        KeyOf(new RedbValue { String = nfc }).Should().Be(KeyOf(new RedbValue { String = nfd }));

        // No case folding and no trimming: the application owns the notion of its key.
        KeyOf(new RedbValue { String = "ORD-1" }).Should().NotBe(KeyOf(new RedbValue { String = "ord-1" }));
        KeyOf(new RedbValue { String = "ORD-1" }).Should().NotBe(KeyOf(new RedbValue { String = "ORD-1 " }));
    }

    [Fact]
    public void Doubles_NegativeZero_IsZero_NaNRefused()
    {
        KeyOf(new RedbValue { Double = -0.0 }).Should().Be(KeyOf(new RedbValue { Double = 0.0 }));
        KeyOf(new RedbValue { Double = 0.1 }).Should().NotBe(KeyOf(new RedbValue { Double = 0.2 }));

        var nan = () => KeyOf(new RedbValue { Double = double.NaN });
        nan.Should().Throw<RedbUniqueKeyValueException>("NaN is not equal to itself and cannot be a key");
        var inf = () => KeyOf(new RedbValue { Double = double.PositiveInfinity });
        inf.Should().Throw<RedbUniqueKeyValueException>();
    }

    [Fact]
    public void ByteArray_IsHashedRaw()
    {
        var a = Encoding.UTF8.GetBytes("payload");
        var b = Encoding.UTF8.GetBytes("payload");
        var c = Encoding.UTF8.GetBytes("payloae");
        KeyOf(new RedbValue { ByteArray = a }).Should().Be(KeyOf(new RedbValue { ByteArray = b }));
        KeyOf(new RedbValue { ByteArray = a }).Should().NotBe(KeyOf(new RedbValue { ByteArray = c }));
    }

    [Fact]
    public void CrossColumn_SameText_IsNotOneKey()
    {
        // The column tag keeps the form injective across columns: long 1 is not string "1",
        // bool true is not long 1.
        KeyOf(new RedbValue { Long = 1 }).Should().NotBe(KeyOf(new RedbValue { String = "1" }));
        KeyOf(new RedbValue { Boolean = true }).Should().NotBe(KeyOf(new RedbValue { Long = 1 }));
        KeyOf(new RedbValue { Boolean = true }).Should().NotBe(KeyOf(new RedbValue { String = "1" }));
    }

    [Fact]
    public void NullValue_HasNoKey()
    {
        UniqueKeyEncoder.Compute(new RedbValue()).Should().BeNull("NULL never takes part in uniqueness");
    }

    [Fact]
    public void Guids_AreCaseInsensitiveByConstruction()
    {
        var g = Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF");
        KeyOf(new RedbValue { Guid = g }).Should().Be(KeyOf(new RedbValue { Guid = Guid.Parse(g.ToString().ToLowerInvariant()) }));
    }
}
