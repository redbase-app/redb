using System.Security.Cryptography;
using System.Text;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Guards the managed MD5 fallback used on browser-wasm (Blazor WebAssembly), where the runtime ships
/// no MD5 provider and <c>MD5.Create()</c> throws.
///
/// <para>
/// Why this matters beyond wasm: REDB stores hashes as <see cref="Guid"/> in the database, so a managed
/// implementation that differs from the platform one by a single bit would silently invalidate every
/// existing database — change tracking and cache validation compare these values. These tests assert
/// bit-for-bit equality against <see cref="System.Security.Cryptography.MD5"/>, which is what makes the
/// fallback safe to ship.
/// </para>
///
/// No database — pure calculator check.
/// </summary>
public class RedbMd5Tests
{
    private static byte[] SystemMd5(byte[] data)
    {
        using var md5 = MD5.Create();
        return md5.ComputeHash(data);
    }

    /// <summary>RFC 1321 section A.5 test suite — the canonical vectors.</summary>
    [Theory]
    [InlineData("", "d41d8cd98f00b204e9800998ecf8427e")]
    [InlineData("a", "0cc175b9c0f1b6a831c399e269772661")]
    [InlineData("abc", "900150983cd24fb0d6963f7d28e17f72")]
    [InlineData("message digest", "f96b697d7cb7938d525a2f31aaf161d0")]
    [InlineData("abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b")]
    [InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                "d174ab98d277d9f5a5611c2c9f419d9f")]
    [InlineData("12345678901234567890123456789012345678901234567890123456789012345678901234567890",
                "57edf4a22be3c955ac49da2e2107b67a")]
    public void Managed_Matches_RfcVectors(string input, string expectedHex)
    {
        var actual = RedbMd5.ComputeHashManaged(Encoding.ASCII.GetBytes(input));
        Assert.Equal(expectedHex, Convert.ToHexString(actual).ToLowerInvariant());
    }

    /// <summary>
    /// Padding is where MD5 implementations go wrong: the message is padded to a 64-byte block with a
    /// trailing 8-byte length, so lengths around 55/56 and 119/120 exercise the "one extra block" branch.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(54)]
    [InlineData(55)]
    [InlineData(56)]
    [InlineData(57)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(119)]
    [InlineData(120)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(129)]
    [InlineData(1000)]
    public void Managed_Matches_System_AtBlockBoundaries(int length)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++)
            data[i] = (byte)(i * 31 + 7);

        Assert.Equal(SystemMd5(data), RedbMd5.ComputeHashManaged(data));
    }

    [Fact]
    public void Managed_Matches_System_OnRandomBuffers()
    {
        var rnd = new Random(20260731);
        for (int i = 0; i < 200; i++)
        {
            var data = new byte[rnd.Next(0, 4096)];
            rnd.NextBytes(data);
            Assert.Equal(SystemMd5(data), RedbMd5.ComputeHashManaged(data));
        }
    }

    /// <summary>
    /// The dispatcher must return the same bytes whichever implementation it picked — otherwise a hash
    /// written on one platform would not match the same object read on another.
    /// </summary>
    [Fact]
    public void ComputeHash_Matches_System()
    {
        var data = Encoding.UTF8.GetBytes("redb|hash|payload|значение|42");
        Assert.Equal(SystemMd5(data), RedbMd5.ComputeHash(data));
    }

    /// <summary>Digest is always 16 bytes — it is stored as a Guid.</summary>
    [Fact]
    public void Digest_Is_16_Bytes()
    {
        Assert.Equal(16, RedbMd5.ComputeHashManaged([]).Length);
        Assert.Equal(16, RedbMd5.ComputeHashManaged(new byte[5000]).Length);
    }
}
