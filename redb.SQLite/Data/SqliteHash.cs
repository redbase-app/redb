using System;

namespace redb.SQLite.Data;

/// <summary>
/// The stored form of a 128-bit hash in SQLite — <c>_objects._hash</c>, <c>_schemes._structure_hash</c>,
/// <c>_users._hash</c>: <c>BLOB(16)</c> in RFC 4122 byte order, i.e. the bytes of the canonical text
/// form read left to right (<c>00112233-4455-6677-8899-aabbccddeeff</c> is <c>00 11 22 33 44 55 …</c>).
///
/// <para>
/// One place for the byte order, because several code paths write it and several read it. Writes go
/// through the Guid's TEXT parameter and the SQL <see cref="FromText"/> wrapper — <c>unhex()</c> of the
/// text without dashes is by construction the order the text shows; the native extension does the same
/// in its own statements. Reads get a <c>byte[]</c> from the driver and use <see cref="FromBlob"/>.
/// The two must agree, and <c>Guid.ToByteArray()</c> without the big-endian flag does not: it puts the
/// first three groups in little-endian order.
/// </para>
///
/// <para>
/// Only hashes. <c>_values._Guid</c>, <c>_objects._value_guid</c>, <c>_users._code_guid</c> are data and
/// stay TEXT — they are compared with values users write and read as text.
/// </para>
///
/// <para>
/// A database created before V4 holds the hashes as 36-character TEXT. SQLite keeps the storage class
/// a value was written with regardless of the declared type, so no <c>ALTER</c> is needed: the values
/// are converted in place on open (<c>ApplySchemaUpgradesAsync</c>), and a reader that meets a TEXT
/// value anyway still understands it.
/// </para>
/// </summary>
public static class SqliteHash
{
    /// <summary>
    /// SQL that turns a canonical uuid TEXT expression (a <c>$n</c> parameter, a column) into the
    /// 16-byte BLOB. NULL stays NULL. <c>unhex()</c> needs SQLite 3.41+; the bundled library is newer.
    /// </summary>
    public static string FromText(string sqlExpression) => $"unhex(replace({sqlExpression},'-',''))";

    /// <summary>The 16 bytes of <paramref name="value"/> in RFC 4122 order.</summary>
    public static byte[] ToBlob(Guid value)
    {
        // Guid.ToByteArray() is little-endian in the first three groups; put them back in text order.
        var b = value.ToByteArray();
        return new[]
        {
            b[3], b[2], b[1], b[0],
            b[5], b[4],
            b[7], b[6],
            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
        };
    }

    /// <summary>The Guid whose canonical text form spells <paramref name="bytes"/>.</summary>
    public static Guid FromBlob(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16)
            throw new FormatException($"A stored hash is 16 bytes, got {bytes.Length}.");

        return new Guid(
            (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3],
            (short)((bytes[4] << 8) | bytes[5]),
            (short)((bytes[6] << 8) | bytes[7]),
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
    }

    /// <summary>
    /// A Guid from whatever the driver returned for a uuid-valued column: the BLOB of a hash, the
    /// TEXT of a data guid (or of a hash not yet converted), or a Guid already.
    /// </summary>
    public static Guid FromDbValue(object value) => value switch
    {
        Guid g => g,
        byte[] b => FromBlob(b),
        _ => Guid.Parse(value.ToString()!),
    };
}
