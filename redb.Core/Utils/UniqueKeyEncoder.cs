using System;
using System.Globalization;
using System.Text;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;

namespace redb.Core.Utils
{
    /// <summary>
    /// Turns the value of a <c>[RedbUnique]</c> field into the 128-bit key stored in
    /// <c>_values._unique</c>: the RedbMd5 hash of the value's <b>canonical form</b>.
    ///
    /// <para>
    /// The requirement is strict and two-sided: one logical value must give exactly one form, and
    /// different logical values must not give the same one. A hash of an uncanonicalised value has the
    /// same hole, only quieter — <c>1.50m</c> and <c>1.5m</c> would be two keys, <c>12:00+03:00</c>
    /// and <c>09:00Z</c> two keys, "é" as one code point and as two would be two keys.
    /// </para>
    ///
    /// <para>
    /// The input is the <see cref="RedbValue"/> as it is about to be written — the typed column, not
    /// the CLR value. That is what makes a recomputation from the database
    /// (<c>RecomputeUniqueAsync</c>) produce the same key as the original save: both start from the
    /// same representation. Precision is therefore pinned to what every provider keeps:
    /// milliseconds for timestamps (SQLite stores a Julian REAL), 18 decimal places for
    /// <c>decimal</c> (the column scale on PostgreSQL and MSSQL).
    /// </para>
    ///
    /// <para>
    /// Rules, one per typed column; the column tag makes the form injective across columns:
    /// <list type="bullet">
    ///   <item><c>_String</c>: Unicode NFC. No trimming, no case folding — the application owns the
    ///   notion of its key (see <see cref="Attributes.RedbUniqueAttribute"/>).</item>
    ///   <item><c>_Long</c>: invariant decimal digits, one form for every integer width.</item>
    ///   <item><c>_Guid</c>: canonical <c>D</c> form, lower case.</item>
    ///   <item><c>_Double</c>: round-trip <c>R</c>; <c>-0</c> is <c>0</c>; NaN and infinities are
    ///   refused — they are not values that can be equal to themselves.</item>
    ///   <item><c>_DateTimeOffset</c>: UTC, truncated to milliseconds, <c>yyyy-MM-ddTHH:mm:ss.fffZ</c>.</item>
    ///   <item><c>_Boolean</c>: <c>1</c> / <c>0</c>.</item>
    ///   <item><c>_Numeric</c>: rounded to 18 places, trailing zeros dropped (<c>G29</c>), one zero.</item>
    ///   <item><c>_ByteArray</c>: the bytes themselves; there is nothing to canonicalise.</item>
    /// </list>
    /// A value with no typed column set (a NULL) has no key: NULL never takes part in uniqueness.
    /// </para>
    ///
    /// <para>
    /// <see cref="Version"/> is stamped into <c>_structures._unique_version</c> when a structure's
    /// keys are computed. Any change to the rules above — a different scale, trimming, another
    /// algorithm — must bump it; scheme synchronisation then recomputes every key of a structure whose
    /// stamp is behind, so stored keys never silently disagree with the encoder that checks them.
    /// </para>
    /// </summary>
    public static class UniqueKeyEncoder
    {
        /// <summary>Version of the canonical-form rules. Bump on any change to them.</summary>
        public const int Version = 1;

        /// <summary>
        /// The key for <paramref name="value"/>'s typed content, or <c>null</c> when every typed column
        /// is NULL. Throws <see cref="RedbUniqueKeyValueException"/> for a value that cannot be a key
        /// (NaN, infinity).
        /// </summary>
        public static Guid? Compute(RedbValue value)
        {
            if (value.ByteArray != null)
                return Hash((byte)'y', value.ByteArray);

            var text = CanonicalText(value, out var tag);
            return text is null ? null : Hash(tag, Encoding.UTF8.GetBytes(text));
        }

        /// <summary>
        /// The salted form for collection-scoped element keys (S3): the same canonical content,
        /// prefixed with the identity of the owning collection (<c>_array_parent_id</c> of the
        /// base row), so equal values in DIFFERENT collections get different keys and only true
        /// in-collection duplicates collide. The <c>'p'</c> tag keeps salted and unsalted forms
        /// injective against each other; the inner tag and a <c>'|'</c> separator keep the salted
        /// space injective across columns (the salt is decimal digits, never a <c>'|'</c>).
        /// </summary>
        public static Guid? Compute(RedbValue value, long? collectionSalt)
        {
            if (collectionSalt is null)
                return Compute(value);

            var saltPrefix = collectionSalt.Value.ToString(CultureInfo.InvariantCulture);
            if (value.ByteArray != null)
            {
                var prefix = Encoding.UTF8.GetBytes(saltPrefix + "|y|");
                var payload = new byte[prefix.Length + value.ByteArray.Length];
                Buffer.BlockCopy(prefix, 0, payload, 0, prefix.Length);
                Buffer.BlockCopy(value.ByteArray, 0, payload, prefix.Length, value.ByteArray.Length);
                return Hash((byte)'p', payload);
            }

            var text = CanonicalText(value, out var tag);
            return text is null
                ? null
                : Hash((byte)'p', Encoding.UTF8.GetBytes(saltPrefix + "|" + (char)tag + "|" + text));
        }

        /// <summary>
        /// The canonical text of a scalar column, with the column tag; <c>null</c> when no scalar
        /// column is set. Exposed for tests and diagnostics — the stored form is the hash.
        /// </summary>
        public static string? CanonicalText(RedbValue value, out byte tag)
        {
            // S3: reference elements canonicalise by the TARGET id - the stable identity of the
            // link. The persisted content hash in _Guid moves whenever the target's content
            // changes, which would silently migrate the key; the id never does. Checked before
            // every scalar column because a reference row may ALSO carry the target's hash in
            // _Guid (the dictionary path does). Additive: no previously keyed row ever carried
            // _Object or _ListItem, so existing keys keep their form - no Version bump.
            if (value.Object.HasValue)
            {
                tag = (byte)'o';
                return value.Object.Value.ToString(CultureInfo.InvariantCulture);
            }

            if (value.ListItem.HasValue)
            {
                tag = (byte)'r';
                return value.ListItem.Value.ToString(CultureInfo.InvariantCulture);
            }

            if (value.String != null)
            {
                tag = (byte)'s';
                return value.String.IsNormalized(NormalizationForm.FormC)
                    ? value.String
                    : value.String.Normalize(NormalizationForm.FormC);
            }

            if (value.Long.HasValue)
            {
                tag = (byte)'l';
                return value.Long.Value.ToString(CultureInfo.InvariantCulture);
            }

            if (value.Guid.HasValue)
            {
                tag = (byte)'g';
                return value.Guid.Value.ToString("D");
            }

            if (value.Double.HasValue)
            {
                tag = (byte)'d';
                var d = value.Double.Value;
                if (double.IsNaN(d) || double.IsInfinity(d))
                    throw new RedbUniqueKeyValueException(value.IdStructure,
                        $"{d} cannot be a unique key: it is not equal to itself or has no finite representation");
                if (d == 0) d = 0; // -0.0 and 0.0 are one value
                return d.ToString("R", CultureInfo.InvariantCulture);
            }

            if (value.DateTimeOffset.HasValue)
            {
                tag = (byte)'t';
                var utc = value.DateTimeOffset.Value.UtcDateTime;
                var ms = new DateTime(utc.Ticks - utc.Ticks % TimeSpan.TicksPerMillisecond, DateTimeKind.Utc);
                return ms.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
            }

            if (value.Boolean.HasValue)
            {
                tag = (byte)'b';
                return value.Boolean.Value ? "1" : "0";
            }

            if (value.Numeric.HasValue)
            {
                tag = (byte)'n';
                var n = decimal.Round(value.Numeric.Value, 18);
                if (n == 0m) n = 0m; // one zero: no "-0"
                return n.ToString("G29", CultureInfo.InvariantCulture);
            }

            tag = 0;
            return null;
        }

        private static Guid Hash(byte tag, byte[] payload)
        {
            var bytes = new byte[payload.Length + 1];
            bytes[0] = tag;
            Buffer.BlockCopy(payload, 0, bytes, 1, payload.Length);
            // Same construction as every other redb hash (RedbHash, SchemeHashCalculator): the 16 MD5
            // bytes through the Guid constructor, so the column holds one representation of one thing.
            return new Guid(RedbMd5.ComputeHash(bytes));
        }
    }
}
