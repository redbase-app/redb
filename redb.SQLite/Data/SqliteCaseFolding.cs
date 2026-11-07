using System;
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace redb.SQLite.Data
{
    /// <summary>
    /// Replaces SQLite's built-in <c>like</c>, <c>lower</c> and <c>upper</c> with Unicode-aware
    /// implementations, on one connection.
    ///
    /// <para>
    /// <b>Why this is a connection concern and not a SQL one.</b> On PostgreSQL the fix is a
    /// <c>COLLATE</c> clause in the generated SQL. SQLite has no equivalent to attach: its only
    /// case-insensitive collation, <c>NOCASE</c>, folds ASCII and nothing else, and <c>LIKE</c> is
    /// implemented as a function, so collations never reach it. Measured on the build redb ships
    /// with (3.46.1, no ICU compiled in): <c>'Привет' LIKE '%привет%'</c> is 0,
    /// <c>lower('Привет')</c> returns the string unchanged, and <c>'Привет' = 'привет' COLLATE
    /// NOCASE</c> is 0. Unlike PostgreSQL, where this depends on how the database was created, in
    /// SQLite it is unconditional.
    /// </para>
    ///
    /// <para>
    /// Overriding a built-in is the supported mechanism, and it is exactly what SQLite's own ICU
    /// extension does. Doing it from managed code keeps the whole fix inside the provider: no ICU
    /// build, no change to the native extension, no per-platform rebuild.
    /// </para>
    ///
    /// <para>
    /// <b>Cost.</b> A managed callback runs per row instead of the C implementation. That matters
    /// less than it sounds for the case this exists to serve: <c>LIKE '%x%'</c> cannot use an index
    /// in SQLite anyway, so both versions scan, and the difference is call overhead rather than
    /// algorithmic. It is opt-in regardless.
    /// </para>
    /// </summary>
    internal static class SqliteCaseFolding
    {
        /// <summary>
        /// Installs the overrides when <paramref name="enabled"/>; otherwise does nothing at all, so
        /// a connection is byte-for-byte what it was before this type existed.
        /// </summary>
        internal static void Install(SqliteConnection connection, bool enabled)
        {
            if (!enabled) return;

            // ToLowerInvariant rather than ToUpperInvariant on purpose. Upper-casing is the
            // length-changing direction in several languages (German ß becomes SS), which would make
            // lower(upper(x)) disagree with x and make the folded comparison disagree with itself.
            // Lower-casing is length-preserving for every mapping we care about here.
            connection.CreateFunction<string?, string?>(
                "lower", s => s?.ToLowerInvariant(), isDeterministic: true);

            connection.CreateFunction<string?, string?>(
                "upper", s => s?.ToUpperInvariant(), isDeterministic: true);

            // Both arities. SQLite exposes LIKE as like(pattern, value) and, for the
            // "LIKE ... ESCAPE c" form, like(pattern, value, escape). Registering only the first
            // would leave every ESCAPE query silently on the ASCII-only built-in — and redb emits
            // ESCAPE on its user-search path, so that is not a hypothetical.
            connection.CreateFunction<string?, string?, long?>(
                "like", (pattern, value) => Like(pattern, value, null), isDeterministic: true);

            connection.CreateFunction<string?, string?, string?, long?>(
                "like", (pattern, value, escape) => Like(pattern, value, escape), isDeterministic: true);
        }

        /// <summary>
        /// SQLite's LIKE semantics, folded for all scripts: <c>%</c> matches any sequence including
        /// empty, <c>_</c> matches exactly one character, and a character preceded by the escape
        /// character is literal. NULL in, NULL out, matching the built-in.
        /// </summary>
        private static long? Like(string? pattern, string? value, string? escape)
        {
            if (pattern is null || value is null) return null;

            char? esc = null;
            if (!string.IsNullOrEmpty(escape))
            {
                // SQLite raises "ESCAPE expression must be a single character"; mirror that rather
                // than silently matching nothing.
                if (escape!.Length != 1)
                    throw new ArgumentException("ESCAPE expression must be a single character.", nameof(escape));
                esc = char.ToLowerInvariant(escape[0]);
            }

            // Fold once, then match on the folded forms. Case folding a pattern is safe: '%' and '_'
            // have no case, and the escape character is folded with it so the two stay in step.
            return IsMatch(pattern.ToLowerInvariant(), value.ToLowerInvariant(), esc) ? 1L : 0L;
        }

        /// <summary>
        /// Iterative wildcard match with backtracking. Iterative rather than recursive so a
        /// pathological pattern cannot exhaust the stack, and hand-written rather than translated to
        /// a regular expression so a user-supplied pattern cannot become a catastrophic-backtracking
        /// weapon.
        /// </summary>
        private static bool IsMatch(string pattern, string value, char? escape)
        {
            int p = 0, v = 0;
            int starP = -1, starV = 0;

            while (v < value.Length)
            {
                if (p < pattern.Length)
                {
                    var pc = pattern[p];

                    if (escape.HasValue && pc == escape.Value)
                    {
                        // A pattern that ends with the escape character matches nothing at all,
                        // which is what SQLite's own like() does. Verified against the built-in
                        // rather than assumed.
                        if (p + 1 >= pattern.Length) return false;

                        // Escaped: the next pattern character is literal, wildcards included.
                        var escLen = RuneLength(pattern, p + 1);
                        var valLen = RuneLength(value, v);
                        if (escLen == valLen
                            && string.CompareOrdinal(pattern, p + 1, value, v, escLen) == 0)
                        {
                            p += 1 + escLen; v += valLen; continue;
                        }
                    }
                    else if (pc == '%')
                    {
                        // Remember where the wildcard was so we can extend it on a later mismatch.
                        starP = p; starV = v; p++; continue;
                    }
                    else if (pc == '_')
                    {
                        // Exactly one CHARACTER, and in SQLite a character is a code point, not a
                        // UTF-16 code unit. Counting units would make '_' fail to match an emoji
                        // or any other astral character, where the built-in matches it.
                        v += RuneLength(value, v); p++; continue;
                    }
                    else
                    {
                        var patLen = RuneLength(pattern, p);
                        var valLen = RuneLength(value, v);
                        if (patLen == valLen
                            && string.CompareOrdinal(pattern, p, value, v, patLen) == 0)
                        {
                            p += patLen; v += valLen; continue;
                        }
                    }
                }

                if (starP >= 0)
                {
                    // Let the last '%' swallow one more code point and retry from just after it.
                    starV += RuneLength(value, starV);
                    p = starP + 1; v = starV; continue;
                }

                return false;
            }

            // Trailing '%' are allowed to match nothing; anything else left over is a mismatch.
            while (p < pattern.Length && pattern[p] == '%') p++;

            // ... including a lone trailing escape character, per the built-in.
            if (escape.HasValue && p == pattern.Length - 1 && pattern[p] == escape.Value)
                return false;

            return p == pattern.Length;
        }

        /// <summary>
        /// Length in UTF-16 units of the code point starting at <paramref name="i"/>: 2 for a
        /// well-formed surrogate pair, 1 otherwise. A lone surrogate counts as 1, so malformed
        /// input degrades instead of throwing.
        /// </summary>
        private static int RuneLength(string s, int i)
            => char.IsHighSurrogate(s[i]) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]) ? 2 : 1;
    }
}
