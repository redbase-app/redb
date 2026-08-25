using System;
using System.Text.RegularExpressions;

namespace redb.Core.Query
{
    /// <summary>
    /// Single source of truth for what may appear as a collation name in generated SQL.
    ///
    /// <para>
    /// This matters more than a normal setting. <c>COLLATE</c> takes an <b>identifier</b>, not a
    /// value, so the name cannot be sent as a bound parameter: it is concatenated into the SQL text
    /// the provider builds. An unchecked name is therefore a direct injection point, reachable from
    /// application configuration.
    /// </para>
    ///
    /// <para>
    /// Two independent defences, because either one alone is a single point of failure:
    /// this validator rejects anything outside a conservative shape at the moment the value is
    /// assigned, long before any SQL exists; and the emitters additionally quote the name
    /// (<see cref="Quote"/> in C#, <c>quote_ident()</c> in the in-database functions). A name that
    /// somehow passed the first would still be inert after the second.
    /// </para>
    ///
    /// <para>
    /// The accepted shape is deliberately narrower than what PostgreSQL allows. Real collation names
    /// are things like <c>und-x-icu</c>, <c>ru-x-icu</c>, <c>en_US.utf8</c>, <c>C</c>; none of them
    /// need quotes, spaces or punctuation beyond the four characters below. Widening this later is
    /// safe, narrowing it is not.
    /// </para>
    /// </summary>
    public static class CollationNameValidator
    {
        /// <summary>Longest name accepted. PostgreSQL identifiers truncate at 63 bytes.</summary>
        public const int MaxLength = 63;

        // Letters, digits, and the four separators that occur in real collation names.
        // Notably absent: the double quote, which is what Quote() would have to escape.
        private static readonly Regex AllowedShape =
            new(@"^[A-Za-z0-9][A-Za-z0-9_.\-]*$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        /// <summary>
        /// Throws <see cref="ArgumentException"/> unless <paramref name="name"/> is a plausible
        /// collation name. Called when the setting is assigned, so a bad value fails at startup with
        /// a message naming the setting, rather than as a driver-level syntax error on the first
        /// search query.
        /// </summary>
        public static void Validate(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Collation name must not be empty.", nameof(name));

            if (name.Length > MaxLength)
                throw new ArgumentException(
                    $"Collation name '{name}' is {name.Length} characters; the maximum is {MaxLength}.",
                    nameof(name));

            if (!AllowedShape.IsMatch(name))
                throw new ArgumentException(
                    $"Collation name '{name}' is not a valid identifier. Allowed: letters, digits, " +
                    "'_', '.' and '-', starting with a letter or digit. Examples: und-x-icu, ru-x-icu, en_US.utf8.",
                    nameof(name));
        }

        /// <summary>True when <paramref name="name"/> would pass <see cref="Validate"/>.</summary>
        public static bool IsValid(string? name) =>
            !string.IsNullOrWhiteSpace(name)
            && name!.Length <= MaxLength
            && AllowedShape.IsMatch(name);

        /// <summary>
        /// Renders the name as a quoted SQL identifier. The double quote is doubled even though
        /// <see cref="Validate"/> already rejects it: the escaping must be correct on its own, not
        /// because something upstream promised it would never be needed.
        /// </summary>
        public static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
    }
}
