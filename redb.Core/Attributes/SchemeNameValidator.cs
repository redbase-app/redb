using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using redb.Core.Exceptions;

namespace redb.Core.Attributes
{
    /// <summary>
    /// Single source of truth for explicit scheme name rules.
    /// <para>
    /// The rules mirror the database-level triggers one-for-one (PostgreSQL
    /// <c>validate_scheme_name</c>, and the MSSql/SQLite equivalents). Validation lives here
    /// because it must behave identically on every provider and must fail before any SQL is
    /// issued — a raw 23505/2627 surfacing from the driver tells the developer nothing about
    /// which type is at fault.
    /// </para>
    /// <para>
    /// Only <b>explicit</b> names (<see cref="RedbSchemeAttribute.Name"/>) are validated. Names
    /// derived from <see cref="Type.FullName"/> are valid by construction, and checking them
    /// would break existing projects that never asked for this feature.
    /// </para>
    /// </summary>
    public static class SchemeNameValidator
    {
        /// <summary>Names starting with this prefix are reserved for internal schemes and skip validation.</summary>
        public const string SystemPrefix = "@@";

        /// <summary>Maximum length accepted by the database triggers.</summary>
        public const int MaxLength = 128;

        private static readonly Regex AllowedShape =
            new(@"^[a-zA-Z_][a-zA-Z0-9_.+]*$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private static readonly Regex ValidPart =
            new(@"^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        // Same list as the database triggers. Kept verbatim so the three providers and this
        // validator can never disagree about what a reserved word is.
        private static readonly HashSet<string> ReservedWords = new(StringComparer.Ordinal)
        {
            "abstract", "as", "bool", "break", "byte", "case", "catch", "char", "checked",
            "class", "const", "continue", "decimal", "default", "delegate", "do", "double", "else",
            "enum", "event", "explicit", "extern", "false", "finally", "fixed", "float", "for",
            "foreach", "goto", "if", "implicit", "in", "int", "interface", "internal", "is", "lock",
            "long", "namespace", "new", "null", "object", "operator", "out", "override", "params",
            "private", "protected", "public", "readonly", "ref", "return", "sbyte", "sealed",
            "short", "sizeof", "stackalloc", "static", "string", "struct", "switch", "this",
            "throw", "true", "try", "typeof", "uint", "ulong", "unchecked", "unsafe", "ushort",
            "using", "virtual", "void", "volatile", "while"
        };

        /// <summary>
        /// Validates an explicit scheme name and throws <see cref="RedbSchemeNameException"/> when it
        /// breaks a rule. The declaring type is carried into the message so the developer knows
        /// exactly which class to fix.
        /// </summary>
        public static void Validate(string? name, Type declaringType)
        {
            if (!IsValid(name, out var error))
                throw new RedbSchemeNameException(declaringType, name, error!);
        }

        /// <summary>
        /// Non-throwing form. Returns <c>false</c> and fills <paramref name="error"/> with a
        /// human-readable reason.
        /// </summary>
        public static bool IsValid(string? name, out string? error)
        {
            error = null;

            // Rule 5: not empty / not whitespace-only.
            if (string.IsNullOrWhiteSpace(name))
            {
                error = "the name is empty";
                return false;
            }

            // Rule 0: system schemes bypass every other rule.
            if (name!.StartsWith(SystemPrefix, StringComparison.Ordinal))
                return true;

            // Rule 6: length.
            if (name.Length > MaxLength)
            {
                error = $"the name is longer than {MaxLength} characters (got {name.Length})";
                return false;
            }

            // Rule 1: must not start with a digit. Checked before the shape rule so the message is specific.
            if (name[0] >= '0' && name[0] <= '9')
            {
                error = "the name starts with a digit; it must start with a letter or underscore";
                return false;
            }

            // Rule 2: allowed character set.
            if (!AllowedShape.IsMatch(name))
            {
                error = "the name contains characters outside [a-zA-Z0-9_.+]; " +
                        "spaces, punctuation and non-Latin letters are not allowed";
                return false;
            }

            // Rule 3: must not end with a dot.
            if (name.EndsWith(".", StringComparison.Ordinal))
            {
                error = "the name ends with a dot";
                return false;
            }

            // Rule 4: no two consecutive dots.
            if (name.Contains(".."))
            {
                error = "the name contains two consecutive dots";
                return false;
            }

            // Rules 7 and 8: every part between dots/pluses must be a valid identifier and
            // must not be a C# reserved word.
            foreach (var part in name.Split('.', '+'))
            {
                if (!ValidPart.IsMatch(part))
                {
                    error = $"namespace part '{part}' is not a valid identifier";
                    return false;
                }

                if (ReservedWords.Contains(part.ToLowerInvariant()))
                {
                    error = $"namespace part '{part}' is a C# reserved word";
                    return false;
                }
            }

            return true;
        }
    }
}
