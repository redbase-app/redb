using System;

namespace redb.Core.Query.Window;

/// <summary>
/// Validates a caller-supplied window function name before it is emitted into SQL.
/// </summary>
/// <remarks>
/// The name arrives from <see cref="WindowFuncRequest.Func"/>, a public settable string, so it is
/// data and not SQL. Every provider used to append it to the call text as-is
/// (<c>func + "()"</c>), which let a quote, a bracket or a comment marker reach the statement and
/// turn the remainder of the name into live SQL (SQLI-PoC: window function name fallback, all
/// dialects including Pro). Providers call this helper instead: a name that is not a plain
/// identifier is refused rather than pasted.
/// </remarks>
public static class WindowFunctionName
{
    /// <summary>Longest accepted name. T-SQL caps an identifier at 128 characters; 64 is ample.</summary>
    public const int MaxLength = 64;

    /// <summary>True when <paramref name="name"/> holds only letters, digits and underscores.</summary>
    public static bool IsPlainIdentifier(string? name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > MaxLength)
            return false;

        foreach (var c in name)
        {
            if (!char.IsLetterOrDigit(c) && c != '_')
                return false;
        }

        return true;
    }

    /// <summary>
    /// Renders a zero-argument call for <paramref name="name"/>, or throws when the name is not a
    /// plain identifier. Throwing is deliberate: the previous behaviour appended the raw string.
    /// </summary>
    public static string ZeroArgCall(string? name)
    {
        if (!IsPlainIdentifier(name))
            throw new ArgumentException(
                $"Unsupported window function: '{name}'. The name must be a plain identifier.",
                nameof(name));

        return name!.ToUpperInvariant() + "()";
    }
}
