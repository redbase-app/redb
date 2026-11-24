namespace redb.Export.Providers;

/// <summary>
/// The wording an import uses to refuse a file whose rows point at rows it does not hold, shared by the providers
/// that check the foreign keys themselves at the end of an import, so the same file is described the same way.
/// </summary>
internal static class ForeignKeyRefusal
{
    /// <summary>One line for one broken key.</summary>
    /// <param name="key">The key: its name, or the referencing column where the database keeps no name.</param>
    /// <param name="table">The table whose rows point nowhere.</param>
    /// <param name="rows">How many of its rows do.</param>
    /// <param name="referencedTable">The table the key points at.</param>
    /// <param name="referencedColumns">The columns of that table the key points at.</param>
    /// <param name="example">One of the rows, as <c>column = value</c> pairs.</param>
    public static string Describe(string key, string table, long rows, string referencedTable,
        IEnumerable<string> referencedColumns, string example) =>
        $"{key}: {rows} row(s) of {table} point at a missing row of {referencedTable} " +
        $"({string.Join(", ", referencedColumns)}), for example {example}";

    /// <summary>The exception that refuses the import.</summary>
    /// <param name="broken">The lines from <see cref="Describe"/>.</param>
    /// <param name="outcome">What the refusal leaves in the database.</param>
    public static InvalidOperationException Exception(IReadOnlyCollection<string> broken, string outcome) =>
        new("The file holds references to rows it does not contain, and the database refuses them " +
            "(a file exported with --schemes whose rows point into schemes left out is one way to get one):" +
            Environment.NewLine + string.Join(Environment.NewLine, broken.Select(b => "  " + b)) +
            Environment.NewLine + outcome);
}
