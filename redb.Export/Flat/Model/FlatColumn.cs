namespace redb.Export.Flat.Model;

/// <summary>
/// One column of a flat table, described independently of any SQL dialect: the target column type
/// is chosen later from <see cref="ClrType"/>.
/// </summary>
public sealed class FlatColumn
{
    /// <summary>Column name as it goes into DDL (already within the identifier limit).</summary>
    public string Name { get; init; } = "";

    /// <summary>The part the column plays in its table.</summary>
    public FlatColumnRole Role { get; init; }

    /// <summary>
    /// CLR type the column carries: the property type for Props values (nullable wrappers kept),
    /// <see cref="long"/> for keys and references.
    /// </summary>
    public Type ClrType { get; init; } = typeof(object);

    /// <summary>Whether the column accepts NULL. Every Props column does: an object saved before a field existed has no row for it.</summary>
    public bool IsNullable { get; init; }

    /// <summary>The <c>_values</c> column the value is read from; <see cref="FlatValueSource.None"/> for service columns.</summary>
    public FlatValueSource ValueSource { get; init; }

    /// <summary>
    /// <c>_structures._id</c> of the field. Null for service columns, and for every column when the
    /// model was built without a structure tree.
    /// </summary>
    public long? StructureId { get; init; }

    /// <summary>For a <see cref="FlatColumnRole.Header"/> column: the redb column it is copied from.</summary>
    public string? SourceColumn { get; init; }

    /// <summary>For one component of a tuple key: the field of the tuple (<c>Item1</c>, <c>Item2</c>, ...), as stored in the key JSON.</summary>
    public string? KeyComponent { get; init; }
}
