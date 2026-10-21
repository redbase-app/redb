namespace redb.Export.Flat.Model;

/// <summary>
/// One flat table: its columns and constraints, plus where its rows come from in redb.
/// </summary>
public sealed class FlatTable
{
    private readonly List<FlatColumn> _columns = [];
    private readonly List<FlatForeignKey> _foreignKeys = [];
    private readonly List<FlatUniqueConstraint> _uniqueConstraints = [];
    private readonly List<FlatIndex> _indexes = [];

    /// <summary>Creates a table; <paramref name="fullName"/> is shortened to the identifier limit when needed.</summary>
    public FlatTable(string fullName)
    {
        FullName = fullName;
        Name = FlatNaming.Shorten(fullName);
        PrimaryKeyName = FlatNaming.Shorten($"PK_{fullName}");
    }

    /// <summary>Table name as it goes into DDL.</summary>
    public string Name { get; }

    /// <summary>Name before shortening: the class name or the full property path.</summary>
    public string FullName { get; }

    /// <summary>What the table holds.</summary>
    public FlatTableKind Kind { get; init; }

    /// <summary>The Props type of the scheme the table belongs to; null for service tables.</summary>
    public Type? PropsType { get; init; }

    /// <summary>The class whose properties are the columns: the Props type, a nested class, or the element class of a collection.</summary>
    public Type? ClassType { get; init; }

    /// <summary>Property path from the Props type (<c>HomeAddress.Building</c>); null for scheme and service tables.</summary>
    public string? PropertyPath { get; init; }

    /// <summary>Name of the table whose rows own the rows of this one; null for scheme and service tables.</summary>
    public string? OwnerTable { get; init; }

    /// <summary>
    /// <c>_structures._id</c> of the field the table is built from: the class field of a nested
    /// table, the collection field of a collection table. Null for scheme and service tables, and
    /// when the model was built without a structure tree.
    /// </summary>
    public long? StructureId { get; init; }

    /// <summary>Array or dictionary, for a collection table.</summary>
    public FlatCollectionKind CollectionKind { get; init; }

    /// <summary>What one element is, for a collection table.</summary>
    public FlatElementKind ElementKind { get; init; }

    /// <summary>CLR type of one element (the value type of a dictionary), for a collection table.</summary>
    public Type? ElementType { get; init; }

    /// <summary>CLR type of the key, for a dictionary table.</summary>
    public Type? KeyType { get; init; }

    /// <summary>How the key is laid out, for a dictionary table.</summary>
    public FlatKeyKind KeyKind { get; init; }

    /// <summary>Name of the primary key column.</summary>
    public string PrimaryKey { get; init; } = FlatNaming.IdColumn;

    /// <summary>Name of the primary key constraint.</summary>
    public string PrimaryKeyName { get; }

    /// <summary>Columns in declaration order.</summary>
    public IReadOnlyList<FlatColumn> Columns => _columns;

    /// <summary>Foreign keys. They are meant to be created after every table exists: a reference may point to a table that comes later.</summary>
    public IReadOnlyList<FlatForeignKey> ForeignKeys => _foreignKeys;

    /// <summary>Unique constraints.</summary>
    public IReadOnlyList<FlatUniqueConstraint> UniqueConstraints => _uniqueConstraints;

    internal void Add(FlatColumn column) => _columns.Add(column);

    internal void Add(FlatForeignKey foreignKey) => _foreignKeys.Add(foreignKey);

    /// <summary>Plain indexes. Like foreign keys, they are meant to be created after the data is loaded.</summary>
    public IReadOnlyList<FlatIndex> Indexes => _indexes;

    internal void Add(FlatUniqueConstraint uniqueConstraint) => _uniqueConstraints.Add(uniqueConstraint);

    internal void Add(FlatIndex index) => _indexes.Add(index);
}

/// <summary>
/// The whole flat model: the three service tables followed by the tables of every scheme, each
/// owner before the tables it owns.
/// </summary>
public sealed class FlatDatabaseModel
{
    /// <summary>Creates the model over an ordered list of tables.</summary>
    public FlatDatabaseModel(IReadOnlyList<FlatTable> tables)
    {
        Tables = tables;
    }

    /// <summary>Tables in creation order.</summary>
    public IReadOnlyList<FlatTable> Tables { get; }
}
