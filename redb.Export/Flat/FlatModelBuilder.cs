using redb.Core.Models.Entities;
using redb.Core.Utils;
using redb.Export.Flat.Model;

namespace redb.Export.Flat;

/// <summary>
/// Builds the flat model of a set of Props types: the shared header table, the two list tables and,
/// for every Props type, its scheme table with the nested-class and collection tables below it.
/// <para>
/// The walk follows scheme synchronisation property by property, so the tables line up with the
/// structure tree: pass the tree of each scheme and every table and column is bound to its
/// <c>_structures._id</c>. Without the trees the model is complete except for those ids - enough to
/// review the shape of the tables.
/// </para>
/// </summary>
public sealed class FlatModelBuilder
{
    /// <summary>
    /// Builds the model.
    /// </summary>
    /// <param name="propsTypes">Props types to flatten, each becoming one scheme table.</param>
    /// <param name="structureTrees">
    /// Root nodes of the structure tree of each Props type (see
    /// <see cref="StructureTreeBuilder.BuildFromFlat"/>). Optional; a type without a tree is built
    /// with no structure ids.
    /// </param>
    public FlatDatabaseModel Build(
        IEnumerable<Type> propsTypes,
        IReadOnlyDictionary<Type, IReadOnlyList<StructureTreeNode>>? structureTrees = null)
    {
        ArgumentNullException.ThrowIfNull(propsTypes);

        var tables = new List<FlatTable>
        {
            BuildObjectsTable(),
            BuildListsTable(),
            BuildListItemsTable()
        };

        foreach (var propsType in propsTypes)
        {
            IReadOnlyList<StructureTreeNode>? roots = null;
            if (structureTrees != null)
                structureTrees.TryGetValue(propsType, out roots);

            BuildSchemeTable(propsType, roots, tables);
        }

        return new FlatDatabaseModel(tables);
    }

    // ---------------------------------------------------------------------------------------
    // Service tables: column for column the redb tables, minus the hashes.
    // ---------------------------------------------------------------------------------------

    private static FlatTable BuildObjectsTable()
    {
        var table = new FlatTable(FlatNaming.ObjectsTable) { Kind = FlatTableKind.Objects };

        table.Add(Header("_id", typeof(long), nullable: false, FlatColumnRole.PrimaryKey));
        table.Add(Header("_id_parent", typeof(long), nullable: true));
        table.Add(Header("_id_scheme", typeof(long), nullable: false));
        table.Add(Header("_id_owner", typeof(long), nullable: false));
        table.Add(Header("_id_who_change", typeof(long), nullable: false));
        table.Add(Header("_date_create", typeof(DateTimeOffset), nullable: false));
        table.Add(Header("_date_modify", typeof(DateTimeOffset), nullable: false));
        table.Add(Header("_date_begin", typeof(DateTimeOffset), nullable: true));
        table.Add(Header("_date_complete", typeof(DateTimeOffset), nullable: true));
        table.Add(Header("_key", typeof(long), nullable: true));
        table.Add(Header("_name", typeof(string), nullable: true));
        table.Add(Header("_note", typeof(string), nullable: true));
        table.Add(Header("_value_long", typeof(long), nullable: true));
        table.Add(Header("_value_string", typeof(string), nullable: true));
        table.Add(Header("_value_guid", typeof(Guid), nullable: true));
        table.Add(Header("_value_bool", typeof(bool), nullable: true));
        table.Add(Header("_value_double", typeof(double), nullable: true));
        table.Add(Header("_value_numeric", typeof(decimal), nullable: true));
        table.Add(Header("_value_datetime", typeof(DateTimeOffset), nullable: true));
        table.Add(Header("_value_bytes", typeof(byte[]), nullable: true));

        // The parent of an object may be of any scheme; the header table holds them all.
        table.Add(ForeignKey(table, "_id_parent", FlatNaming.ObjectsTable));
        return table;
    }

    private static FlatTable BuildListsTable()
    {
        var table = new FlatTable(FlatNaming.ListsTable) { Kind = FlatTableKind.Lists };

        table.Add(Header("_id", typeof(long), nullable: false, FlatColumnRole.PrimaryKey));
        table.Add(Header("_name", typeof(string), nullable: false));
        table.Add(Header("_alias", typeof(string), nullable: true));
        return table;
    }

    private static FlatTable BuildListItemsTable()
    {
        var table = new FlatTable(FlatNaming.ListItemsTable) { Kind = FlatTableKind.ListItems };

        table.Add(Header("_id", typeof(long), nullable: false, FlatColumnRole.PrimaryKey));
        table.Add(Header("_id_list", typeof(long), nullable: false));
        table.Add(Header("_value", typeof(string), nullable: true));
        table.Add(Header("_alias", typeof(string), nullable: true));
        table.Add(Header("_id_object", typeof(long), nullable: true));

        table.Add(ForeignKey(table, "_id_list", FlatNaming.ListsTable));
        table.Add(ForeignKey(table, "_id_object", FlatNaming.ObjectsTable));
        return table;
    }

    // ---------------------------------------------------------------------------------------
    // Scheme tables
    // ---------------------------------------------------------------------------------------

    private static void BuildSchemeTable(Type propsType, IReadOnlyList<StructureTreeNode>? roots, List<FlatTable> tables)
    {
        var table = new FlatTable(propsType.Name)
        {
            Kind = FlatTableKind.Scheme,
            PropsType = propsType,
            ClassType = propsType
        };

        // The key of a scheme row is the object id, and the header of that object lives in `objects`.
        table.Add(Service(FlatNaming.IdColumn, FlatColumnRole.PrimaryKey, typeof(long)));
        table.Add(ForeignKey(table, FlatNaming.IdColumn, FlatNaming.ObjectsTable));
        tables.Add(table);

        AddMembers(table, propsType, roots, propsType, path: null, [], tables);
    }

    /// <summary>
    /// Adds the properties of <paramref name="type"/> to <paramref name="table"/>: scalars and single
    /// references as columns, nested classes and collections as tables of their own.
    /// </summary>
    private static void AddMembers(
        FlatTable table,
        Type type,
        IReadOnlyList<StructureTreeNode>? nodes,
        Type propsType,
        string? path,
        HashSet<Type> typesOnPath,
        List<FlatTable> tables)
    {
        // Scheme sync does not descend into a type already on the path; the structure of the
        // property exists, its children do not.
        if (!typesOnPath.Add(type))
            return;

        foreach (var property in FlatTypeClassifier.StoredProperties(type))
        {
            var node = nodes?.FirstOrDefault(n => n.Structure.Name == property.Name);
            var structureId = node?.Structure.Id;
            var children = node?.Children;
            var memberPath = path == null ? property.Name : $"{path}.{property.Name}";
            var description = $"Property '{propsType.Name}.{memberPath}'";
            var propertyType = property.PropertyType;

            if (FlatTypeClassifier.IsDictionary(propertyType))
            {
                var (keyType, valueType) = FlatTypeClassifier.DictionaryTypes(propertyType);
                BuildCollectionTable(table, property.Name, memberPath, description, valueType, keyType,
                    structureId, children, propsType, typesOnPath, tables);
            }
            else if (FlatTypeClassifier.IsArray(propertyType))
            {
                BuildCollectionTable(table, property.Name, memberPath, description,
                    FlatTypeClassifier.ArrayElementType(propertyType), keyType: null,
                    structureId, children, propsType, typesOnPath, tables);
            }
            else if (FlatTypeClassifier.IsObjectReference(propertyType, description))
            {
                var target = propertyType.GetGenericArguments()[0];
                table.Add(Payload(property.Name, FlatColumnRole.ObjectReference, typeof(long), FlatValueSource.Object, structureId));
                table.Add(ForeignKey(table, property.Name, target.Name, target));
            }
            else if (FlatTypeClassifier.IsListItem(propertyType))
            {
                table.Add(Payload(property.Name, FlatColumnRole.ListItemReference, typeof(long), FlatValueSource.ListItem, structureId));
                table.Add(ForeignKey(table, property.Name, FlatNaming.ListItemsTable));
            }
            else if (FlatTypeClassifier.IsBusinessClass(propertyType))
            {
                var nested = new FlatTable(FlatNaming.ChildTable(table.FullName, property.Name))
                {
                    Kind = FlatTableKind.NestedClass,
                    PropsType = propsType,
                    ClassType = propertyType,
                    PropertyPath = memberPath,
                    OwnerTable = table.Name,
                    StructureId = structureId
                };
                AddKeyAndOwner(nested, table);
                // One nested instance per owner row.
                nested.Add(Unique(nested, FlatNaming.OwnerColumn));
                tables.Add(nested);

                AddMembers(nested, propertyType, children, propsType, memberPath, typesOnPath, tables);
            }
            else
            {
                table.Add(Payload(property.Name, FlatColumnRole.Scalar, propertyType,
                    FlatTypeClassifier.ScalarSource(propertyType), structureId));
            }
        }

        typesOnPath.Remove(type);
    }

    private static void BuildCollectionTable(
        FlatTable owner,
        string propertyName,
        string memberPath,
        string description,
        Type elementType,
        Type? keyType,
        long? structureId,
        IReadOnlyList<StructureTreeNode>? children,
        Type propsType,
        HashSet<Type> typesOnPath,
        List<FlatTable> tables)
    {
        FlatElementKind elementKind;
        if (FlatTypeClassifier.IsObjectReference(elementType, description))
            elementKind = FlatElementKind.ObjectReference;
        else if (FlatTypeClassifier.IsListItem(elementType))
            elementKind = FlatElementKind.ListItemReference;
        else if (FlatTypeClassifier.IsBusinessClass(elementType))
            elementKind = FlatElementKind.Class;
        else
            elementKind = FlatElementKind.Scalar;

        var keyKind = keyType == null ? FlatKeyKind.None : ClassifyKey(keyType);

        var table = new FlatTable(FlatNaming.ChildTable(owner.FullName, propertyName))
        {
            Kind = FlatTableKind.Collection,
            PropsType = propsType,
            ClassType = elementKind == FlatElementKind.Class ? elementType : null,
            PropertyPath = memberPath,
            OwnerTable = owner.Name,
            StructureId = structureId,
            CollectionKind = keyType == null ? FlatCollectionKind.Array : FlatCollectionKind.Dictionary,
            ElementKind = elementKind,
            ElementType = elementType,
            KeyType = keyType,
            KeyKind = keyKind
        };
        AddKeyAndOwner(table, owner);

        // Position or key: what tells the elements of one owner apart.
        var identity = new List<string> { FlatNaming.OwnerColumn };
        switch (keyKind)
        {
            case FlatKeyKind.None:
                table.Add(Service(FlatNaming.IndexColumn, FlatColumnRole.ArrayIndex, typeof(int)));
                identity.Add(FlatNaming.IndexColumn);
                break;

            case FlatKeyKind.Simple:
                table.Add(Service(FlatNaming.KeyColumn, FlatColumnRole.DictionaryKey, keyType!));
                identity.Add(FlatNaming.KeyColumn);
                break;

            case FlatKeyKind.Tuple:
                foreach (var field in keyType!.GetFields())
                {
                    var name = FlatNaming.Shorten($"{FlatNaming.KeyColumn}_{field.Name}");
                    table.Add(new FlatColumn
                    {
                        Name = name,
                        Role = FlatColumnRole.DictionaryKey,
                        ClrType = field.FieldType,
                        IsNullable = false,
                        KeyComponent = field.Name
                    });
                    identity.Add(name);
                }
                break;

            default:
                table.Add(Service(FlatNaming.KeyColumn, FlatColumnRole.DictionaryKey, typeof(string)));
                identity.Add(FlatNaming.KeyColumn);
                break;
        }
        table.Add(Unique(table, [.. identity]));
        tables.Add(table);

        switch (elementKind)
        {
            case FlatElementKind.ObjectReference:
                var target = elementType.GetGenericArguments()[0];
                table.Add(Payload(FlatNaming.ValueColumn, FlatColumnRole.ObjectReference, typeof(long), FlatValueSource.Object, structureId));
                table.Add(ForeignKey(table, FlatNaming.ValueColumn, target.Name, target));
                break;

            case FlatElementKind.ListItemReference:
                table.Add(Payload(FlatNaming.ValueColumn, FlatColumnRole.ListItemReference, typeof(long), FlatValueSource.ListItem, structureId));
                table.Add(ForeignKey(table, FlatNaming.ValueColumn, FlatNaming.ListItemsTable));
                break;

            case FlatElementKind.Class:
                // The element row is the class instance: its properties are columns of this table.
                AddMembers(table, elementType, children, propsType, memberPath, typesOnPath, tables);
                break;

            default:
                table.Add(Payload(FlatNaming.ValueColumn, FlatColumnRole.Scalar, elementType,
                    FlatTypeClassifier.ScalarSource(elementType), structureId));
                break;
        }
    }

    /// <summary>
    /// A simple key is stored as plain text and gets one typed column. A <c>ValueTuple</c> of simple
    /// components is stored as JSON keyed by <c>Item1</c>, <c>Item2</c>, ... and gets one column per
    /// component. Everything else stays one text column.
    /// </summary>
    private static FlatKeyKind ClassifyKey(Type keyType)
    {
        if (!RedbKeySerializer.IsComplexKey(keyType))
            return FlatKeyKind.Simple;

        if (FlatTypeClassifier.IsValueTuple(keyType) &&
            keyType.GetFields().All(f => !RedbKeySerializer.IsComplexKey(f.FieldType)))
            return FlatKeyKind.Tuple;

        return FlatKeyKind.Json;
    }

    // ---------------------------------------------------------------------------------------
    // Pieces
    // ---------------------------------------------------------------------------------------

    private static void AddKeyAndOwner(FlatTable table, FlatTable owner)
    {
        table.Add(Service(FlatNaming.IdColumn, FlatColumnRole.PrimaryKey, typeof(long)));
        table.Add(Service(FlatNaming.OwnerColumn, FlatColumnRole.Owner, typeof(long)));
        table.Add(ForeignKey(table, FlatNaming.OwnerColumn, owner.FullName));
    }

    private static FlatColumn Header(string name, Type clrType, bool nullable, FlatColumnRole role = FlatColumnRole.Header) => new()
    {
        Name = name,
        Role = role,
        ClrType = clrType,
        IsNullable = nullable,
        SourceColumn = name
    };

    private static FlatColumn Service(string name, FlatColumnRole role, Type clrType) => new()
    {
        Name = name,
        Role = role,
        ClrType = clrType,
        IsNullable = false
    };

    private static FlatColumn Payload(string propertyName, FlatColumnRole role, Type clrType, FlatValueSource source, long? structureId) => new()
    {
        Name = FlatNaming.Shorten(propertyName),
        Role = role,
        ClrType = clrType,
        IsNullable = true,
        ValueSource = source,
        StructureId = structureId
    };

    private static FlatForeignKey ForeignKey(FlatTable table, string column, string targetFullName, Type? targetPropsType = null) => new()
    {
        Name = FlatNaming.Shorten($"FK_{table.FullName}_{column}"),
        Column = FlatNaming.Shorten(column),
        TargetTable = FlatNaming.Shorten(targetFullName),
        TargetColumn = FlatNaming.IdColumn,
        TargetPropsType = targetPropsType
    };

    private static FlatUniqueConstraint Unique(FlatTable table, params string[] columns) => new()
    {
        Name = FlatNaming.Shorten($"UQ_{table.FullName}"),
        Columns = columns
    };
}
