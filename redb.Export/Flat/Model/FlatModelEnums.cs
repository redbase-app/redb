namespace redb.Export.Flat.Model;

/// <summary>What a flat table holds.</summary>
public enum FlatTableKind
{
    /// <summary>The shared header table (<c>objects</c>).</summary>
    Objects,

    /// <summary>The flat copy of <c>_lists</c>.</summary>
    Lists,

    /// <summary>The flat copy of <c>_list_items</c>.</summary>
    ListItems,

    /// <summary>Props of one scheme; one row per object, keyed by the object id.</summary>
    Scheme,

    /// <summary>A nested class outside a collection; one-to-one with its owner row.</summary>
    NestedClass,

    /// <summary>Elements of an array or a dictionary; many rows per owner row.</summary>
    Collection
}

/// <summary>Kind of the collection a <see cref="FlatTableKind.Collection"/> table stores.</summary>
public enum FlatCollectionKind
{
    /// <summary>Not a collection table.</summary>
    None,

    /// <summary>Array, <c>List&lt;T&gt;</c> and the other shapes scheme sync treats as an array.</summary>
    Array,

    /// <summary><c>Dictionary&lt;TKey, TValue&gt;</c> or <c>IDictionary&lt;TKey, TValue&gt;</c>.</summary>
    Dictionary
}

/// <summary>What one element of a collection is.</summary>
public enum FlatElementKind
{
    /// <summary>Not a collection table.</summary>
    None,

    /// <summary>A scalar stored in the <c>_value</c> column.</summary>
    Scalar,

    /// <summary>A class; its properties are the columns of the collection table.</summary>
    Class,

    /// <summary>A <c>RedbObject&lt;T&gt;</c>; <c>_value</c> holds the target object id.</summary>
    ObjectReference,

    /// <summary>A <c>RedbListItem</c>; <c>_value</c> holds the list item id.</summary>
    ListItemReference
}

/// <summary>How a dictionary key is laid out in the flat table.</summary>
public enum FlatKeyKind
{
    /// <summary>Not a dictionary table.</summary>
    None,

    /// <summary>A simple key in one typed <c>_key</c> column.</summary>
    Simple,

    /// <summary>A <c>ValueTuple</c> of simple components, one <c>_key_ItemN</c> column per component.</summary>
    Tuple,

    /// <summary>Any other key; one text <c>_key</c> column with the JSON the key is stored as.</summary>
    Json
}

/// <summary>The part a column plays in its table.</summary>
public enum FlatColumnRole
{
    /// <summary>Primary key: the object id in a scheme table, the <c>_values._id</c> of the row elsewhere.</summary>
    PrimaryKey,

    /// <summary>Reference to the owner row.</summary>
    Owner,

    /// <summary>Position of an array element, read from <c>_array_index</c>.</summary>
    ArrayIndex,

    /// <summary>Dictionary key or one component of it, read from <c>_array_index</c>.</summary>
    DictionaryKey,

    /// <summary>A column of a service table, copied from the redb column of the same name.</summary>
    Header,

    /// <summary>A scalar Props value.</summary>
    Scalar,

    /// <summary>The id of the object a <c>RedbObject&lt;T&gt;</c> points to.</summary>
    ObjectReference,

    /// <summary>The id of the list item a <c>RedbListItem</c> points to.</summary>
    ListItemReference
}

/// <summary>The <c>_values</c> column a payload value is read from.</summary>
public enum FlatValueSource
{
    /// <summary>Not read from a typed <c>_values</c> column.</summary>
    None,

    /// <summary><c>_String</c>.</summary>
    String,

    /// <summary><c>_Long</c>.</summary>
    Long,

    /// <summary><c>_Guid</c>.</summary>
    Guid,

    /// <summary><c>_Double</c>.</summary>
    Double,

    /// <summary><c>_DateTimeOffset</c>.</summary>
    DateTimeOffset,

    /// <summary><c>_Boolean</c>.</summary>
    Boolean,

    /// <summary><c>_ByteArray</c>.</summary>
    ByteArray,

    /// <summary><c>_Numeric</c>.</summary>
    Numeric,

    /// <summary><c>_ListItem</c>.</summary>
    ListItem,

    /// <summary><c>_Object</c>.</summary>
    Object
}
