namespace redb.Core.Utils;

/// <summary>
/// System type IDs for REDB. Matches _types table in database.
/// Negative IDs are reserved for system types.
/// </summary>
public static class RedbTypeIds
{
    // ═══════════════════════════════════════════════════════════════
    // PRIMITIVE TYPES (basic C# types)
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>String type - maps to _String column in _values</summary>
    public const long String = -9223372036854775700;
    
    /// <summary>Long type - maps to _Long column in _values</summary>
    public const long Long = -9223372036854775704;
    
    /// <summary>Guid type - maps to _Guid column in _values</summary>
    public const long Guid = -9223372036854775705;
    
    /// <summary>Double type - maps to _Double column in _values</summary>
    public const long Double = -9223372036854775707;
    
    /// <summary>Boolean type - maps to _Boolean column in _values</summary>
    public const long Boolean = -9223372036854775709;
    
    /// <summary>ByteArray type - maps to _ByteArray column in _values</summary>
    public const long ByteArray = -9223372036854775701;
    
    // ═══════════════════════════════════════════════════════════════
    // EXTENDED TYPES
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>DateTime type - maps to _DateTimeOffset (timestamptz) in database</summary>
    public const long DateTime = -9223372036854775708;
    
    /// <summary>DateTimeOffset type - maps to _DateTimeOffset (timestamptz) in database</summary>
    public const long DateTimeOffset = -9223372036854775673;
    
    /// <summary>Numeric type for precise decimal numbers - maps to _Numeric column</summary>
    public const long Numeric = -9223372036854775674;
    
    /// <summary>ListItem type - reference to _list_items table</summary>
    public const long ListItem = -9223372036854775706;
    
    /// <summary>Object type - reference to another _objects record (redbObject)</summary>
    public const long Object = -9223372036854775703;
    
    // ═══════════════════════════════════════════════════════════════
    // ADDITIONAL PRIMITIVE TYPES (mapped to base types)
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>Int type - maps to _Long column</summary>
    public const long Int = -9223372036854775699;
    
    /// <summary>Short type - maps to _Long column</summary>
    public const long Short = -9223372036854775698;
    
    /// <summary>Byte type - maps to _Long column</summary>
    public const long Byte = -9223372036854775697;
    
    /// <summary>Float type - maps to _Double column</summary>
    public const long Float = -9223372036854775696;
    
    /// <summary>Decimal type - maps to _Numeric column</summary>
    public const long Decimal = -9223372036854775695;
    
    /// <summary>Char type - maps to _String column</summary>
    public const long Char = -9223372036854775694;
    
    // ═══════════════════════════════════════════════════════════════
    // NESTED CLASS TYPE
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>
    /// Class type for nested objects (Address, Contact, etc.)
    /// Child fields stored in _values with _id_parent reference
    /// </summary>
    public const long Class = -9223372036854775675;
    
    // ═══════════════════════════════════════════════════════════════
    // COLLECTION TYPES (NEW! For RedbObject<T[]> and Dictionary<K,V>)
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>
    /// Array type for RedbObject&lt;T[]&gt; / RedbObject&lt;List&lt;T&gt;&gt;.
    /// Also used for _structures._collection_type to mark array fields.
    /// </summary>
    public const long Array = -9223372036854775668;
    
    /// <summary>
    /// Dictionary type for RedbObject&lt;Dictionary&lt;K,V&gt;&gt;.
    /// Also used for _structures._collection_type to mark dictionary fields.
    /// </summary>
    public const long Dictionary = -9223372036854775667;
    
    // ═══════════════════════════════════════════════════════════════
    // DOCUMENT TYPES (hierarchy stored via _array_parent_id)
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>
    /// JsonDocument type for RedbObject&lt;JsonDocument&gt;.
    /// Hierarchy stored via _values._array_parent_id.
    /// </summary>
    public const long JsonDocument = -9223372036854775666;
    
    /// <summary>
    /// XDocument type for RedbObject&lt;XDocument&gt;.
    /// Hierarchy stored via _values._array_parent_id.
    /// </summary>
    public const long XDocument = -9223372036854775665;
    
    // ═══════════════════════════════════════════════════════════════
    // SPECIALIZED STRING TYPES
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>Url type - validated string</summary>
    public const long Url = -9223372036854775693;
    
    /// <summary>Email type - validated string</summary>
    public const long Email = -9223372036854775692;
    
    /// <summary>Phone type - validated string</summary>
    public const long Phone = -9223372036854775691;
    
    /// <summary>Json type - JSON as string</summary>
    public const long Json = -9223372036854775690;
    
    /// <summary>Xml type - XML as string</summary>
    public const long Xml = -9223372036854775689;
    
    /// <summary>Base64 type - Base64 encoded string</summary>
    public const long Base64 = -9223372036854775688;
    
    /// <summary>Color type - color representation string</summary>
    public const long Color = -9223372036854775687;
    
    // ═══════════════════════════════════════════════════════════════
    // TIME TYPES
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>DateOnly type (.NET 6+)</summary>
    public const long DateOnly = -9223372036854775686;
    
    /// <summary>TimeOnly type (.NET 6+) - stored as string</summary>
    public const long TimeOnly = -9223372036854775685;
    
    /// <summary>TimeSpan type - stored as string (HH:MM:SS format)</summary>
    public const long TimeSpan = -9223372036854775684;
    
    // ═══════════════════════════════════════════════════════════════
    // ENUM TYPES
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>Enum type - stored as string</summary>
    public const long Enum = -9223372036854775683;
    
    /// <summary>EnumInt type - stored as number</summary>
    public const long EnumInt = -9223372036854775682;
    
    // ═══════════════════════════════════════════════════════════════
    // GEO TYPES
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>Latitude type</summary>
    public const long Latitude = -9223372036854775681;
    
    /// <summary>Longitude type</summary>
    public const long Longitude = -9223372036854775680;
    
    /// <summary>GeoPoint type - JSON string</summary>
    public const long GeoPoint = -9223372036854775679;
    
    // ═══════════════════════════════════════════════════════════════
    // FILE TYPES
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>FilePath type</summary>
    public const long FilePath = -9223372036854775678;
    
    /// <summary>FileName type</summary>
    public const long FileName = -9223372036854775677;
    
    /// <summary>MimeType type</summary>
    public const long MimeType = -9223372036854775676;
    
    // ═══════════════════════════════════════════════════════════════
    // SOFT DELETE SYSTEM (reserved IDs)
    // ═══════════════════════════════════════════════════════════════
    
    /// <summary>
    /// Reserved scheme ID for deleted objects (@@__deleted).
    /// Objects marked for soft-deletion get this scheme ID.
    /// </summary>
    public const long DeletedScheme = -10;
    
    /// <summary>
    /// Reserved scheme name for deleted objects.
    /// Uses @@ prefix to bypass validation rules.
    /// </summary>
    public const string DeletedSchemeName = "@@__deleted";
}

