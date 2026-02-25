using redb.Core.Query;

namespace redb.MSSql.Sql;

/// <summary>
/// MS SQL Server implementation of ISqlDialect.
/// Uses SQL Server 2016+ features (JSON, STRING_SPLIT, sequences).
/// Parameters use @p0, @p1 format (converted from $1, $2 in SqlRedbConnection).
/// </summary>
public class MsSqlDialect : ISqlDialect
{
    public string ProviderName => "MSSql";
    
    /// <summary>
    /// MSSQL pagination: OFFSET n ROWS FETCH NEXT m ROWS ONLY.
    /// Requires ORDER BY clause in the query.
    /// </summary>
    public string FormatPagination(int? limit, int? offset)
    {
        if (!limit.HasValue && !offset.HasValue) return string.Empty;
        
        var off = offset ?? 0;
        var lim = limit ?? int.MaxValue;
        
        return $"OFFSET {off} ROWS FETCH NEXT {lim} ROWS ONLY";
    }
    
    public string WrapSubquery(string subquery, string alias)
        => $"({subquery}) AS {alias}";
    
    /// <summary>
    /// PVT via subquery with TOP 1 (MSSQL doesn't have array_agg FILTER).
    /// </summary>
    public string FormatPvtColumn(long structureId, string dbColumn, string alias)
        => $"(SELECT TOP 1 {dbColumn} FROM _values v2 WHERE v2._id_object = v._id_object AND v2._id_structure = {structureId}) AS [{alias}]";
    
    /// <summary>
    /// MSSQL uses IN with STRING_SPLIT for array containment.
    /// </summary>
    public string FormatArrayContains(string column, string paramName)
        => $"{column} IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT({paramName}, ','))";
    
    public string GetDbTypeName(string redbType) => redbType switch
    {
        "Long" => "bigint",
        "String" => "nvarchar(max)",
        "Boolean" => "bit",
        "DateTime" => "datetimeoffset",
        "DateTimeOffset" => "datetimeoffset",
        "Double" => "float",
        "Numeric" => "decimal(38,18)",
        "Guid" => "uniqueidentifier",
        "ByteArray" => "varbinary(max)",
        _ => "nvarchar(max)"
    };
    
    /// <summary>
    /// MSSQL parameter format (@p0, @p1, etc.).
    /// Note: SqlRedbConnection converts $1 to @p0 automatically.
    /// </summary>
    public string FormatParameter(int index)
        => $"@p{index - 1}";
    
    public string QuoteIdentifier(string name)
        => $"[{name}]";
    
    // ============================================================
    // === ROLES SQL ===
    // ============================================================
    
    public string Roles_SelectById() =>
        "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles WHERE _id = @p0";
    
    public string Roles_SelectByName() =>
        "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles WHERE _name = @p0";
    
    public string Roles_SelectAll() =>
        "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles ORDER BY _name";
    
    public string Roles_Insert() =>
        "INSERT INTO _roles (_id, _name) VALUES (@p0, @p1)";
    
    public string Roles_UpdateName() =>
        "UPDATE _roles SET _name = @p0 WHERE _id = @p1";
    
    public string Roles_Delete() =>
        "DELETE FROM _roles WHERE _id = @p0";
    
    public string Roles_ExistsByName() =>
        "SELECT _id FROM _roles WHERE _name = @p0";
    
    public string Roles_ExistsByNameExcluding() =>
        "SELECT _id FROM _roles WHERE _name = @p0 AND _id != @p1";
    
    public string Roles_Count() =>
        "SELECT COUNT(*) FROM _roles";
    
    public string Roles_UpdateConfiguration() =>
        "UPDATE _roles SET _id_configuration = @p0 WHERE _id = @p1";
    
    public string Roles_SelectConfigurationId() =>
        "SELECT _id_configuration FROM _roles WHERE _id = @p0";
    
    // ============================================================
    // === USERS_ROLES SQL ===
    // ============================================================
    
    public string UsersRoles_Insert() =>
        "INSERT INTO _users_roles (_id, _id_user, _id_role) VALUES (@p0, @p1, @p2)";
    
    public string UsersRoles_Delete() =>
        "DELETE FROM _users_roles WHERE _id_user = @p0 AND _id_role = @p1";
    
    public string UsersRoles_DeleteByUser() =>
        "DELETE FROM _users_roles WHERE _id_user = @p0";
    
    public string UsersRoles_DeleteByRole() =>
        "DELETE FROM _users_roles WHERE _id_role = @p0";
    
    public string UsersRoles_Exists() =>
        "SELECT _id FROM _users_roles WHERE _id_user = @p0 AND _id_role = @p1";
    
    public string UsersRoles_SelectRolesByUser() =>
        """
        SELECT r._id AS Id, r._name AS Name, r._id_configuration AS IdConfiguration
        FROM _roles r
        INNER JOIN _users_roles ur ON ur._id_role = r._id
        WHERE ur._id_user = @p0
        ORDER BY r._name
        """;
    
    public string UsersRoles_SelectUsersByRole() =>
        """
        SELECT u._id, u._login, u._name, u._password, u._phone, u._email,
               u._enabled, u._date_register, u._date_dismiss,
               u._key, u._code_int, u._code_string, u._code_guid, u._note, u._hash
        FROM _users u
        INNER JOIN _users_roles ur ON ur._id_user = u._id
        WHERE ur._id_role = @p0
        ORDER BY u._name
        """;
    
    public string UsersRoles_CountByRole() =>
        "SELECT COUNT(*) FROM _users_roles WHERE _id_role = @p0";
    
    // ============================================================
    // === USERS SQL ===
    // ============================================================
    
    public string Users_ExistsById() =>
        "SELECT _id FROM _users WHERE _id = @p0";
    
    public string Users_SelectIdByLogin() =>
        "SELECT _id FROM _users WHERE _login = @p0";
    
    // ============================================================
    // === PERMISSIONS SQL ===
    // ============================================================
    
    public string Permissions_DeleteByRole() =>
        "DELETE FROM _permissions WHERE _id_role = @p0";
    
    /// <summary>
    /// Uses MSSQL function get_user_permissions_for_object() from redb_permissions.sql.
    /// </summary>
    public string Permissions_GetEffectiveForObject() =>
        "SELECT * FROM dbo.get_user_permissions_for_object(@p0, @p1)";
    
    public string Permissions_SelectReadableObjectIds() =>
        """
        SELECT DISTINCT p.object_id 
        FROM dbo.get_user_permissions_for_object(NULL, @p0) p
        WHERE p.can_select = 1
        """;
    
    public string Permissions_Insert() =>
        """
        INSERT INTO _permissions (_id, _id_user, _id_role, _id_ref, _select, _insert, _update, _delete)
        VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7)
        """;
    
    public string Permissions_Update() =>
        "UPDATE _permissions SET _select = @p0, _insert = @p1, _update = @p2, _delete = @p3 WHERE _id = @p4";
    
    public string Permissions_Delete() =>
        "DELETE FROM _permissions WHERE _id = @p0";
    
    public string Permissions_SelectById() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef, 
               _select AS [Select], _insert AS [Insert], _update AS [Update], _delete AS [Delete]
        FROM _permissions WHERE _id = @p0
        """;
    
    public string Permissions_SelectByUser() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS [Select], _insert AS [Insert], _update AS [Update], _delete AS [Delete]
        FROM _permissions WHERE _id_user = @p0
        """;
    
    public string Permissions_SelectByRole() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS [Select], _insert AS [Insert], _update AS [Update], _delete AS [Delete]
        FROM _permissions WHERE _id_role = @p0
        """;
    
    public string Permissions_SelectByObject() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS [Select], _insert AS [Insert], _update AS [Update], _delete AS [Delete]
        FROM _permissions WHERE _id_ref = @p0 OR _id_ref = 0
        """;
    
    /// <summary>
    /// MSSQL equivalent of IS NOT DISTINCT FROM.
    /// </summary>
    public string Permissions_SelectByUserRoleObject() =>
        """
        SELECT _id AS Id, _select AS [Select], _insert AS [Insert], _update AS [Update], _delete AS [Delete]
        FROM _permissions 
        WHERE ((_id_user = @p0) OR (_id_user IS NULL AND @p0 IS NULL)) 
          AND ((_id_role = @p1) OR (_id_role IS NULL AND @p1 IS NULL)) 
          AND _id_ref = @p2
        """;
    
    public string Permissions_DeleteByUserRoleObject() =>
        """
        DELETE FROM _permissions 
        WHERE ((_id_user = @p0) OR (_id_user IS NULL AND @p0 IS NULL)) 
          AND ((_id_role = @p1) OR (_id_role IS NULL AND @p1 IS NULL)) 
          AND _id_ref = @p2
        """;
    
    public string Permissions_DeleteByUser() =>
        "DELETE FROM _permissions WHERE _id_user = @p0";
    
    public string Permissions_Count() =>
        "SELECT COUNT(*) FROM _permissions";
    
    public string Permissions_CountByUser() =>
        "SELECT COUNT(*) FROM _permissions WHERE _id_user = @p0";
    
    public string Permissions_CountByRole() =>
        "SELECT COUNT(*) FROM _permissions WHERE _id_role = @p0";
    
    public string Permissions_SelectUserRoleIds() =>
        "SELECT _id_role AS IdRole FROM _users_roles WHERE _id_user = @p0";
    
    // ============================================================
    // === USERS SQL (full) ===
    // ============================================================
    
    public string Users_SelectById() =>
        """
        SELECT _id AS Id, _login AS Login, _name AS Name, _password AS Password, 
               _phone AS Phone, _email AS Email, _enabled AS Enabled,
               _date_register AS DateRegister, _date_dismiss AS DateDismiss,
               _key AS [Key], _code_int AS CodeInt, _code_string AS CodeString,
               _code_guid AS CodeGuid, _note AS Note, _hash AS Hash
        FROM _users WHERE _id = @p0
        """;
    
    public string Users_SelectByLogin() =>
        """
        SELECT _id AS Id, _login AS Login, _name AS Name, _password AS Password, 
               _phone AS Phone, _email AS Email, _enabled AS Enabled,
               _date_register AS DateRegister, _date_dismiss AS DateDismiss,
               _key AS [Key], _code_int AS CodeInt, _code_string AS CodeString,
               _code_guid AS CodeGuid, _note AS Note, _hash AS Hash
        FROM _users WHERE _login = @p0
        """;
    
    public string Users_Insert() =>
        """
        INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _enabled,
                           _date_register, _date_dismiss, _key, _code_int, _code_string,
                           _code_guid, _note, _hash)
        VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14)
        """;
    
    public string Users_Update() =>
        """
        UPDATE _users SET _login = @p0, _name = @p1, _phone = @p2, _email = @p3, _enabled = @p4,
                         _date_dismiss = @p5, _key = @p6, _code_int = @p7, _code_string = @p8,
                         _code_guid = @p9, _note = @p10, _hash = @p11
        WHERE _id = @p12
        """;
    
    public string Users_SoftDelete() =>
        "UPDATE _users SET _login = @p0, _name = @p1, _enabled = @p2, _date_dismiss = @p3 WHERE _id = @p4";
    
    public string Users_UpdatePassword() =>
        "UPDATE _users SET _password = @p0 WHERE _id = @p1";
    
    public string Users_UpdateStatus() =>
        "UPDATE _users SET _enabled = @p0, _date_dismiss = @p1 WHERE _id = @p2";
    
    public string Users_ExistsByLogin() =>
        "SELECT _id FROM _users WHERE _login = @p0";
    
    public string Users_ExistsByLoginExcluding() =>
        "SELECT _id FROM _users WHERE _login = @p0 AND _id != @p1";
    
    public string Users_ExistsByEmail() =>
        "SELECT TOP 1 _id FROM _users WHERE _email = @p0";
    
    public string Users_Count() =>
        "SELECT COUNT(*) FROM _users";
    
    public string Users_CountEnabled() =>
        "SELECT COUNT(*) FROM _users WHERE _enabled = 1";
    
    public string Users_SelectConfigurationId() =>
        "SELECT _id_configuration FROM _users WHERE _id = @p0";
    
    public string Users_UpdateConfiguration() =>
        "UPDATE _users SET _id_configuration = @p0 WHERE _id = @p1";
    
    public string Roles_SelectIdByName() =>
        "SELECT _id AS Id FROM _roles WHERE _name = @p0";
    
    public string Roles_ExistsById() =>
        "SELECT _id FROM _roles WHERE _id = @p0";
    
    // ============================================================
    // === SCHEMES SQL ===
    // ============================================================
    
    public string Schemes_SelectByName() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _name = @p0";
    
    public string Schemes_SelectById() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _id = @p0";
    
    public string Schemes_SelectAll() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes";
    
    public string Schemes_Insert() =>
        "INSERT INTO _schemes (_id, _name, _alias, _type) VALUES (@p0, @p1, @p2, @p3)";
    
    public string Schemes_UpdateHash() =>
        "UPDATE _schemes SET _structure_hash = @p0 WHERE _id = @p1";
    
    public string Schemes_UpdateName() =>
        "UPDATE _schemes SET _name = @p0 WHERE _id = @p1";
    
    public string Schemes_SelectHashById() =>
        "SELECT _structure_hash FROM _schemes WHERE _id = @p0";
    
    public string Schemes_ExistsByName() =>
        "SELECT TOP 1 _id FROM _schemes WHERE _name = @p0";
    
    public string Schemes_SelectObjectByName() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _name = @p0 AND _type = @p1";
    
    public string Schemes_InsertObject() =>
        "INSERT INTO _schemes (_id, _name, _type) VALUES (@p0, @p1, @p2)";
    
    // ============================================================
    // === STRUCTURES SQL ===
    // ============================================================
    
    public string Structures_SelectByScheme() =>
        """
        SELECT _id, _id_parent, _id_scheme, _id_override, _id_type, _id_list,
               _name, _alias, _order, _readonly, _allow_not_null, 
               _collection_type, _key_type, _is_compress, _store_null,
               _default_value, _default_editor
        FROM _structures WHERE _id_scheme = @p0
        """;
    
    public string Structures_SelectBySchemeShort() =>
        "SELECT _id, _id_parent, _id_scheme, _id_type, _name, _order FROM _structures WHERE _id_scheme = @p0";
    
    public string Structures_SelectBySchemeCacheable() =>
        """
        SELECT _id, _id_parent, _id_scheme, _id_type, _name, _alias, _order,
               _readonly, _allow_not_null, _collection_type, _key_type
        FROM _structures WHERE _id_scheme = @p0
        """;
    
    public string Structures_Insert() =>
        """
        INSERT INTO _structures (_id, _id_scheme, _id_parent, _name, _alias, _id_type, _allow_not_null, _collection_type, _key_type, _order)
        VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9)
        """;
    
    public string Structures_UpdateType() =>
        "UPDATE _structures SET _id_type = @p0 WHERE _id = @p1";
    
    public string Structures_UpdateCollectionType() =>
        "UPDATE _structures SET _collection_type = @p0 WHERE _id = @p1";
    
    public string Structures_UpdateKeyType() =>
        "UPDATE _structures SET _key_type = @p0 WHERE _id = @p1";
    
    public string Structures_UpdateAlias() =>
        "UPDATE _structures SET _alias = @p0 WHERE _id = @p1";
    
    public string Structures_UpdateAllowNotNull() =>
        "UPDATE _structures SET _allow_not_null = @p0 WHERE _id = @p1";
    
    public string Structures_DeleteByIds(IEnumerable<long> ids) =>
        $"DELETE FROM _structures WHERE _id IN ({string.Join(",", ids)})";
    
    // ============================================================
    // === TYPES SQL ===
    // ============================================================
    
    public string Types_SelectByName() =>
        "SELECT _id, _name FROM _types WHERE _name = @p0";
    
    public string Types_SelectAll() =>
        "SELECT _id AS Id, _name AS Name, _db_type AS DbType, _type AS Type1 FROM _types";
    
    // ============================================================
    // === SCHEME FUNCTIONS (MSSQL stored procedures) ===
    // ============================================================
    
    /// <summary>
    /// Sync metadata cache for scheme. Uses MSSQL stored procedure.
    /// </summary>
    public string Schemes_SyncMetadataCache() =>
        "EXEC dbo.sync_metadata_cache_for_scheme @p0";
    
    public string Schemes_MigrateStructureType() =>
        "EXEC dbo.migrate_structure_type @p0, @p1, @p2, @p3";
    
    public string Schemes_GetStructureTree() =>
        "SELECT dbo.get_scheme_structure_tree(@p0)";
    
    // ============================================================
    // === TREE SQL ===
    // ============================================================
    
    public string Tree_GetObjectJson() =>
        "SELECT dbo.get_object_json(@p0, @p1)";
    
    public string Tree_SelectChildrenJson() =>
        """
        SELECT dbo.get_object_json(o._id, 1) as json_data 
        FROM _objects o 
        WHERE o._id_parent = @p0 AND o._id_scheme = @p1
        """;
    
    public string Tree_SelectPolymorphicChildren() =>
        """
        SELECT o._id as ObjectId, o._id_scheme as SchemeId, dbo.get_object_json(o._id, 1) as JsonData
        FROM _objects o 
        WHERE o._id_parent = @p0 
        ORDER BY o._name, o._id
        """;
    
    public string Tree_SelectSchemeAndJson() =>
        """
        SELECT o._id_scheme as SchemeId, dbo.get_object_json(o._id, 1) as JsonData
        FROM _objects o WHERE o._id = @p0
        """;
    
    public string Tree_SelectChildrenBySchemeBase() =>
        """
        SELECT _id as Id, _id_parent as IdParent, _id_scheme as IdScheme, _name as Name, 
               _id_owner as IdOwner, _id_who_change as IdWhoChange,
               _date_create as DateCreate, _date_modify as DateModify, 
               _date_begin as DateBegin, _date_complete as DateComplete,
               _key as [Key], _value_long as ValueLong, _value_string as ValueString, 
               _value_guid as ValueGuid, _value_bool as ValueBool, _value_double as ValueDouble, 
               _value_numeric as ValueNumeric, _value_datetime as ValueDatetime, 
               _value_bytes as ValueBytes, _note as Note, _hash as Hash
        FROM _objects 
        WHERE _id_parent = @p0 AND _id_scheme = @p1
        ORDER BY _name, _id
        """;
    
    public string Tree_SelectChildrenBase() =>
        """
        SELECT _id as Id, _id_parent as IdParent, _id_scheme as IdScheme, _name as Name, 
               _id_owner as IdOwner, _id_who_change as IdWhoChange,
               _date_create as DateCreate, _date_modify as DateModify, 
               _date_begin as DateBegin, _date_complete as DateComplete,
               _key as [Key], _value_long as ValueLong, _value_string as ValueString, 
               _value_guid as ValueGuid, _value_bool as ValueBool, _value_double as ValueDouble, 
               _value_numeric as ValueNumeric, _value_datetime as ValueDatetime, 
               _value_bytes as ValueBytes, _note as Note, _hash as Hash
        FROM _objects 
        WHERE _id_parent = @p0
        ORDER BY _name, _id
        """;
    
    public string Tree_ObjectExists() =>
        "SELECT TOP 1 _id FROM _objects WHERE _id = @p0";
    
    public string Tree_SelectParentId() =>
        "SELECT _id_parent FROM _objects WHERE _id = @p0";
    
    public string Tree_UpdateParent() =>
        "UPDATE _objects SET _id_parent = @p0, _date_modify = @p1, _id_who_change = @p2 WHERE _id = @p3";
    
    public string Tree_DeleteValuesByObjectIds() =>
        "DELETE FROM _values WHERE _id_object IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))";
    
    public string Tree_DeleteObjectsByIds() =>
        "DELETE FROM _objects WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))";
    
    // ============================================================
    // === OBJECT STORAGE SQL ===
    // ============================================================
    
    public string ObjectStorage_SelectObjectById() =>
        """
        SELECT _id as Id, _id_scheme as IdScheme, _hash as Hash, _name as Name, 
               _id_parent as IdParent, _id_owner as IdOwner, _id_who_change as IdWhoChange,
               _date_create as DateCreate, _date_modify as DateModify, 
               _date_begin as DateBegin, _date_complete as DateComplete,
               _key as [Key], _value_long as ValueLong, _value_string as ValueString, 
               _value_guid as ValueGuid, _value_bool as ValueBool, _value_double as ValueDouble, 
               _value_numeric as ValueNumeric, _value_datetime as ValueDatetime, 
               _value_bytes as ValueBytes, _note as Note
        FROM _objects WHERE _id = @p0
        """;
    
    public string ObjectStorage_SelectIdHash() =>
        "SELECT _id as Id, _hash as Hash FROM _objects WHERE _id = @p0";
    
    public string ObjectStorage_SelectIdHashScheme() =>
        "SELECT _id as Id, _hash as Hash, _id_scheme as IdScheme FROM _objects WHERE _id = @p0";
    
    public string ObjectStorage_SelectObjectsByIds() =>
        """
        SELECT _id as Id, _id_scheme as IdScheme, _id_parent as IdParent, _id_owner as IdOwner,
               _id_who_change as IdWhoChange, _name as Name, _hash as Hash,
               _date_create as DateCreate, _date_modify as DateModify, _date_begin as DateBegin,
               _date_complete as DateComplete, _key as [Key], _note as Note,
               _value_long as ValueLong, _value_string as ValueString, _value_guid as ValueGuid,
               _value_bool as ValueBool, _value_double as ValueDouble, _value_numeric as ValueNumeric,
               _value_datetime as ValueDatetime, _value_bytes as ValueBytes
        FROM _objects WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))
        """;
    
    public string ObjectStorage_DeleteById() =>
        "EXEC dbo.delete_object_cascade @p0";
    
    public string ObjectStorage_DeleteByIds() =>
        "EXEC dbo.delete_objects_cascade @p0";
    
    public string ObjectStorage_InsertObject() => """
        INSERT INTO _objects (
            _id, _id_scheme, _name, _note, _date_create, _date_modify,
            _id_owner, _id_who_change, _id_parent, _hash,
            _value_string, _value_long, _value_guid, _value_bool,
            _value_double, _value_numeric, _value_datetime, _value_bytes,
            _key, _date_begin, _date_complete
        ) VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, CAST(@p17 AS VARBINARY(MAX)), @p18, @p19, @p20)
        """;
    
    public string ObjectStorage_UpdateObject() => """
        UPDATE _objects SET 
            _name = @p0, _note = @p1, _date_modify = @p2, _id_who_change = @p3, _hash = @p4,
            _value_string = @p5, _value_long = @p6, _value_guid = @p7, _value_bool = @p8,
            _value_double = @p9, _value_numeric = @p10, _value_datetime = @p11, _value_bytes = CAST(@p12 AS VARBINARY(MAX)),
            _key = @p13, _date_begin = @p14, _date_complete = @p15
        WHERE _id = @p16
        """;
    
    public string ObjectStorage_DeleteValuesByObjectId() =>
        "DELETE FROM _values WHERE _id_object = @p0";
    
    public string ObjectStorage_GetObjectJson() =>
        "SELECT dbo.get_object_json(@p0, @p1)";
    
    /// <summary>
    /// Bulk get object JSON. Uses function call directly in SELECT.
    /// </summary>
    public string ObjectStorage_GetObjectsJsonBulk() =>
        """
        SELECT dbo.get_object_json(CAST(value AS BIGINT), @p1) as JsonData 
        FROM STRING_SPLIT(@p0, ',')
        """;
    
    public string ObjectStorage_SelectStructuresWithMetadata() =>
        """
        SELECT s._id as Id, s._id_parent as IdParent, s._name as Name, 
               COALESCE(t._db_type, 'String') as DbType,
               s._collection_type as CollectionType, s._key_type as KeyType, 
               COALESCE(s._store_null, 0) as StoreNull,
               COALESCE(t._type, 'string') as TypeSemantic
        FROM _structures s
        LEFT JOIN _types t ON s._id_type = t._id
        WHERE s._id_scheme = @p0
        """;
    
    public string ObjectStorage_SelectValuesWithTypes() =>
        """
        SELECT v._id as Id, v._id_structure as IdStructure, v._id_object as IdObject,
               v._string as String, v._long as Long, v._guid as Guid, v.[_Double] as [Double],
               v._datetimeoffset as DateTimeOffset, v._boolean as Boolean, v._bytearray as ByteArray,
               v._numeric as Numeric, v._listitem as ListItem, v._object as Object,
               v._array_parent_id as ArrayParentId, v._array_index as ArrayIndex,
               COALESCE(t._db_type, 'String') as DbType
        FROM _values v
        JOIN _structures s ON v._id_structure = s._id
        JOIN _types t ON s._id_type = t._id
        WHERE v._id_object = @p0 AND v._id_structure IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p1, ','))
        """;
    
    public string ObjectStorage_SelectStructureTypes() =>
        """
        SELECT s._id as StructureId, COALESCE(t._db_type, 'String') as DbType
        FROM _structures s
        JOIN _types t ON s._id_type = t._id
        WHERE s._id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))
        """;
    
    public string ObjectStorage_SelectTypeById() =>
        "SELECT _id as Id, _name as Name, _db_type as DbType, _type as Type1 FROM _types WHERE _id = @p0";
    
    public string ObjectStorage_CheckObjectExists() =>
        "SELECT TOP 1 _id FROM _objects WHERE _id = @p0";
    
    public string ObjectStorage_SelectSchemeById() =>
        "SELECT _id as Id, _name as Name FROM _schemes WHERE _id = @p0";
    
    public string ObjectStorage_SelectValuesForObjects() =>
        """
        SELECT _id as Id, _id_structure as IdStructure, _id_object as IdObject, 
               _string as String, _long as Long, _guid as Guid, [_Double] as [Double],
               _datetimeoffset as DateTimeOffset, _boolean as Boolean, _bytearray as ByteArray,
               _numeric as Numeric, _listitem as ListItem, _object as Object,
               _array_parent_id as ArrayParentId, _array_index as ArrayIndex
        FROM _values 
        WHERE _id_object IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))
        """;
    
    public string ObjectStorage_SelectSchemeIdsForObjects() =>
        "SELECT _id as ObjectId, _id_scheme as SchemeId FROM _objects WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))";
    
    public string ObjectStorage_SelectValueById() =>
        """
        SELECT _id as Id, _id_structure as IdStructure, _id_object as IdObject,
               _string as String, _long as Long, _guid as Guid, [_Double] as [Double],
               _datetimeoffset as DateTimeOffset, _boolean as Boolean, _bytearray as ByteArray,
               _numeric as Numeric, _listitem as ListItem, _object as Object,
               _array_parent_id as ArrayParentId, _array_index as ArrayIndex
        FROM _values WHERE _id = @p0
        """;
    
    public string ObjectStorage_SelectAllTypes() =>
        "SELECT _id as Id, _name as Name, _db_type as DbType, _type as Type1 FROM _types";
    
    public string ObjectStorage_SelectExistingIds() =>
        "SELECT _id as Id FROM _objects WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))";
    
    public string ObjectStorage_SelectSchemesByIds() =>
        """
        SELECT _id as Id, _name as Name, _alias as Alias, _name_space as NameSpace, 
               _structure_hash as StructureHash, _type as Type 
        FROM _schemes WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))
        """;
    
    public string ObjectStorage_LockObjectsForUpdate() =>
        "SELECT 1 FROM _objects WITH (UPDLOCK) WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))";
    
    public string ObjectStorage_SelectSchemeIdByObjectId() =>
        "SELECT _id_scheme FROM _objects WHERE _id = @p0";
    
    // ============================================================
    // === LIST SQL ===
    // ============================================================
    
    public string Lists_SelectById() =>
        "SELECT _id, _name FROM _lists WHERE _id = @p0";
    
    public string Lists_SelectByName() =>
        "SELECT _id, _name FROM _lists WHERE _name = @p0";
    
    public string Lists_SelectAll() =>
        "SELECT _id, _name FROM _lists ORDER BY _name";
    
    public string Lists_Insert() =>
        "INSERT INTO _lists (_id, _name, _alias) VALUES (@p0, @p1, @p2)";
    
    public string Lists_Update() =>
        "UPDATE _lists SET _name = @p0, _alias = @p1 WHERE _id = @p2";
    
    public string Lists_Delete() =>
        "DELETE FROM _lists WHERE _id = @p0";
    
    public string Lists_IsUsedInStructures() =>
        "SELECT TOP 1 _id FROM _structures WHERE _id_list = @p0";
    
    // ============================================================
    // === LIST ITEMS SQL ===
    // ============================================================
    
    public string ListItems_SelectById() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id = @p0";
    
    public string ListItems_SelectByListId() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id_list = @p0 ORDER BY _value";
    
    public string ListItems_SelectByListIdAndValue() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id_list = @p0 AND _value = @p1";
    
    public string ListItems_Insert() =>
        "INSERT INTO _list_items (_id, _id_list, _value, _alias, _id_object) VALUES (@p0, @p1, @p2, @p3, @p4)";
    
    public string ListItems_UpdateAliasAndObject() =>
        "UPDATE _list_items SET _alias = @p0, _id_object = @p1 WHERE _id = @p2";
    
    public string ListItems_Update() =>
        "UPDATE _list_items SET _value = @p0, _alias = @p1, _id_object = @p2 WHERE _id = @p3";
    
    public string ListItems_Delete() =>
        "DELETE FROM _list_items WHERE _id = @p0";
    
    public string ListItems_SelectByObjectId() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id_object = @p0";
    
    // ============================================================
    // === VALIDATION SQL ===
    // ============================================================
    
    public string Validation_SelectAllTypes() =>
        "SELECT _id, _name, _db_type, _type FROM _types";
    
    public string Validation_SelectSchemeByName() =>
        "SELECT _id, _id_parent, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _name = @p0";
    
    public string Validation_SelectStructuresBySchemeId() =>
        """
        SELECT s._id, s._id_parent, s._id_scheme, s._id_override, s._id_type, s._id_list, 
               s._name, s._alias, s._order, s._readonly, s._allow_not_null, 
               s._collection_type, s._key_type, s._is_compress, s._store_null, 
               s._default_value, s._default_editor
        FROM _structures s WHERE s._id_scheme = @p0
        """;
    
    // ============================================================
    // === LAZY LOADER SQL ===
    // ============================================================
    
    public string LazyLoader_SelectObjectBase() =>
        """
        SELECT _id, _id_scheme, _hash, _name, _id_parent, _id_owner, _id_who_change,
               _date_create, _date_modify, _date_begin, _date_complete,
               _key, _value_long, _value_string, _value_guid, _value_bool,
               _value_double, _value_numeric, _value_datetime, _value_bytes, _note
        FROM _objects WHERE _id = @p0
        """;
    
    public string LazyLoader_GetObjectJson() =>
        "SELECT dbo.get_object_json(@p0, @p1)";
    
    /// <summary>
    /// Batch load Props JSON for multiple objects. Uses function call directly in SELECT.
    /// Deep nested objects will be loaded on demand via individual get_object_json calls.
    /// </summary>
    public string LazyLoader_GetObjectJsonBatch() =>
        """
        SELECT CAST(value AS BIGINT) as Id, dbo.get_object_json(CAST(value AS BIGINT), 1) as JsonData 
        FROM STRING_SPLIT(@p0, ',')
        """;
    
    public string LazyLoader_SelectObjectHash() =>
        "SELECT _hash FROM _objects WHERE _id = @p0";
    
    // ============================================================
    // === QUERY PROVIDER SQL ===
    // ============================================================
    
    // MSSQL: Full version (8 params) delegates to _base internally
    public string Query_SearchObjectsFunction() => "dbo.search_objects_with_facets";
    
    // MSSQL: Base version (7 params) for lazy loading
    public string Query_SearchObjectsBaseFunction() => "dbo.search_objects_with_facets_base";
    
    public string Query_SearchObjectsProjectionByPathsFunction() => "dbo.search_objects_with_projection_by_paths";
    
    public string Query_SearchObjectsProjectionByIdsFunction() => "dbo.search_objects_with_projection_by_ids";
    
    // MSSQL: Full version with Props (uses get_object_json internally)
    public string Query_SearchTreeObjectsFunction() => "dbo.search_tree_objects_with_facets";
    
    // MSSQL: Base version without Props (for lazy loading)
    public string Query_SearchTreeObjectsBaseFunction() => "dbo.search_tree_objects_with_facets_base";
    
    /// <summary>
    /// MSSQL uses EXEC and OPENJSON for JSON parameters.
    /// </summary>
    public string Query_CountTemplate() =>
        "SELECT JSON_VALUE({0}(@p0, @p1, NULL, NULL, NULL, @p2), '$.total_count')";
    
    /// <summary>
    /// MSSQL uses EXEC for stored procedures. Procedure returns 'result' column.
    /// </summary>
    public string Query_SearchTemplate() =>
        "EXEC {0} @p0, @p1, @p2, @p3, @p4, @p5";
    
    public string Query_SearchWithDistinctTemplate() =>
        "EXEC {0} @p0, @p1, @p2, @p3, @p4, @p5, @p6";
    
    /// <summary>
    /// MSSQL always uses _base version, but C# passes 8 params when useLazyLoading=false.
    /// The _base procedure has dummy @include_facets param that is ignored.
    /// </summary>
    public string Query_SearchFullTemplate() =>
        "EXEC {0} @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7";
    
    /// <summary>
    /// MSSQL doesn't need explicit JSON cast - it's handled by function signature.
    /// </summary>
    public string Query_JsonCast() => "";
    
    /// <summary>
    /// MSSQL uses STRING_SPLIT instead of array cast.
    /// </summary>
    public string Query_TextArrayCast() => "";
    
    public string Query_BigintArrayCast() => "";
    
    public string Query_ProjectionByPathsTemplate() =>
        "EXEC dbo.search_objects_with_projection_by_paths @p0, @p1, @p2, @p3, @p4, @p5, @p6";
    
    public string Query_ProjectionByIdsTemplate(string structureIdsArray) =>
        $"EXEC dbo.search_objects_with_projection_by_ids @p0, @p1, '{structureIdsArray}', @p2, @p3, @p4, @p5";
    
    public string Query_CheckPermissionSql() =>
        "SELECT CASE WHEN EXISTS(SELECT 1 FROM dbo.get_user_permissions_for_object(@p0, @p1) WHERE can_select = 1) THEN 1 ELSE 0 END as has_permission";
    
    public string Query_AggregateBatchPreviewSql() =>
        "EXEC dbo.aggregate_batch_preview @p0, @p1, @p2";
    
    public string Query_AggregateFieldSql() =>
        "EXEC dbo.aggregate_field @p0, @p1, @p2, @p3";
    
    public string Query_SqlPreviewTemplate() =>
        "EXEC {0} @p0, @p1, @p2, @p3, @p4, @p5, @p6";
    
    public string Query_AggregateBatchSql() =>
        "EXEC dbo.aggregate_batch @p0, @p1, @p2";
    
    /// <summary>
    /// Simple search for Delete operations - uses search_objects_with_facets with minimal params.
    /// Returns JSON with {objects: [{id:...},...], total_count:...}
    /// </summary>
    public string Query_SearchObjectsSimpleSql() =>
        "EXEC dbo.search_objects_with_facets @p0, @p1, NULL, 0, NULL, 10, 0, 0";
    
    public string Query_AggregateGroupedSql() =>
        "EXEC dbo.aggregate_grouped @p0, @p1, @p2, @p3";
    
    public string Query_AggregateArrayGroupedSql() =>
        "EXEC dbo.aggregate_array_grouped @p0, @p1, @p2, @p3, @p4";
    
    public string Query_WindowSql() =>
        "EXEC dbo.query_with_window @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7";
    
    /// <summary>
    /// For Tree procedures, we use temp table to capture EXEC result and extract total_count.
    /// </summary>
    public string Query_TreeCountNormalSql(string functionName) =>
        $@"CREATE TABLE #tmp_cnt (result NVARCHAR(MAX));
        INSERT INTO #tmp_cnt EXEC {functionName} @p0, @p1, 1, 0, NULL, @p2;
        SELECT CAST(JSON_VALUE(result, '$.total_count') AS INT) AS [Value] FROM #tmp_cnt;
        DROP TABLE #tmp_cnt";
    
    public string Query_TreeSearchNormalSql(string functionName) =>
        $"EXEC {functionName} @p0, @p1, @p2, @p3, @p4, @p5";
    
    /// <summary>
    /// Tree search with parent_ids array (8 params).
    /// </summary>
    public string Query_TreeSearchWithParentIdsSql(string functionName) =>
        $"EXEC {functionName} @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7";
    
    /// <summary>
    /// HasAncestor with tree function (8 params) - MSSQL captures EXEC result in temp table.
    /// </summary>
    public string Query_HasAncestorTreeSql(string functionName) =>
        $@"CREATE TABLE #tmp_result (result NVARCHAR(MAX));
        INSERT INTO #tmp_result EXEC {functionName} @p0, @p1, @p2, NULL, 0, NULL, @p3, 10;
        SELECT CAST(j.id AS NVARCHAR(20)) AS [Value] FROM #tmp_result CROSS APPLY OPENJSON(JSON_QUERY(result, '$.objects')) WITH (id BIGINT '$.id') j;
        DROP TABLE #tmp_result";
    
    /// <summary>
    /// HasAncestor with normal function (6 params) - MSSQL captures EXEC result in temp table.
    /// </summary>
    public string Query_HasAncestorNormalSql(string functionName) =>
        $@"CREATE TABLE #tmp_result (result NVARCHAR(MAX));
        INSERT INTO #tmp_result EXEC {functionName} @p0, @p1, NULL, 0, NULL, 10;
        SELECT CAST(j.id AS NVARCHAR(20)) AS [Value] FROM #tmp_result CROSS APPLY OPENJSON(JSON_QUERY(result, '$.objects')) WITH (id BIGINT '$.id') j;
        DROP TABLE #tmp_result";
    
    /// <summary>
    /// HasDescendant with normal function (6 params) - MSSQL captures EXEC result in temp table.
    /// </summary>
    public string Query_HasDescendantSql(string functionName) =>
        $@"CREATE TABLE #tmp_result (result NVARCHAR(MAX));
        INSERT INTO #tmp_result EXEC {functionName} @p0, @p1, NULL, 0, NULL, 10;
        SELECT CAST(j.id AS NVARCHAR(20)) AS [Value] FROM #tmp_result CROSS APPLY OPENJSON(JSON_QUERY(result, '$.objects')) WITH (id BIGINT '$.id') j;
        DROP TABLE #tmp_result";
    
    /// <summary>
    /// MSSQL: WITH (without RECURSIVE) for traversing ancestors.
    /// </summary>
    public string Query_GetParentIdsFromDescendantsSql(string idsString, int depthLimit) =>
        $@"WITH ancestors AS (
            SELECT _id, _id_parent, 0 as level
            FROM _objects
            WHERE _id IN ({idsString})
            UNION ALL
            SELECT o._id, o._id_parent, a.level + 1
            FROM _objects o
            INNER JOIN ancestors a ON a._id_parent = o._id
            WHERE a.level < {depthLimit}
        )
        SELECT DISTINCT _id FROM ancestors WHERE _id_parent IS NULL OR level > 0";
    
    /// <summary>
    /// MSSQL: capture EXEC result and extract total_count.
    /// </summary>
    public string Query_TreeCountWithParentIdsSql(string functionName) =>
        $@"CREATE TABLE #tmp_cnt (result NVARCHAR(MAX));
        INSERT INTO #tmp_cnt EXEC {functionName} @p0, @p1, @p2, 1, 0, NULL, @p3, @p4;
        SELECT CAST(JSON_VALUE(result, '$.total_count') AS INT) AS [Value] FROM #tmp_cnt;
        DROP TABLE #tmp_cnt";
    
    /// <summary>
    /// MSSQL: Get all IDs with their ancestors using recursive CTE.
    /// MSSQL uses WITH (no RECURSIVE keyword).
    /// </summary>
    public string Query_GetIdsWithAncestorsSql(string idsString) =>
        $@";WITH parent_chain AS (
            SELECT _id, _id_parent 
            FROM _objects 
            WHERE _id IN ({idsString})
            UNION ALL
            SELECT o._id, o._id_parent 
            FROM _objects o
            INNER JOIN parent_chain pc ON pc._id_parent = o._id
        )
        SELECT DISTINCT _id FROM parent_chain";
    
    /// <summary>
    /// MSSQL: Load objects by IDs as JSON using get_object_json function.
    /// </summary>
    public string Query_LoadObjectsByIdsSql(string idsString, int maxDepth) =>
        $@"SELECT dbo.get_object_json(_id, {maxDepth}) AS [Value]
           FROM _objects 
           WHERE _id IN ({idsString})
           ORDER BY _id";
    
    public string Query_TreeSqlPreviewTemplate(string functionName) =>
        $"EXEC {functionName} @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7";
    
    // ============================================================
    // === SQL PREVIEW FUNCTION NAMES ===
    // ============================================================
    
    // MSSQL: Eager loading not implemented, always use _base (lazy) versions
    public string Query_SqlPreviewFunction() => "dbo.get_search_sql_preview_base";
    
    public string Query_SqlPreviewBaseFunction() => "dbo.get_search_sql_preview_base";
    
    public string Query_TreeSqlPreviewFunction() => "dbo.get_search_tree_sql_preview_base";
    
    public string Query_TreeSqlPreviewBaseFunction() => "dbo.get_search_tree_sql_preview_base";
    
    // ============================================================
    // === SOFT DELETE ===
    // ============================================================
    
    /// <summary>
    /// Calls sp_mark_for_deletion procedure to soft-delete objects.
    /// Params: @p0=objectIds (comma-separated string), @p1=userId, @p2=trashParentId (nullable)
    /// Returns via OUTPUT params: @trash_id, @marked_count
    /// </summary>
    public string SoftDelete_MarkForDeletion() => 
        "DECLARE @trash_id BIGINT, @marked_count BIGINT; EXEC sp_mark_for_deletion @p0, @p1, @p2, @trash_id OUTPUT, @marked_count OUTPUT; SELECT @trash_id AS trash_id, @marked_count AS marked_count;";
    
    /// <summary>
    /// Calls sp_purge_trash procedure to physically delete objects from trash.
    /// Params: @p0=trashId, @p1=batchSize
    /// Returns via OUTPUT params: @deleted_count, @remaining_count
    /// </summary>
    public string SoftDelete_PurgeTrash() => 
        "DECLARE @deleted_count BIGINT, @remaining_count BIGINT; EXEC sp_purge_trash @p0, @p1, @deleted_count OUTPUT, @remaining_count OUTPUT; SELECT @deleted_count AS deleted_count, @remaining_count AS remaining_count;";
    
    /// <summary>
    /// Gets deletion progress for a specific trash container.
    /// Params: @p0=trashId
    /// Returns: trash_id, total, deleted, status, started_at, owner_id
    /// </summary>
    public string SoftDelete_GetDeletionProgress() => """
        SELECT 
            [_id] AS trash_id,
            COALESCE([_value_long], 0) AS total,
            COALESCE([_key], 0) AS deleted,
            COALESCE([_value_string], 'pending') AS status,
            [_date_create] AS started_at,
            [_id_owner] AS owner_id
        FROM [dbo].[_objects] 
        WHERE [_id] = @p0 AND [_id_scheme] = -10
        """;
    
    /// <summary>
    /// Gets all active deletions for a user.
    /// Params: @p0=userId
    /// Returns: trash_id, total, deleted, status, started_at, owner_id
    /// </summary>
    public string SoftDelete_GetUserActiveDeletions() => """
        SELECT 
            [_id] AS trash_id,
            COALESCE([_value_long], 0) AS total,
            COALESCE([_key], 0) AS deleted,
            COALESCE([_value_string], 'pending') AS status,
            [_date_create] AS started_at,
            [_id_owner] AS owner_id
        FROM [dbo].[_objects] 
        WHERE [_id_owner] = @p0 
          AND [_id_scheme] = -10 
          AND [_id_parent] IS NULL
          AND [_value_string] IN ('pending', 'running')
        ORDER BY [_date_create] DESC
        """;
    
    /// <summary>
    /// Gets orphaned deletion tasks for recovery at startup.
    /// CLUSTER-SAFE: Only returns pending OR running with stale _date_modify.
    /// Params: @p0=timeoutMinutes
    /// Returns: trash_id, total, deleted, status, owner_id
    /// </summary>
    public string SoftDelete_GetOrphanedTasks() => """
        SELECT 
            [_id] AS trash_id,
            COALESCE([_value_long], 0) AS total,
            COALESCE([_key], 0) AS deleted,
            COALESCE([_value_string], 'pending') AS status,
            [_id_owner] AS owner_id
        FROM [dbo].[_objects] 
        WHERE [_id_scheme] = -10 
          AND [_id_parent] IS NULL
          AND (
            [_value_string] = 'pending'
            OR (
              [_value_string] = 'running' 
              AND [_date_modify] < DATEADD(MINUTE, -@p0, SYSDATETIMEOFFSET())
            )
          )
        ORDER BY [_date_create]
        """;
    
    /// <summary>
    /// Atomically claim an orphaned task for processing.
    /// CLUSTER-SAFE: UPDATE with condition prevents race.
    /// Params: @p0=trashId, @p1=timeoutMinutes
    /// Returns: affected rows (1=success, 0=already taken)
    /// </summary>
    public string SoftDelete_ClaimOrphanedTask() => """
        UPDATE [dbo].[_objects] WITH (ROWLOCK)
        SET [_value_string] = 'running',
            [_date_modify] = SYSDATETIMEOFFSET()
        WHERE [_id] = @p0 
          AND [_id_scheme] = -10
          AND (
            [_value_string] = 'pending'
            OR (
              [_value_string] = 'running' 
              AND [_date_modify] < DATEADD(MINUTE, -@p1, SYSDATETIMEOFFSET())
            )
          )
        """;
    
    // ============================================================
    // === METADATA CACHE WARMUP ===
    // ============================================================
    
    /// <summary>
    /// MSSQL uses stored procedure, returns same columns as PostgreSQL function.
    /// </summary>
    public string Warmup_AllMetadataCaches() =>
        "EXEC warmup_all_metadata_caches";
}

