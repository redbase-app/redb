using System.Collections.Generic;

namespace redb.Core.Query;

/// <summary>
/// SQL syntax abstraction for different DBMS support.
/// 
/// IMPORTANT: Implementations in different projects MAY DIFFER!
/// - PostgreSqlDialect: CASE WHEN (tested - 15 ms on 15K objects)
/// - MsSqlDialect: will be later, possibly different implementation
/// 
/// Current focus: PostgreSQL + performance.
/// </summary>
public interface ISqlDialect
{
    /// <summary>
    /// Provider name (PostgreSQL, SqlServer, MySql)
    /// </summary>
    string ProviderName { get; }
    
    /// <summary>
    /// Pagination format: LIMIT/OFFSET for PostgreSQL, TOP/OFFSET for MSSQL
    /// </summary>
    string FormatPagination(int? limit, int? offset);
    
    /// <summary>
    /// Subquery wrapper: (subquery) AS alias
    /// </summary>
    string WrapSubquery(string subquery, string alias);
    
    /// <summary>
    /// PVT column for CASE WHEN approach.
    /// PostgreSQL: MAX(CASE WHEN _id_structure = X THEN _Long END) AS "Age"
    /// </summary>
    string FormatPvtColumn(long structureId, string dbColumn, string alias);
    
    /// <summary>
    /// Array containment check: = ANY($1) for PostgreSQL, IN for MSSQL
    /// </summary>
    string FormatArrayContains(string column, string paramName);
    
    /// <summary>
    /// Mapping of redb types to SQL types
    /// </summary>
    string GetDbTypeName(string redbType);
    
    /// <summary>
    /// Parameter format: $1 for PostgreSQL, @p1 for MSSQL
    /// </summary>
    string FormatParameter(int index);
    
    /// <summary>
    /// Identifier escaping: "name" for PostgreSQL, [name] for MSSQL
    /// </summary>
    string QuoteIdentifier(string name);
    
    // ============================================================
    // === ROLES SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT role by ID. Returns: Id, Name, IdConfiguration
    /// </summary>
    string Roles_SelectById();
    
    /// <summary>
    /// SELECT role by name. Returns: Id, Name, IdConfiguration
    /// </summary>
    string Roles_SelectByName();
    
    /// <summary>
    /// SELECT all roles ordered by name. Returns: Id, Name, IdConfiguration
    /// </summary>
    string Roles_SelectAll();
    
    /// <summary>
    /// INSERT new role. Params: $1=id, $2=name
    /// </summary>
    string Roles_Insert();
    
    /// <summary>
    /// UPDATE role name. Params: $1=newName, $2=roleId
    /// </summary>
    string Roles_UpdateName();
    
    /// <summary>
    /// DELETE role by ID. Params: $1=roleId
    /// </summary>
    string Roles_Delete();
    
    /// <summary>
    /// Check if role name exists (excluding role). Params: $1=name, $2=excludeId (optional)
    /// </summary>
    string Roles_ExistsByName();
    
    /// <summary>
    /// Check if role name exists excluding specific role. Params: $1=name, $2=excludeId
    /// </summary>
    string Roles_ExistsByNameExcluding();
    
    /// <summary>
    /// SELECT COUNT of all roles
    /// </summary>
    string Roles_Count();
    
    /// <summary>
    /// UPDATE role configuration. Params: $1=configId, $2=roleId
    /// </summary>
    string Roles_UpdateConfiguration();
    
    /// <summary>
    /// SELECT role configuration ID. Params: $1=roleId
    /// </summary>
    string Roles_SelectConfigurationId();
    
    // ============================================================
    // === USERS_ROLES SQL ===
    // ============================================================
    
    /// <summary>
    /// INSERT user-role assignment. Params: $1=id, $2=userId, $3=roleId
    /// </summary>
    string UsersRoles_Insert();
    
    /// <summary>
    /// DELETE user-role assignment. Params: $1=userId, $2=roleId
    /// </summary>
    string UsersRoles_Delete();
    
    /// <summary>
    /// DELETE all user-role assignments for user. Params: $1=userId
    /// </summary>
    string UsersRoles_DeleteByUser();
    
    /// <summary>
    /// DELETE all user-role assignments for role. Params: $1=roleId
    /// </summary>
    string UsersRoles_DeleteByRole();
    
    /// <summary>
    /// Check if user-role assignment exists. Params: $1=userId, $2=roleId
    /// </summary>
    string UsersRoles_Exists();
    
    /// <summary>
    /// SELECT roles for user. Params: $1=userId. Returns: Id, Name, IdConfiguration
    /// </summary>
    string UsersRoles_SelectRolesByUser();
    
    /// <summary>
    /// SELECT users for role. Params: $1=roleId. Returns user fields
    /// </summary>
    string UsersRoles_SelectUsersByRole();
    
    /// <summary>
    /// SELECT COUNT of users for role. Params: $1=roleId
    /// </summary>
    string UsersRoles_CountByRole();
    
    // ============================================================
    // === USERS SQL (for role operations) ===
    // ============================================================
    
    /// <summary>
    /// Check if user exists by ID. Params: $1=userId
    /// </summary>
    string Users_ExistsById();
    
    /// <summary>
    /// SELECT user by login. Params: $1=login. Returns: Id
    /// </summary>
    string Users_SelectIdByLogin();
    
    // ============================================================
    // === PERMISSIONS SQL ===
    // ============================================================
    
    /// <summary>
    /// DELETE permissions by role. Params: $1=roleId
    /// </summary>
    string Permissions_DeleteByRole();
    
    /// <summary>
    /// Get user permissions for object via SQL function or query.
    /// CRITICAL: PostgreSQL uses get_user_permissions_for_object() function!
    /// MSSQL: Must be expanded to CTE/JOIN query.
    /// Params: $1=objectId, $2=userId
    /// </summary>
    string Permissions_GetEffectiveForObject();
    
    /// <summary>
    /// SELECT readable object IDs for user. Params: $1=userId
    /// </summary>
    string Permissions_SelectReadableObjectIds();
    
    /// <summary>
    /// INSERT new permission. Params: $1=id, $2=userId, $3=roleId, $4=refId, $5=select, $6=insert, $7=update, $8=delete
    /// </summary>
    string Permissions_Insert();
    
    /// <summary>
    /// UPDATE permission flags. Params: $1=select, $2=insert, $3=update, $4=delete, $5=permissionId
    /// </summary>
    string Permissions_Update();
    
    /// <summary>
    /// DELETE permission by ID. Params: $1=permissionId
    /// </summary>
    string Permissions_Delete();
    
    /// <summary>
    /// SELECT permission by ID. Params: $1=permissionId
    /// </summary>
    string Permissions_SelectById();
    
    /// <summary>
    /// SELECT permissions by user. Params: $1=userId
    /// </summary>
    string Permissions_SelectByUser();
    
    /// <summary>
    /// SELECT permissions by role. Params: $1=roleId
    /// </summary>
    string Permissions_SelectByRole();
    
    /// <summary>
    /// SELECT permissions by object. Params: $1=objectId
    /// </summary>
    string Permissions_SelectByObject();
    
    /// <summary>
    /// SELECT permission by user/role/object. Params: $1=userId, $2=roleId, $3=objectId
    /// </summary>
    string Permissions_SelectByUserRoleObject();
    
    /// <summary>
    /// DELETE permission by user/role/object. Params: $1=userId, $2=roleId, $3=objectId
    /// </summary>
    string Permissions_DeleteByUserRoleObject();
    
    /// <summary>
    /// DELETE all permissions by user. Params: $1=userId
    /// </summary>
    string Permissions_DeleteByUser();
    
    /// <summary>
    /// SELECT COUNT of all permissions
    /// </summary>
    string Permissions_Count();
    
    /// <summary>
    /// SELECT COUNT of permissions by user. Params: $1=userId
    /// </summary>
    string Permissions_CountByUser();
    
    /// <summary>
    /// SELECT COUNT of permissions by role. Params: $1=roleId
    /// </summary>
    string Permissions_CountByRole();
    
    /// <summary>
    /// SELECT user's role IDs. Params: $1=userId
    /// </summary>
    string Permissions_SelectUserRoleIds();
    
    // ============================================================
    // === USERS SQL (full) ===
    // ============================================================
    
    /// <summary>
    /// SELECT all user fields by ID. Params: $1=userId
    /// </summary>
    string Users_SelectById();
    
    /// <summary>
    /// SELECT all user fields by login. Params: $1=login
    /// </summary>
    string Users_SelectByLogin();
    
    /// <summary>
    /// INSERT new user with all fields.
    /// Params: $1=id, $2=login, $3=password, $4=name, $5=phone, $6=email, $7=enabled,
    ///         $8=dateRegister, $9=dateDismiss, $10=key, $11=codeInt, $12=codeString,
    ///         $13=codeGuid, $14=note, $15=hash
    /// </summary>
    string Users_Insert();
    
    /// <summary>
    /// UPDATE user fields.
    /// Params: $1=login, $2=name, $3=phone, $4=email, $5=enabled, $6=dateDismiss,
    ///         $7=key, $8=codeInt, $9=codeString, $10=codeGuid, $11=note, $12=hash, $13=userId
    /// </summary>
    string Users_Update();
    
    /// <summary>
    /// UPDATE user for soft delete. Params: $1=newLogin, $2=newName, $3=enabled, $4=dateDismiss, $5=userId
    /// </summary>
    string Users_SoftDelete();
    
    /// <summary>
    /// UPDATE user password. Params: $1=hashedPassword, $2=userId
    /// </summary>
    string Users_UpdatePassword();
    
    /// <summary>
    /// UPDATE user enabled status. Params: $1=enabled, $2=dateDismiss, $3=userId
    /// </summary>
    string Users_UpdateStatus();
    
    /// <summary>
    /// Check login exists. Params: $1=login
    /// </summary>
    string Users_ExistsByLogin();
    
    /// <summary>
    /// Check login exists excluding user. Params: $1=login, $2=excludeUserId
    /// </summary>
    string Users_ExistsByLoginExcluding();
    
    /// <summary>
    /// Check email exists. Params: $1=email
    /// </summary>
    string Users_ExistsByEmail();
    
    /// <summary>
    /// SELECT COUNT of all users
    /// </summary>
    string Users_Count();
    
    /// <summary>
    /// SELECT COUNT of enabled users
    /// </summary>
    string Users_CountEnabled();
    
    /// <summary>
    /// SELECT user configuration ID. Params: $1=userId
    /// </summary>
    string Users_SelectConfigurationId();
    
    /// <summary>
    /// UPDATE user configuration. Params: $1=configId, $2=userId
    /// </summary>
    string Users_UpdateConfiguration();
    
    /// <summary>
    /// SELECT role by name. Params: $1=roleName
    /// </summary>
    string Roles_SelectIdByName();
    
    /// <summary>
    /// Check role exists by ID. Params: $1=roleId
    /// </summary>
    string Roles_ExistsById();
    
    // ============================================================
    // === SCHEMES SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT scheme by name. Params: $1=name
    /// Returns: _id, _name, _alias, _name_space, _structure_hash, _type
    /// </summary>
    string Schemes_SelectByName();
    
    /// <summary>
    /// SELECT scheme by ID. Params: $1=schemeId
    /// Returns: _id, _name, _alias, _name_space, _structure_hash, _type
    /// </summary>
    string Schemes_SelectById();
    
    /// <summary>
    /// SELECT all schemes.
    /// Returns: _id, _name, _alias, _name_space, _structure_hash, _type
    /// </summary>
    string Schemes_SelectAll();
    
    /// <summary>
    /// INSERT new scheme. Params: $1=id, $2=name, $3=alias, $4=type
    /// </summary>
    string Schemes_Insert();
    
    /// <summary>
    /// UPDATE scheme structure hash. Params: $1=hash, $2=schemeId
    /// </summary>
    string Schemes_UpdateHash();
    
    /// <summary>
    /// Update scheme name by ID.
    /// Used for migration from short name to full name (with namespace).
    /// Params: $1=newName, $2=schemeId
    /// </summary>
    string Schemes_UpdateName();
    
    /// <summary>
    /// SELECT scheme hash by ID. Params: $1=schemeId
    /// </summary>
    string Schemes_SelectHashById();
    
    /// <summary>
    /// Check scheme exists by name. Params: $1=name
    /// </summary>
    string Schemes_ExistsByName();
    
    /// <summary>
    /// SELECT Object scheme by name and type. Params: $1=name, $2=type
    /// </summary>
    string Schemes_SelectObjectByName();
    
    /// <summary>
    /// INSERT Object scheme. Params: $1=id, $2=name, $3=type
    /// </summary>
    string Schemes_InsertObject();
    
    // ============================================================
    // === STRUCTURES SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT full structures by scheme. Params: $1=schemeId
    /// Returns all structure fields
    /// </summary>
    string Structures_SelectByScheme();
    
    /// <summary>
    /// SELECT structures short fields by scheme. Params: $1=schemeId
    /// Returns: _id, _id_parent, _id_scheme, _id_type, _name, _order
    /// </summary>
    string Structures_SelectBySchemeShort();
    
    /// <summary>
    /// SELECT structures with cache fields by scheme. Params: $1=schemeId
    /// Returns fields needed for caching
    /// </summary>
    string Structures_SelectBySchemeCacheable();
    
    /// <summary>
    /// INSERT new structure. 
    /// Params: $1=id, $2=schemeId, $3=parentId, $4=name, $5=alias, $6=typeId, $7=allowNotNull, $8=collectionType, $9=keyType, $10=order
    /// </summary>
    string Structures_Insert();
    
    /// <summary>
    /// UPDATE structure type. Params: $1=typeId, $2=structureId
    /// </summary>
    string Structures_UpdateType();
    
    /// <summary>
    /// UPDATE structure collection type. Params: $1=collectionType, $2=structureId
    /// </summary>
    string Structures_UpdateCollectionType();
    
    /// <summary>
    /// UPDATE structure key type. Params: $1=keyType, $2=structureId
    /// </summary>
    string Structures_UpdateKeyType();
    
    /// <summary>
    /// UPDATE structure alias. Params: $1=alias, $2=structureId
    /// </summary>
    string Structures_UpdateAlias();
    
    /// <summary>
    /// UPDATE structure allow_not_null. Params: $1=allowNotNull, $2=structureId
    /// </summary>
    string Structures_UpdateAllowNotNull();
    
    /// <summary>
    /// DELETE structures by IDs. Params: dynamic IN clause
    /// </summary>
    string Structures_DeleteByIds(IEnumerable<long> ids);
    
    // ============================================================
    // === TYPES SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT type by name. Params: $1=name
    /// Returns: _id, _name
    /// </summary>
    string Types_SelectByName();
    
    /// <summary>
    /// SELECT all types. Returns: _id, _name, _db_type, _type
    /// </summary>
    string Types_SelectAll();
    
    // ============================================================
    // === SCHEME FUNCTIONS (PostgreSQL-specific, MSSQL: stored procs or queries) ===
    // ============================================================
    
    /// <summary>
    /// Sync metadata cache for scheme. Params: $1=schemeId
    /// PostgreSQL: SELECT sync_metadata_cache_for_scheme($1)
    /// MSSQL: EXEC sp_sync_metadata_cache @schemeId
    /// </summary>
    string Schemes_SyncMetadataCache();
    
    /// <summary>
    /// Migrate structure type. Params: $1=structureId, $2=oldType, $3=newType, $4=dryRun
    /// PostgreSQL: SELECT * FROM migrate_structure_type($1, $2, $3, $4)
    /// </summary>
    string Schemes_MigrateStructureType();
    
    /// <summary>
    /// Get structure tree JSON. Params: $1=schemeId
    /// PostgreSQL: SELECT get_scheme_structure_tree($1)
    /// </summary>
    string Schemes_GetStructureTree();
    
    // ============================================================
    // === TREE SQL ===
    // ============================================================
    
    /// <summary>
    /// Get object as JSON. Params: $1=objectId, $2=depth
    /// PostgreSQL: SELECT get_object_json($1, $2)
    /// MSSQL: Must implement as stored procedure or CTE query
    /// </summary>
    string Tree_GetObjectJson();
    
    /// <summary>
    /// SELECT children with JSON by parent and scheme. Params: $1=parentId, $2=schemeId
    /// Returns: json_data column
    /// </summary>
    string Tree_SelectChildrenJson();
    
    /// <summary>
    /// SELECT polymorphic children with scheme. Params: $1=parentId
    /// Returns: ObjectId, SchemeId, JsonData columns
    /// </summary>
    string Tree_SelectPolymorphicChildren();
    
    /// <summary>
    /// SELECT scheme and JSON for object. Params: $1=objectId
    /// Returns: SchemeId, JsonData columns
    /// </summary>
    string Tree_SelectSchemeAndJson();
    
    /// <summary>
    /// SELECT children base fields by parent and scheme (Pro PVT mode).
    /// Params: $1=parentId, $2=schemeId
    /// Returns: RedbObjectRow columns
    /// </summary>
    string Tree_SelectChildrenBySchemeBase();
    
    /// <summary>
    /// SELECT all children base fields by parent (Pro PVT polymorphic mode).
    /// Params: $1=parentId
    /// Returns: RedbObjectRow columns
    /// </summary>
    string Tree_SelectChildrenBase();
    
    /// <summary>
    /// Check if object exists. Params: $1=objectId
    /// </summary>
    string Tree_ObjectExists();
    
    /// <summary>
    /// SELECT parent_id by object ID. Params: $1=objectId
    /// </summary>
    string Tree_SelectParentId();
    
    /// <summary>
    /// UPDATE object parent (move). Params: $1=newParentId, $2=dateModify, $3=whoChangeId, $4=objectId
    /// </summary>
    string Tree_UpdateParent();
    
    /// <summary>
    /// DELETE values by object IDs. Params: $1=objectIds (array)
    /// </summary>
    string Tree_DeleteValuesByObjectIds();
    
    /// <summary>
    /// DELETE objects by IDs. Params: $1=objectIds (array)
    /// </summary>
    string Tree_DeleteObjectsByIds();
    
    // ============================================================
    // === OBJECT STORAGE SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT all base fields for object by ID. Params: $1=objectId
    /// Returns: Id, IdScheme, Hash, Name, IdParent, IdOwner, IdWhoChange, DateCreate, DateModify, DateBegin, DateComplete, Key, ValueLong, ValueString, ValueGuid, ValueBool, ValueDouble, ValueNumeric, ValueDatetime, ValueBytes, Note
    /// </summary>
    string ObjectStorage_SelectObjectById();
    
    /// <summary>
    /// SELECT only Id and Hash for object. Params: $1=objectId
    /// </summary>
    string ObjectStorage_SelectIdHash();
    
    /// <summary>
    /// SELECT Id, Hash, IdScheme for cache check. Params: $1=objectId
    /// </summary>
    string ObjectStorage_SelectIdHashScheme();
    
    /// <summary>
    /// SELECT all base fields for objects by IDs. Params: $1=objectIds (array)
    /// </summary>
    string ObjectStorage_SelectObjectsByIds();
    
    /// <summary>
    /// DELETE object by ID. Params: $1=objectId
    /// </summary>
    string ObjectStorage_DeleteById();
    
    /// <summary>
    /// DELETE objects by IDs (bulk). Params: $1=objectIds (array)
    /// </summary>
    string ObjectStorage_DeleteByIds();
    
    /// <summary>
    /// INSERT new object with all fields.
    /// Params: $1=id, $2=schemeId, $3=name, $4=note, $5=dateCreate, $6=dateModify,
    ///         $7=ownerId, $8=whoChangeId, $9=parentId, $10=hash,
    ///         $11=valueString, $12=valueLong, $13=valueGuid, $14=valueBool,
    ///         $15=valueDouble, $16=valueNumeric, $17=valueDatetime, $18=valueBytes,
    ///         $19=key, $20=dateBegin, $21=dateComplete
    /// </summary>
    string ObjectStorage_InsertObject();
    
    /// <summary>
    /// UPDATE object with all fields.
    /// Params: $1=name, $2=note, $3=dateModify, $4=whoChangeId, $5=hash,
    ///         $6=valueString, $7=valueLong, $8=valueGuid, $9=valueBool,
    ///         $10=valueDouble, $11=valueNumeric, $12=valueDatetime, $13=valueBytes,
    ///         $14=key, $15=dateBegin, $16=dateComplete, $17=objectId
    /// </summary>
    string ObjectStorage_UpdateObject();
    
    /// <summary>
    /// DELETE all values for object. Params: $1=objectId
    /// </summary>
    string ObjectStorage_DeleteValuesByObjectId();
    
    /// <summary>
    /// SELECT object as JSON. Params: $1=objectId, $2=depth
    /// PostgreSQL: SELECT get_object_json($1, $2)::text
    /// MSSQL: Stored procedure or CTE
    /// </summary>
    string ObjectStorage_GetObjectJson();
    
    /// <summary>
    /// SELECT objects as JSON (bulk). Params: $1=objectIds (array), $2=depth
    /// PostgreSQL: SELECT get_object_json(id, $2)::text FROM unnest($1::bigint[]) AS id
    /// MSSQL: Cursor or table-valued function
    /// </summary>
    string ObjectStorage_GetObjectsJsonBulk();
    
    /// <summary>
    /// SELECT structure metadata by scheme ID. Params: $1=schemeId
    /// Returns: Id, IdParent, Name, DbType, CollectionType, KeyType, StoreNull, TypeSemantic
    /// </summary>
    string ObjectStorage_SelectStructuresWithMetadata();
    
    /// <summary>
    /// SELECT existing values with types. Params: $1=objectId, $2=structureIds (array)
    /// Returns: Id, IdStructure, IdObject, String, Long, Guid, Double, DateTimeOffset, Boolean, ByteArray, Numeric, ListItem, Object, ArrayParentId, ArrayIndex, DbType
    /// </summary>
    string ObjectStorage_SelectValuesWithTypes();
    
    /// <summary>
    /// SELECT structure types by IDs. Params: $1=structureIds (array)
    /// Returns: StructureId, DbType
    /// </summary>
    string ObjectStorage_SelectStructureTypes();
    
    /// <summary>
    /// SELECT type info by ID. Params: $1=typeId
    /// Returns: Id, Name, DbType, Type1
    /// </summary>
    string ObjectStorage_SelectTypeById();
    
    /// <summary>
    /// Check if object exists by ID. Params: $1=objectId
    /// Returns: _id or NULL
    /// </summary>
    string ObjectStorage_CheckObjectExists();
    
    /// <summary>
    /// SELECT scheme by ID. Params: $1=schemeId
    /// Returns: Id, Name
    /// </summary>
    string ObjectStorage_SelectSchemeById();
    
    /// <summary>
    /// SELECT all values for object IDs (ChangeTracking). Params: $1=objectIds (array)
    /// Returns: Id, IdStructure, IdObject, String, Long, Guid, Double, DateTimeOffset, Boolean, ByteArray, Numeric, ListItem, Object, ArrayParentId, ArrayIndex
    /// </summary>
    string ObjectStorage_SelectValuesForObjects();
    
    /// <summary>
    /// SELECT scheme IDs for objects. Params: $1=objectIds (array)
    /// Returns: ObjectId, SchemeId
    /// </summary>
    string ObjectStorage_SelectSchemeIdsForObjects();
    
    /// <summary>
    /// SELECT single value by ID. Params: $1=valueId
    /// Returns: all value fields
    /// </summary>
    string ObjectStorage_SelectValueById();
    
    /// <summary>
    /// SELECT all types (for cache preload). No params.
    /// Returns: Id, Name, DbType, Type1
    /// </summary>
    string ObjectStorage_SelectAllTypes();
    
    /// <summary>
    /// SELECT existing object IDs from array. Params: $1=objectIds (array)
    /// Returns: Id only
    /// </summary>
    string ObjectStorage_SelectExistingIds();
    
    /// <summary>
    /// SELECT schemes by IDs. Params: $1=schemeIds (array)
    /// Returns: Id, Name, Alias, NameSpace, StructureHash, Type
    /// </summary>
    string ObjectStorage_SelectSchemesByIds();
    
    /// <summary>
    /// Lock objects for update (row locking). Params: $1=objectIds (array)
    /// PostgreSQL: SELECT 1 FROM _objects WHERE _id = ANY($1) FOR UPDATE
    /// MSSQL: SELECT 1 FROM _objects WITH (UPDLOCK) WHERE _id IN (...)
    /// </summary>
    string ObjectStorage_LockObjectsForUpdate();
    
    /// <summary>
    /// SELECT scheme ID for object. Params: $1=objectId
    /// Returns: _id_scheme (long)
    /// </summary>
    string ObjectStorage_SelectSchemeIdByObjectId();
    
    // ============================================================
    // === LIST SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT list by ID. Params: $1=listId
    /// </summary>
    string Lists_SelectById();
    
    /// <summary>
    /// SELECT list by name. Params: $1=name
    /// </summary>
    string Lists_SelectByName();
    
    /// <summary>
    /// SELECT all lists ordered by name.
    /// </summary>
    string Lists_SelectAll();
    
    /// <summary>
    /// INSERT new list. Params: $1=id, $2=name, $3=alias
    /// </summary>
    string Lists_Insert();
    
    /// <summary>
    /// UPDATE list. Params: $1=name, $2=alias, $3=id
    /// </summary>
    string Lists_Update();
    
    /// <summary>
    /// DELETE list by ID. Params: $1=listId
    /// </summary>
    string Lists_Delete();
    
    /// <summary>
    /// Check if list is used in structures. Params: $1=listId
    /// </summary>
    string Lists_IsUsedInStructures();
    
    // ============================================================
    // === LIST ITEMS SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT list item by ID. Params: $1=itemId
    /// </summary>
    string ListItems_SelectById();
    
    /// <summary>
    /// SELECT list items by list ID. Params: $1=listId
    /// </summary>
    string ListItems_SelectByListId();
    
    /// <summary>
    /// SELECT list item by list ID and value. Params: $1=listId, $2=value
    /// </summary>
    string ListItems_SelectByListIdAndValue();
    
    /// <summary>
    /// INSERT list item. Params: $1=id, $2=idList, $3=value, $4=alias, $5=idObject
    /// </summary>
    string ListItems_Insert();
    
    /// <summary>
    /// UPDATE list item alias and idObject. Params: $1=alias, $2=idObject, $3=id
    /// </summary>
    string ListItems_UpdateAliasAndObject();
    
    /// <summary>
    /// UPDATE list item. Params: $1=value, $2=alias, $3=idObject, $4=id
    /// </summary>
    string ListItems_Update();
    
    /// <summary>
    /// DELETE list item by ID. Params: $1=itemId
    /// </summary>
    string ListItems_Delete();
    
    /// <summary>
    /// SELECT list items by object reference. Params: $1=objectId
    /// </summary>
    string ListItems_SelectByObjectId();
    
    // ============================================================
    // === VALIDATION SQL ===
    // ============================================================
    
    /// <summary>
    /// SELECT all types for validation.
    /// </summary>
    string Validation_SelectAllTypes();
    
    /// <summary>
    /// SELECT scheme by name. Params: $1=schemeName
    /// </summary>
    string Validation_SelectSchemeByName();
    
    /// <summary>
    /// SELECT structures by scheme ID. Params: $1=schemeId
    /// </summary>
    string Validation_SelectStructuresBySchemeId();
    
    // ============================================================
    // === LAZY LOADER SQL ===
    // ============================================================
    
    /// <summary>
    /// Get object base fields (without Props). Params: $1=objectId
    /// </summary>
    string LazyLoader_SelectObjectBase();
    
    /// <summary>
    /// Get object as JSON via get_object_json. Params: $1=objectId, $2=maxDepth
    /// </summary>
    string LazyLoader_GetObjectJson();
    
    /// <summary>
    /// Get multiple objects as JSON via get_object_json batch. Params: $1=objectIds array
    /// Returns: (Id, JsonData) tuples
    /// </summary>
    string LazyLoader_GetObjectJsonBatch();
    
    /// <summary>
    /// Get object hash for cache validation. Params: $1=objectId
    /// </summary>
    string LazyLoader_SelectObjectHash();
    
    // ============================================================
    // === QUERY PROVIDER SQL ===
    // ============================================================
    
    /// <summary>
    /// Name of the search function for objects with facets (eager loading).
    /// PostgreSQL: "search_objects_with_facets"
    /// </summary>
    string Query_SearchObjectsFunction();
    
    /// <summary>
    /// Name of the search function for objects base fields only (lazy loading).
    /// PostgreSQL: "search_objects_with_facets_base"
    /// </summary>
    string Query_SearchObjectsBaseFunction();
    
    /// <summary>
    /// Name of the search function with projection by paths.
    /// PostgreSQL: "search_objects_with_projection_by_paths"
    /// </summary>
    string Query_SearchObjectsProjectionByPathsFunction();
    
    /// <summary>
    /// Name of the search function with projection by IDs.
    /// PostgreSQL: "search_objects_with_projection_by_ids"
    /// </summary>
    string Query_SearchObjectsProjectionByIdsFunction();
    
    /// <summary>
    /// Name of the tree search function (eager loading).
    /// PostgreSQL: "search_tree_objects_with_facets"
    /// </summary>
    string Query_SearchTreeObjectsFunction();
    
    /// <summary>
    /// Name of the tree search function base fields only (lazy loading).
    /// PostgreSQL: "search_tree_objects_with_facets_base"
    /// </summary>
    string Query_SearchTreeObjectsBaseFunction();
    
    /// <summary>
    /// SQL template for COUNT query. Params: functionName
    /// PostgreSQL: "SELECT ({0}($1, $2::jsonb, NULL, NULL, NULL, $3))->>'total_count'"
    /// </summary>
    string Query_CountTemplate();
    
    /// <summary>
    /// SQL template for search query result. Params: functionName
    /// PostgreSQL: "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6) as result"
    /// </summary>
    string Query_SearchTemplate();
    
    /// <summary>
    /// SQL template for search query with distinct. Params: functionName
    /// PostgreSQL: "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6, $7) as result"
    /// </summary>
    string Query_SearchWithDistinctTemplate();
    
    /// <summary>
    /// SQL template for full search query with distinct and facets. Params: functionName
    /// PostgreSQL: "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6, $7, $8) as result"
    /// </summary>
    string Query_SearchFullTemplate();
    
    /// <summary>
    /// JSON cast expression for the database.
    /// PostgreSQL: "::jsonb", MSSQL: ""
    /// </summary>
    string Query_JsonCast();
    
    /// <summary>
    /// Text array cast. PostgreSQL: "::text[]"
    /// </summary>
    string Query_TextArrayCast();
    
    /// <summary>
    /// Bigint array cast. PostgreSQL: "::bigint[]"
    /// </summary>
    string Query_BigintArrayCast();
    
    /// <summary>
    /// SQL for projection by paths query.
    /// </summary>
    string Query_ProjectionByPathsTemplate();
    
    /// <summary>
    /// SQL for projection by structure IDs query.
    /// </summary>
    string Query_ProjectionByIdsTemplate(string structureIdsArray);
    
    /// <summary>
    /// SQL for checking user permission on object.
    /// </summary>
    string Query_CheckPermissionSql();
    
    /// <summary>
    /// SQL for aggregate batch preview.
    /// </summary>
    string Query_AggregateBatchPreviewSql();
    
    /// <summary>
    /// SQL for aggregate field.
    /// </summary>
    string Query_AggregateFieldSql();
    
    /// <summary>
    /// SQL for SQL preview function.
    /// </summary>
    string Query_SqlPreviewTemplate();
    
    /// <summary>
    /// SQL for aggregate batch function.
    /// </summary>
    string Query_AggregateBatchSql();
    
    /// <summary>
    /// SQL for simple search objects (used in distinct).
    /// </summary>
    string Query_SearchObjectsSimpleSql();
    
    /// <summary>
    /// SQL for grouped aggregation.
    /// </summary>
    string Query_AggregateGroupedSql();
    
    /// <summary>
    /// SQL for array grouped aggregation.
    /// </summary>
    string Query_AggregateArrayGroupedSql();
    
    /// <summary>
    /// SQL for window query.
    /// </summary>
    string Query_WindowSql();
    
    /// <summary>
    /// SQL for tree count query (normal search).
    /// </summary>
    string Query_TreeCountNormalSql(string functionName);
    
    /// <summary>
    /// SQL for tree search query (normal search).
    /// </summary>
    string Query_TreeSearchNormalSql(string functionName);
    
    /// <summary>
    /// SQL for tree search with multiple parent IDs.
    /// PostgreSQL: SELECT result->>'objects' FROM func($1,$2,...) as result
    /// MSSQL: EXEC func @p0, @p1, ...
    /// </summary>
    string Query_TreeSearchWithParentIdsSql(string functionName);
    
    /// <summary>
    /// SQL for HasAncestor tree search - extracts IDs from JSON result.
    /// PostgreSQL: WITH ... SELECT jsonb_array_elements(...)
    /// MSSQL: WITH ... CROSS APPLY OPENJSON(...)
    /// </summary>
    string Query_HasAncestorTreeSql(string functionName);
    
    /// <summary>
    /// SQL for HasAncestor normal search - extracts IDs from JSON result.
    /// </summary>
    string Query_HasAncestorNormalSql(string functionName);
    
    /// <summary>
    /// SQL for HasDescendant search - extracts IDs from JSON result.
    /// </summary>
    string Query_HasDescendantSql(string functionName);
    
    /// <summary>
    /// SQL for getting parent IDs from descendant IDs using recursive CTE.
    /// PostgreSQL: WITH RECURSIVE ancestors AS (...)
    /// MSSQL: WITH ancestors AS (...)
    /// </summary>
    string Query_GetParentIdsFromDescendantsSql(string idsString, int depthLimit);
    
    /// <summary>
    /// SQL for tree COUNT with parent_ids array.
    /// PostgreSQL: SELECT (result->>'total_count')::int FROM func(...) as result
    /// MSSQL: temp table + JSON_VALUE
    /// </summary>
    string Query_TreeCountWithParentIdsSql(string functionName);
    
    /// <summary>
    /// SQL for getting all IDs with their ancestors using recursive CTE.
    /// Used by ToTreeListAsync to build parent chains.
    /// PostgreSQL: WITH RECURSIVE parent_chain AS (...)
    /// MSSQL: WITH parent_chain AS (...)
    /// </summary>
    string Query_GetIdsWithAncestorsSql(string idsString);
    
    /// <summary>
    /// SQL for loading objects by IDs as JSON.
    /// PostgreSQL: SELECT get_object_json(id, 10)::text FROM unnest(ARRAY[...])
    /// MSSQL: SELECT dbo.get_object_json(_id, 10) FROM _objects WHERE _id IN (...)
    /// </summary>
    string Query_LoadObjectsByIdsSql(string idsString, int maxDepth);
    
    /// <summary>
    /// SQL for tree SQL preview.
    /// </summary>
    string Query_TreeSqlPreviewTemplate(string functionName);
    
    // ============================================================
    // === SQL PREVIEW FUNCTION NAMES ===
    // ============================================================
    
    /// <summary>
    /// SQL preview function name for regular search.
    /// PostgreSQL: "get_search_sql_preview"
    /// MSSQL: "dbo.get_search_sql_preview"
    /// </summary>
    string Query_SqlPreviewFunction();
    
    /// <summary>
    /// SQL preview function name for base (lazy loading) search.
    /// PostgreSQL: "get_search_sql_preview_base"
    /// MSSQL: "dbo.get_search_sql_preview_base"
    /// </summary>
    string Query_SqlPreviewBaseFunction();
    
    /// <summary>
    /// SQL preview function name for tree search.
    /// PostgreSQL: "get_search_tree_sql_preview"
    /// MSSQL: "dbo.get_search_tree_sql_preview"
    /// </summary>
    string Query_TreeSqlPreviewFunction();
    
    /// <summary>
    /// SQL preview function name for tree base (lazy loading) search.
    /// PostgreSQL: "get_search_tree_sql_preview_base"
    /// MSSQL: "dbo.get_search_tree_sql_preview_base"
    /// </summary>
    string Query_TreeSqlPreviewBaseFunction();
    
    // =====================================================
    // SOFT DELETE METHODS
    // =====================================================
    
    /// <summary>
    /// SQL to mark objects for soft-deletion.
    /// Creates trash container and moves objects under it.
    /// Params: $1=objectIds (array/comma-separated), $2=userId, $3=trashParentId (nullable)
    /// Returns: trash_id, marked_count
    /// PostgreSQL: SELECT * FROM mark_for_deletion($1, $2, $3)
    /// MSSQL: EXEC sp_mark_for_deletion @p0, @p1, @p2 (with OUTPUT params)
    /// </summary>
    string SoftDelete_MarkForDeletion();
    
    /// <summary>
    /// SQL to purge (physically delete) objects from trash container.
    /// Deletes in batches, removes trash container when empty.
    /// Params: $1=trashId, $2=batchSize
    /// Returns: deleted_count, remaining_count
    /// PostgreSQL: SELECT * FROM purge_trash($1, $2)
    /// MSSQL: EXEC sp_purge_trash @p0, @p1 (with OUTPUT params)
    /// </summary>
    string SoftDelete_PurgeTrash();
    
    /// <summary>
    /// SQL query to get deletion progress for a specific trash container.
    /// Reads progress from trash object fields: _value_long=total, _key=deleted, _value_string=status.
    /// Params: $1=trashId
    /// Returns: trash_id, total, deleted, status, started_at, owner_id
    /// </summary>
    string SoftDelete_GetDeletionProgress();
    
    /// <summary>
    /// SQL query to get all active deletions for a user.
    /// Returns trash containers with status='pending' or 'running' owned by user.
    /// Params: $1=userId
    /// Returns: trash_id, total, deleted, status, started_at, owner_id
    /// </summary>
    string SoftDelete_GetUserActiveDeletions();
    
    /// <summary>
    /// SQL query to get orphaned deletion tasks (for recovery at startup).
    /// Returns 'pending' tasks OR 'running' tasks where _date_modify older than timeout.
    /// CLUSTER-SAFE: Only returns tasks that are likely abandoned.
    /// Params: $1=timeoutMinutes (int)
    /// Returns: trash_id, total, deleted, status, owner_id
    /// </summary>
    string SoftDelete_GetOrphanedTasks();
    
    /// <summary>
    /// SQL to atomically claim an orphaned task (prevent race condition in cluster).
    /// Updates status to 'running' and _date_modify to NOW() only if conditions still match.
    /// CLUSTER-SAFE: Uses atomic UPDATE with condition check.
    /// Params: $1=trashId, $2=timeoutMinutes
    /// Returns: claimed (1 = success, 0 = task already taken by another instance)
    /// </summary>
    string SoftDelete_ClaimOrphanedTask();
    
    // ============================================================
    // === METADATA CACHE WARMUP ===
    // ============================================================
    
    /// <summary>
    /// SQL to warmup all metadata caches.
    /// PostgreSQL: SELECT scheme_id, structures_count, scheme_name FROM warmup_all_metadata_caches()
    /// MSSQL: EXEC warmup_all_metadata_caches (returns same columns)
    /// Returns: scheme_id, structures_count, scheme_name
    /// </summary>
    string Warmup_AllMetadataCaches();
}

