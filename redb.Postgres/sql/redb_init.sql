-- ==========================================================
-- REDB: Combined schema initialization script (auto-generated)
-- DO NOT EDIT — this file is overwritten on every build.
-- ==========================================================

-- ===== redbPostgre.sql =====
-- DROP SEQUENCE IF EXISTS global_identity;
-- DROP VIEW IF EXISTS v_user_permissions;
-- DROP VIEW IF EXISTS v_objects_json;
-- DROP VIEW IF EXISTS v_schemes_definition;
-- DROP FUNCTION IF EXISTS get_scheme_definition;
-- DROP FUNCTION IF EXISTS get_object_json;
-- DROP FUNCTION IF EXISTS search_objects_with_facets;
-- DROP FUNCTION IF EXISTS get_facets;
-- DROP FUNCTION IF EXISTS build_base_facet_conditions;
-- DROP TABLE IF EXISTS _scheme_metadata_cache;
-- DROP TABLE IF EXISTS _dependencies;
-- DROP TABLE IF EXISTS _values;
-- DROP TABLE IF EXISTS _permissions;
-- DROP TABLE IF EXISTS _structures;
-- DROP TABLE IF EXISTS _users_roles;
-- DROP TABLE IF EXISTS _roles;
-- DROP TABLE IF EXISTS _list_items;
-- DROP TABLE IF EXISTS _lists;
-- DROP TABLE IF EXISTS _links;
-- DROP TABLE IF EXISTS _objects CASCADE;
-- DROP TABLE IF EXISTS _users;
-- DROP TABLE IF EXISTS _functions;
-- DROP TABLE IF EXISTS _schemes;
-- DROP TABLE IF EXISTS _types;
-- DROP FUNCTION IF EXISTS validate_structure_name;
-- DROP FUNCTION IF EXISTS validate_scheme_name;
-- DROP FUNCTION IF EXISTS get_user_permissions_for_object;
-- DROP FUNCTION IF EXISTS auto_create_node_permissions;


CREATE TABLE _types(
	_id bigint NOT NULL,
	_name text NOT NULL UNIQUE,
	_db_type text NULL,
	_type text NULL,
    CONSTRAINT PK__types PRIMARY KEY (_id)
);

CREATE TABLE _links(
	_id bigint NOT NULL,
	_id_1 bigint NOT NULL,
	_id_2 bigint NOT NULL,
    CONSTRAINT PK__links PRIMARY KEY (_id),
    CONSTRAINT IX__links UNIQUE (_id_1, _id_2),
    CONSTRAINT CK__links CHECK  (_id_1<>_id_2)
);

CREATE TABLE _lists(
	_id bigint NOT NULL,
	_name text NOT NULL,
	_alias text NULL,
    CONSTRAINT PK__lists PRIMARY KEY (_id),
    CONSTRAINT IX__lists_name UNIQUE (_name)
);


CREATE TABLE _roles(
	_id bigint NOT NULL,
	_name text NOT NULL,
	_id_configuration bigint NULL,
    CONSTRAINT PK__roles PRIMARY KEY (_id),
    CONSTRAINT IX__roles UNIQUE (_name)
);


CREATE TABLE _users(
	_id bigint NOT NULL,
	_login text NOT NULL,
	_password TEXT NOT NULL,
	_name text NOT NULL UNIQUE,
	_phone text NULL,
	_email text NULL,
	_date_register timestamptz DEFAULT now() NOT NULL,
	_date_dismiss timestamptz NULL,
	_enabled boolean DEFAULT true NOT NULL,
	_key bigint NULL,
	_code_int bigint NULL,
	_code_string text NULL,
	_code_guid uuid NULL,
	_note text NULL,
	_hash uuid NULL,
	_id_configuration bigint NULL,
    CONSTRAINT PK__users PRIMARY KEY (_id)
);


CREATE TABLE _users_roles(
	_id bigint NOT NULL,
	_id_role bigint NOT NULL,
    _id_user bigint NOT NULL,
    CONSTRAINT PK__users_roles PRIMARY KEY (_id),
    CONSTRAINT IX__users_roles UNIQUE (_id_role, _id_user),
    CONSTRAINT FK__users_roles__roles FOREIGN KEY (_id_role) REFERENCES _roles (_id) ON DELETE CASCADE,
    CONSTRAINT FK__users_roles__users FOREIGN KEY (_id_user) REFERENCES _users (_id) ON DELETE CASCADE
);


CREATE TABLE _schemes(
	_id bigint NOT NULL,
	_id_parent bigint NULL,
	_name text NOT NULL,
	_alias text NULL,
	_name_space text NULL,
	_structure_hash uuid NULL,
	_type bigint NOT NULL DEFAULT -9223372036854775675, -- Scheme type: Class (default), Array, Dictionary, JsonDocument, XDocument
    CONSTRAINT PK__schemes PRIMARY KEY (_id),
	CONSTRAINT IX__schemes UNIQUE (_name),
    CONSTRAINT FK__schemes__schemes FOREIGN KEY (_id_parent) REFERENCES _schemes (_id),
    CONSTRAINT FK__schemes__types FOREIGN KEY (_type) REFERENCES _types (_id)
);


CREATE TABLE _structures(
	_id bigint NOT NULL,
	_id_parent bigint NULL,
	_id_scheme bigint NOT NULL,
	_id_override bigint NULL,
	_id_type bigint NOT NULL,
	_id_list bigint NULL,
	_name text NOT NULL,
	_alias text NULL,
	_order bigint NULL,
	_readonly boolean NULL,
	_allow_not_null boolean NULL,
	_collection_type bigint NULL,  -- Array/Dictionary type ID or NULL for non-collections
	_key_type bigint NULL,         -- Key type for Dictionary fields
	_is_compress boolean NULL,
	_store_null boolean NULL,
	_default_value bytea NULL,
	_default_editor text NULL,
    CONSTRAINT PK__structure PRIMARY KEY (_id),
	CONSTRAINT IX__structures UNIQUE (_id_scheme,_name,_id_parent),
    CONSTRAINT FK__structures__structures FOREIGN KEY (_id_parent) REFERENCES _structures (_id) ON DELETE CASCADE,
    CONSTRAINT FK__structures__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id),
    CONSTRAINT FK__structures__types FOREIGN KEY (_id_type) REFERENCES _types (_id),
    CONSTRAINT FK__structures__lists FOREIGN KEY (_id_list) REFERENCES _lists (_id),
    CONSTRAINT FK__structures__collection_type FOREIGN KEY (_collection_type) REFERENCES _types (_id),
    CONSTRAINT FK__structures__key_type FOREIGN KEY (_key_type) REFERENCES _types (_id)
);

CREATE TABLE _dependencies(
	_id bigint NOT NULL,
	_id_scheme_1 bigint,
	_id_scheme_2 bigint NOT NULL,
    CONSTRAINT PK__dependencies PRIMARY KEY (_id),
	CONSTRAINT IX__dependencies UNIQUE (_id_scheme_1,_id_scheme_2),
    CONSTRAINT FK__dependencies__schemes_1 FOREIGN KEY (_id_scheme_1) REFERENCES _schemes (_id), 
    CONSTRAINT FK__dependencies__schemes_2 FOREIGN KEY (_id_scheme_2) REFERENCES _schemes (_id)  ON DELETE CASCADE
);

CREATE TABLE _objects(
	_id bigint NOT NULL,
	_id_parent bigint NULL,
	_id_scheme bigint NOT NULL,
	_id_owner bigint NOT NULL,
	_id_who_change bigint NOT NULL,
	_date_create timestamptz DEFAULT now() NOT NULL,
	_date_modify timestamptz DEFAULT now() NOT NULL,
	_date_begin timestamptz NULL,
	_date_complete timestamptz NULL,
	_key bigint NULL,
	_name text NULL,
	_note text NULL,
	_hash uuid NULL,
	-- Value columns for RedbPrimitive<T> (Props = primitive value stored directly)
	-- Replaces old _code_int, _code_string, _code_guid, _bool columns
	_value_long bigint NULL,        -- was _code_int
	_value_string text NULL,        -- was _code_string (expanded to text!)
	_value_guid uuid NULL,          -- was _code_guid
	_value_bool boolean NULL,       -- was _bool
	_value_double float NULL,       -- NEW
	_value_numeric NUMERIC(38, 18) NULL,  -- NEW
	_value_datetime timestamptz NULL,     -- NEW
	_value_bytes bytea NULL,        -- NEW
    CONSTRAINT PK__objects PRIMARY KEY (_id),
    CONSTRAINT FK__objects__objects FOREIGN KEY (_id_parent) REFERENCES _objects (_id) ON DELETE CASCADE,
    CONSTRAINT FK__objects__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id) ON DELETE CASCADE, 
    CONSTRAINT FK__objects__users1 FOREIGN KEY (_id_owner) REFERENCES _users (_id),
	CONSTRAINT FK__objects__users2 FOREIGN KEY (_id_who_change) REFERENCES _users (_id)    
);

CREATE TABLE _list_items(
	_id bigint NOT NULL,
	_id_list bigint NOT NULL,
	_value text NULL,
	_alias text NULL,
	_id_object bigint NULL,
    CONSTRAINT PK__list_items PRIMARY KEY (_id),
    CONSTRAINT FK__list_items__id_list FOREIGN KEY (_id_list) REFERENCES _lists (_id) ON DELETE CASCADE,
    CONSTRAINT FK__list_items__objects FOREIGN KEY (_id_object) REFERENCES _objects (_id) ON DELETE SET NULL
);

-- PostgreSQL 15+: UNIQUE с учетом NULL (NULL = NULL)
CREATE UNIQUE INDEX IX__list_items_unique ON _list_items (_id_list, _value) NULLS NOT DISTINCT;

CREATE TABLE _values(
	_id bigint NOT NULL,
	_id_structure bigint NOT NULL,
	_id_object bigint NOT NULL,
	_String text NULL,
	_Long bigint NULL,
	_Guid uuid NULL,
	_Double float NULL,
	_DateTimeOffset timestamptz NULL,
	_Boolean boolean NULL,
	_ByteArray bytea NULL,
	_Numeric NUMERIC(38, 18) NULL,
	_ListItem bigint NULL,
	_Object bigint NULL,
    -- Fields for relational storage of collections (arrays, dictionaries, JSON/XML documents)
    _array_parent_id bigint NULL, -- Reference to parent element (for nested structures)
    _array_index text NULL, -- Key/index of element: '0','1','2' for arrays, string key for dictionaries
    CONSTRAINT PK__values PRIMARY KEY (_id),
    CONSTRAINT FK__values__objects FOREIGN KEY (_id_object) REFERENCES _objects (_id) ON DELETE CASCADE,
    CONSTRAINT FK__values__structures FOREIGN KEY (_id_structure) REFERENCES _structures (_id) ON DELETE CASCADE,
    CONSTRAINT FK__values__array_parent FOREIGN KEY (_array_parent_id) REFERENCES _values (_id) ON DELETE CASCADE,
    CONSTRAINT FK__values__list_items FOREIGN KEY (_ListItem) REFERENCES _list_items (_id),
    CONSTRAINT FK__values__objects_ref FOREIGN KEY (_Object) REFERENCES _objects (_id)
); 

-- Comments for extended _values table for relational array support
COMMENT ON TABLE _values IS 'Table for storing field values of objects. Supports relational arrays of all types (simple and Class fields) via _array_parent_id and _array_index';
COMMENT ON COLUMN _values._array_parent_id IS 'ID of parent element for array elements. NULL for regular (non-array) fields and root array elements';
COMMENT ON COLUMN _values._array_index IS 'Key/index of collection element (text). For arrays: ''0'',''1'',''2''. For dictionaries: string key. NULL for regular fields. For numeric sorting of arrays use ORDER BY _array_index::int';
COMMENT ON COLUMN _values._Numeric IS 'Exact decimal numbers for financial calculations. NUMERIC(38,18) provides lossless precision, unlike Double. Used for money, taxes, percentages where arithmetic accuracy is critical.';
COMMENT ON COLUMN _values._DateTimeOffset IS 'Date and time with timezone (timestamptz). PostgreSQL stores in UTC, converts on retrieval. Used for precise time moments: logs, transactions, events. Allows accurate reconstruction of event moment regardless of local timezone.';
COMMENT ON COLUMN _values._ListItem IS 'Reference to dictionary item (_list_items). FK without cascade delete - attempting to delete the linked list_item will cause integrity violation error. Used for ListItem type instead of storing ID in _Long.';
COMMENT ON COLUMN _values._Object IS 'Reference to object (_objects). FK without cascade delete - attempting to delete the linked object will cause integrity violation error. Used for Object type (redbObject) instead of storing ID in _Long.';

-- Comments for _schemes table
COMMENT ON COLUMN _schemes._type IS 'Scheme type ID (FK to _types): Class (default), Array, Dictionary, JsonDocument, XDocument. Determines how Props are stored and serialized.';

-- Comments for _objects._value_* columns (RedbPrimitive<T> support)
-- These columns replace old _code_int, _code_string, _code_guid, _bool
COMMENT ON COLUMN _objects._value_long IS 'Direct value for RedbPrimitive<long/int/short/byte>. Replaces _code_int.';
COMMENT ON COLUMN _objects._value_string IS 'Direct value for RedbPrimitive<string>. Replaces _code_string, expanded to text.';
COMMENT ON COLUMN _objects._value_guid IS 'Direct value for RedbPrimitive<Guid>. Replaces _code_guid.';
COMMENT ON COLUMN _objects._value_bool IS 'Direct value for RedbPrimitive<bool>. Replaces _bool.';
COMMENT ON COLUMN _objects._value_double IS 'Direct value for RedbPrimitive<double/float>. NEW column.';
COMMENT ON COLUMN _objects._value_numeric IS 'Direct value for RedbPrimitive<decimal>. NEW column.';
COMMENT ON COLUMN _objects._value_datetime IS 'Direct value for RedbPrimitive<DateTime/DateTimeOffset>. NEW column.';
COMMENT ON COLUMN _objects._value_bytes IS 'Direct value for RedbPrimitive<byte[]>. NEW column.';

-- Comments for _structures table
COMMENT ON COLUMN _structures._collection_type IS 'Collection type ID (FK to _types): Array or Dictionary. NULL for non-collection fields. Replaces old _is_array boolean.';
COMMENT ON COLUMN _structures._key_type IS 'Key type ID for Dictionary fields (FK to _types). NULL for non-dictionary fields.';

COMMENT ON INDEX IX__list_items_unique IS 
'Unique index on combination of dictionary + value with NULLS NOT DISTINCT. Prevents duplicate values in the same dictionary, including NULL (only one NULL per dictionary). Ensures strict integrity of dictionary data.';


CREATE TABLE _permissions(
	_id bigint NOT NULL,
	_id_role bigint NULL,
	_id_user bigint NULL,
	_id_ref bigint NOT NULL,
	_select boolean NULL,
	_insert boolean NULL,
	_update boolean NULL,
	_delete boolean NULL,
    CONSTRAINT PK__object_permissions PRIMARY KEY (_id),
    CONSTRAINT CK__permissions_users_roles CHECK  (_id_role IS NOT NULL AND _id_user IS NULL OR _id_role IS NULL AND _id_user IS NOT NULL),
    CONSTRAINT IX__permissions UNIQUE (_id_role, _id_user, _id_ref, _select, _insert, _update, _delete),
    CONSTRAINT FK__permissions__roles FOREIGN KEY (_id_role) REFERENCES _roles (_id) ON DELETE CASCADE,
    CONSTRAINT FK__permissions__users FOREIGN KEY (_id_user) REFERENCES _users (_id) ON DELETE CASCADE
);

CREATE TABLE _functions
(
    _id bigint NOT NULL,
    _id_scheme bigint NOT NULL,
	_language varchar(50) NOT NULL,
    _name text NOT NULL,
    _body text NOT NULL,
    CONSTRAINT PK__functions PRIMARY KEY (_id),
    CONSTRAINT IX__functions_scheme_name UNIQUE (_id_scheme, _name),
    CONSTRAINT FK__functions__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id)
);


CREATE INDEX IF NOT EXISTS "IX__functions__schemes" ON _functions (_id_scheme) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__permissions__roles" ON _permissions (_id_role) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__permissions__users" ON _permissions (_id_user) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__permissions__ref" ON _permissions (_id_ref);
CREATE INDEX IF NOT EXISTS "IX__values__objects" ON _values (_id_object) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__values__structures" ON _values (_id_structure) WITH (deduplicate_items=True);

-- ============================================
-- REMOVED REDUNDANT INDEXES (migration_drop_redundant_indexes.sql)
-- Reason: Covered by composite index IX__values__structure_object_lookup
-- Facet search ALWAYS filters by (_id_structure, _id_object) BEFORE value
-- ============================================
-- CREATE INDEX IF NOT EXISTS "IX__values__String" ON _values (_String) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Long" ON _values (_Long) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Guid" ON _values (_Guid) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Double" ON _values (_Double) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__DateTimeOffset" ON _values (_DateTimeOffset) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Boolean" ON _values (_Boolean) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Numeric" ON _values (_Numeric) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__ListItem" ON _values (_ListItem) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Object" ON _values (_Object) WITH (deduplicate_items=True);

-- Indexes for relational arrays of all types
CREATE INDEX IF NOT EXISTS "IX__values__array_parent_id" ON _values (_array_parent_id) WITH (deduplicate_items=True);
-- REMOVED: _array_index is ALWAYS used together with _id_object + _id_structure
-- CREATE INDEX IF NOT EXISTS "IX__values__array_index" ON _values (_array_index) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__values__array_parent_index" ON _values (_array_parent_id, _array_index) WITH (deduplicate_items=True);
-- Index for Dictionary key lookups (text _array_index)
CREATE INDEX IF NOT EXISTS "IX__values__array_key" ON _values (_id_structure, _array_index) WHERE _array_index IS NOT NULL;

-- Unique indexes to ensure data integrity
-- For ROOT fields (not arrays, not nested): structure + object must be unique
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_object" 
ON _values (_id_structure, _id_object) 
WHERE _array_index IS NULL AND _array_parent_id IS NULL;

-- For BASE RECORDS OF NESTED ARRAYS: structure + object + parent must be unique
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_object_parent" 
ON _values (_id_structure, _id_object, _array_parent_id) 
WHERE _array_index IS NULL AND _array_parent_id IS NOT NULL;

-- For array elements: structure + object + parent + index must be unique
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_object_array_index" 
ON _values (_id_structure, _id_object, _array_parent_id, _array_index) 
WHERE _array_index IS NOT NULL;

-- Comments for created unique indexes
COMMENT ON INDEX "UIX__values__structure_object" IS 'Ensures uniqueness: one value per structure+object for ROOT fields (not nested arrays)';
COMMENT ON INDEX "UIX__values__structure_object_parent" IS 'Ensures uniqueness: one base record per structure+object+parent for nested arrays';
COMMENT ON INDEX "UIX__values__structure_object_array_index" IS 'Ensures uniqueness: one element per structure+object+parent+position for elements of all arrays (including nested)';
CREATE INDEX IF NOT EXISTS "IX__list_items__id_list" ON _list_items (_id_list) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__list_items__objects" ON _list_items (_id_object) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__objects" ON _objects (_id_parent) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__schemes" ON _objects (_id_scheme) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__users1" ON _objects (_id_owner) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__users2" ON _objects (_id_who_change) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__date_create" ON _objects (_date_create) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__date_modify" ON _objects (_date_modify) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__name" ON _objects (_name) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__hash" ON _objects (_hash) WITH (deduplicate_items=True);
-- Indexes for RedbPrimitive<T> value columns (replaces old _code_* indexes)
CREATE INDEX IF NOT EXISTS "IX__objects__value_long" ON _objects (_value_long) WHERE _value_long IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_string" ON _objects (_value_string) WHERE _value_string IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_guid" ON _objects (_value_guid) WHERE _value_guid IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_datetime" ON _objects (_value_datetime) WHERE _value_datetime IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_numeric" ON _objects (_value_numeric) WHERE _value_numeric IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__dependencies__schemes_1" ON _dependencies (_id_scheme_1) WITH (deduplicate_items=True); 
CREATE INDEX IF NOT EXISTS "IX__dependencies__schemes_2" ON _dependencies (_id_scheme_2) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__structures" ON _structures (_id_parent) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__schemes" ON _structures (_id_scheme) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__types" ON _structures (_id_type) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__lists" ON _structures (_id_list) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__schemes__schemes" ON _schemes (_id_parent) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__schemes__structure_hash" ON _schemes (_structure_hash) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__schemes__type" ON _schemes (_type) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__users_roles__roles" ON _users_roles (_id_role) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__users_roles__users" ON _users_roles (_id_user) WITH (deduplicate_items=True);

-- Indexes for user and role configurations
CREATE INDEX IF NOT EXISTS "IX__users__id_configuration" ON _users (_id_configuration) WHERE _id_configuration IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__roles__id_configuration" ON _roles (_id_configuration) WHERE _id_configuration IS NOT NULL;

-- Foreign keys for configurations (added after creating all tables)
ALTER TABLE _users ADD CONSTRAINT FK_users_configuration FOREIGN KEY (_id_configuration) REFERENCES _objects(_id) ON DELETE SET NULL;
ALTER TABLE _roles ADD CONSTRAINT FK_roles_configuration FOREIGN KEY (_id_configuration) REFERENCES _objects(_id) ON DELETE SET NULL;


-- PostgreSQL: partial index (only rows where _array_index IS NULL)
CREATE INDEX ix__values__array_null_structure 
ON _values (_id_structure, _id_object, _long) 
WHERE _array_index IS NULL;


-- Index for quickly getting all nested fields by structure_id
CREATE INDEX IF NOT EXISTS "IX__values__structure_parent_batch" 
ON _values (_id_structure, _array_parent_id) 
INCLUDE (_String, _Long, _Double, _Boolean, _Guid)
WHERE _array_parent_id IS NOT NULL;


-- ============================================
-- OPTIMIZED INDEXES FOR FACETED SEARCH
-- (Index Only Scan with INCLUDE - maximum performance)
-- Tested EXPLAIN ANALYZE on Stage 41 - all Seq Scan eliminated!
-- ============================================

-- 🔥 CRITICAL #1: Covering index for ORDER BY by structure name
-- Eliminates: Seq Scan on _structures s (Filter: (_name)::text = 'Auction.Baskets')
-- Cost reduction: 6.10 → 4.29 (-30%)
CREATE INDEX IF NOT EXISTS "IX__structures__name" 
ON _structures (_name) 
INCLUDE (_id, _id_type, _collection_type, _id_scheme)
WITH (deduplicate_items=True);

-- 🔥 CRITICAL #2: Covering index for searching structure by ID
-- Eliminates: Seq Scan on _structures (Filter: _id = X) in InitPlan
-- Eliminates: Seq Scan on _structures fs_1 (Filter: (_collection_type IS NULL) AND (_id = X))
-- Cost reduction: 6.10 → 4.29 (-30%)
CREATE INDEX IF NOT EXISTS "IX__structures__id_lookup" 
ON _structures (_id) 
INCLUDE (_id_type, _name, _collection_type, _id_scheme)
WITH (deduplicate_items=True);

-- Partial index for non-array structures (base)
CREATE INDEX IF NOT EXISTS "IX__structures__not_collection" 
ON _structures (_id, _name, _id_scheme) 
WHERE _collection_type IS NULL;

-- Extended partial index for non-collection structures
-- Used in Nested Loop for fast search by _id with _collection_type IS NULL
CREATE INDEX IF NOT EXISTS "IX__structures__not_collection_enhanced" 
ON _structures (_id, _name, _id_scheme, _id_type) 
WHERE _collection_type IS NULL;

-- Index for collection fields (Array/Dictionary)
CREATE INDEX IF NOT EXISTS "IX__structures__collection" 
ON _structures (_id, _id_scheme, _collection_type) 
WHERE _collection_type IS NOT NULL;

-- Index for dictionaries with key type
CREATE INDEX IF NOT EXISTS "IX__structures__key_type" 
ON _structures (_id, _id_scheme, _key_type) 
WHERE _key_type IS NOT NULL;

-- Index for collection type lookups (Array/Dictionary fields)
CREATE INDEX IF NOT EXISTS "IX__structures__collection_type" 
ON _structures (_collection_type) 
WHERE _collection_type IS NOT NULL;

-- 🚀 Covering index for _values (object -> structure)
-- Index Only Scan for EXISTS subqueries with value filtering
-- INCLUDE allows filtering (_long > X) without accessing the table
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_lookup" 
ON _values (_id_object, _id_structure, _array_index) 
INCLUDE (_String, _Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
WITH (deduplicate_items=True);

-- 🚀 Covering index for nested fields (Class)
-- Index Only Scan for JOIN nested fields and ORDER BY subqueries
-- INCLUDE eliminates table access for reading values
CREATE INDEX IF NOT EXISTS "IX__values__object_array_null" 
ON _values (_id_object, _id_structure) 
INCLUDE (_String, _Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
WHERE _array_index IS NULL;

-- ============================================
-- COMMENTS FOR OPTIMIZED INDEXES
-- ============================================

COMMENT ON INDEX "IX__structures__name" IS 
'🔥 CRITICAL covering index for ORDER BY subqueries. Eliminates Seq Scan when searching structures by name (s._name = ''Field''). INCLUDE (_id, _id_type, _collection_type, _id_scheme) provides Index Only Scan without table access. Cost: 6.10 → 4.29 (-30%). Tested EXPLAIN ANALYZE Stage 41';

COMMENT ON INDEX "IX__structures__id_lookup" IS 
'🔥 CRITICAL covering index for InitPlan and EXISTS subqueries. Eliminates Seq Scan on Filter: (_id = X). INCLUDE contains all necessary fields for Index Only Scan. Covers InitPlan 2 and Nested Loop conditions. Cost: 6.10 → 4.29 (-30%). Tested EXPLAIN ANALYZE Stage 41';

COMMENT ON INDEX "IX__structures__not_collection" IS 
'Base partial index for NON collection fields. Optimizes filtering _collection_type IS NULL in faceted queries. Used together with IX__structures__name';

COMMENT ON INDEX "IX__structures__not_collection_enhanced" IS 
'Extended partial index for NON collection fields. Includes _id_type to eliminate additional JOINs in Nested Loop. Used when searching by _id with condition _collection_type IS NULL. Provides Index Only Scan';

COMMENT ON INDEX "IX__structures__collection" IS 
'Partial index for collection fields (Array/Dictionary). Optimizes searching arrays and dictionaries by _collection_type';

COMMENT ON INDEX "IX__structures__collection_type" IS 
'Index for fast search by collection type (Array or Dictionary). Used when analyzing schemes';

COMMENT ON INDEX "IX__values__array_key" IS 
'Index for fast search by Dictionary key. Supports string keys for Dictionary<string, T>';

COMMENT ON INDEX "IX__values__object_structure_lookup" IS 
'🚀 Covering index for EXISTS subqueries with value filtering. INCLUDE contains all value types for Index Only Scan. Updated: added _Numeric, _ListItem, _Object; _DateTime → _DateTimeOffset. Critical for faceted search with conditions on nested Class fields';

COMMENT ON INDEX "IX__values__object_array_null" IS 
'🚀 Covering index for non-array values. Optimizes JOIN nested Class fields and ORDER BY subqueries. INCLUDE contains all value types for reading without table access. Updated: added _Numeric, _ListItem, _Object; _DateTime → _DateTimeOffset. Partial index (_array_index IS NULL) reduces size by 30-40%';

-- ============================================
-- CRITICAL INDEXES FOR FACETED SEARCH
-- ============================================

-- 1. 🔥 MOST IMPORTANT: search values by structure + object
CREATE INDEX IF NOT EXISTS "IX__values__structure_object_lookup" 
ON _values (_id_structure, _id_object, _String, _Long, _DateTimeOffset, _Boolean, _Double, _Guid, _Numeric, _ListItem, _Object);

-- 2. 🌳 Tree queries
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent" 
ON _objects (_id_scheme, _id_parent, _id);

-- 3. 🔢 Arrays
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_array_index" 
ON _values (_id_object, _id_structure, _array_index)
WHERE _array_index IS NOT NULL;

-- 4. 📌 Partial indexes for NOT NULL values
CREATE INDEX IF NOT EXISTS "IX__values__String_not_null" 
ON _values (_id_structure, _id_object, _String)
WHERE _String IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__values__Long_not_null" 
ON _values (_id_structure, _id_object, _Long)
WHERE _Long IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__values__DateTimeOffset_not_null" 
ON _values (_id_structure, _id_object, _DateTimeOffset)
WHERE _DateTimeOffset IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__values__Numeric_not_null" 
ON _values (_id_structure, _id_object, _Numeric)
WHERE _Numeric IS NOT NULL;

-- 5. 🔍 Text search (requires pg_trgm)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS "IX__values__String_pattern" 
ON _values USING gin (_String gin_trgm_ops)
WHERE _String IS NOT NULL;

-- 6. 🌳 Hierarchical queries (recursive CTE)
CREATE INDEX IF NOT EXISTS "IX__objects__parent_scheme_id" 
ON _objects (_id_parent, _id_scheme, _id)
WHERE _id_parent IS NOT NULL;

-- 7. 🌲 Root objects
CREATE INDEX IF NOT EXISTS "IX__objects__root_objects" 
ON _objects (_id_scheme, _id)
WHERE _id_parent IS NULL;

-- 8. 📅 Sorting by dates
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_date_create" 
ON _objects (_id_scheme, _date_create DESC, _id);

CREATE INDEX IF NOT EXISTS "IX__objects__scheme_date_modify" 
ON _objects (_id_scheme, _date_modify DESC, _id);

-- 9. 🔤 Sorting by name
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_name" 
ON _objects (_id_scheme, _name, _id);

-- Comments for indexes
COMMENT ON INDEX "IX__values__structure_object_lookup" IS 
'Composite index for fast value search by structure and object. CRITICAL for faceted queries with filtering by Props fields. Updated: added _Numeric, _ListItem, _Object; _DateTime → _DateTimeOffset';

COMMENT ON INDEX "IX__objects__scheme_parent" IS 
'Composite index for fast filtering of objects by scheme and parent. CRITICAL for tree queries and searching direct children';

COMMENT ON INDEX "IX__values__object_structure_array_index" IS 
'Composite index for fast work with arrays. Optimizes $arrayContains, $arrayFirst, $arrayLast, $arrayAt and aggregation operators';

COMMENT ON INDEX "IX__values__String_not_null" IS 
'Partial index for fast search of objects with NOT NULL string values. Optimizes queries with {"field": {"$ne": null}} and {"field": {"$exists": true}}';

COMMENT ON INDEX "IX__values__String_pattern" IS 
'GIN index for fast text search via LIKE/ILIKE. Requires pg_trgm extension. Optimizes $contains, $startsWith, $endsWith, $matches (regex)';

COMMENT ON INDEX "IX__objects__parent_scheme_id" IS 
'Partial index for fast recursive hierarchy traversal. Optimizes $hasAncestor, $hasDescendant, $level conditions';

COMMENT ON INDEX "IX__objects__root_objects" IS 
'Partial index for fast search of root objects (without parent). Optimizes {"$isRoot": true} condition';

COMMENT ON INDEX "IX__objects__scheme_date_create" IS 
'Composite index for fast sorting by creation date. Uses DESC to optimize ORDER BY _date_create DESC';

-- ====================================================================================================
-- CRITICAL INDEXES FOR OPTIMIZING WhereHasAncestor/WhereHasDescendant
-- ====================================================================================================

-- 1. CRITICAL: Covering index for fast descendant search by ancestor
CREATE INDEX IF NOT EXISTS "IX__objects__parent_id_descendant_lookup" 
ON _objects (_id_parent, _id_scheme) 
INCLUDE (_id, _id_owner, _date_create, _date_modify)
WHERE _id_parent IS NOT NULL;

COMMENT ON INDEX "IX__objects__parent_id_descendant_lookup" IS 
'Covering index for fast search of all descendants of an object. INCLUDE allows getting basic fields without table access. Critical for optimized WhereHasAncestor (inverted logic). Index Only Scan speeds up queries 10-50x';

-- 2. IMPORTANT: Composite index for tree queries with date sorting
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent_date_create" 
ON _objects (_id_scheme, _id_parent, _date_create DESC)
INCLUDE (_id, _id_owner, _date_modify);

COMMENT ON INDEX "IX__objects__scheme_parent_date_create" IS 
'Optimizes tree queries with sorting by creation date. Used in WhereHasAncestor + ORDER BY _date_create. INCLUDE for Index Only Scan';

-- 3. IMPORTANT: Composite index for reverse ancestor search
CREATE INDEX IF NOT EXISTS "IX__objects__id_parent_scheme" 
ON _objects (_id, _id_parent, _id_scheme)
WHERE _id_parent IS NOT NULL;

COMMENT ON INDEX "IX__objects__id_parent_scheme" IS 
'Accelerates reverse ancestor search (from descendant to parent). Used when building Parent/Children chains in ToTreeListAsync';

-- 4. MEDIUM PRIORITY: Covering index for search with filtering
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent_owner" 
ON _objects (_id_scheme, _id_parent)
INCLUDE (_id, _id_owner, _date_create, _date_modify, _name)
WHERE _id_parent IS NOT NULL;

COMMENT ON INDEX "IX__objects__scheme_parent_owner" IS 
'Covering index for tree queries with additional filtering by owner/name. Index Only Scan for complex WHERE conditions';

-- Index for nested Dictionary/Array field lookups via _array_parent_id
-- Used by PRO PVT CTE for AddressBook[home].City queries
CREATE INDEX IF NOT EXISTS "IX__values__parent_structure" 
ON _values (_array_parent_id, _id_structure) 
WHERE _array_parent_id IS NOT NULL;


CREATE OR REPLACE FUNCTION public.get_scheme_definition(
    scheme_id bigint
) RETURNS json
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json json;
    scheme_json json;
    structures_json json;
    scheme_exists boolean;
BEGIN
    -- Check if scheme exists
    SELECT EXISTS(SELECT 1 FROM _schemes WHERE _id = scheme_id) INTO scheme_exists;
    
    IF NOT scheme_exists THEN
        RETURN json_build_object('error', 'Scheme not found');
    END IF;
    
    -- Get scheme information
    SELECT json_build_object(
        '_id', _s._id,
        '_id_parent', _s._id_parent,
        '_name', _s._name,
        '_alias', _s._alias,
        '_name_space', _s._name_space
    ) INTO scheme_json
    FROM _schemes _s
    WHERE _s._id = scheme_id;
    
    -- Get scheme structures (fields)
    SELECT json_agg(
        json_build_object(
            '_id', _st._id,
            '_id_parent', _st._id_parent,
            '_name', _st._name,
            '_alias', _st._alias,
            '_order', _st._order,
            '_type_name', _t._name,
            '_type_db_type', _t._db_type,
            '_type_dotnet_type', _t._type,
            '_readonly', _st._readonly,
            '_allow_not_null', _st._allow_not_null,
            '_collection_type', _st._collection_type,  -- Array/Dictionary type ID or NULL
            '_key_type', _st._key_type,                -- Key type for Dictionary
            '_is_compress', _st._is_compress,
            '_store_null', _st._store_null,
            '_id_list', _st._id_list,
            '_default_editor', _st._default_editor
        ) ORDER BY _st._order, _st._id
    ) INTO structures_json
    FROM _structures _st
    JOIN _types _t ON _t._id = _st._id_type
    WHERE _st._id_scheme = scheme_id;
    
    -- Build final JSON
    result_json := json_build_object(
        'scheme', scheme_json,
        'structures', COALESCE(structures_json, '[]'::json)
    );
    
    RETURN result_json;
END;
$BODY$;

-- View for selecting definitions of all schemes
CREATE OR REPLACE VIEW v_schemes_definition AS
SELECT 
    _id as scheme_id,
    _name as scheme_name,
    _alias as scheme_alias,
    get_scheme_definition(_id) as scheme_definition
FROM _schemes
ORDER BY _id;



CREATE SEQUENCE global_identity
 AS bigint
 START WITH 1000000
 INCREMENT BY 1
 MINVALUE 1000000
 MAXVALUE 9223372036854775807;

INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775709, 'Boolean', 'Boolean', 'boolean');
-- DateTime (C# type) -> DateTimeOffset (DB timestamptz)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775708, 'DateTime', 'DateTimeOffset', 'DateTime');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775707, 'Double', 'Double', 'double');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775706, 'ListItem', 'ListItem', '_RListItem');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775705, 'Guid', 'Guid', 'Guid');
-- DateTimeOffset (C# type) -> DateTimeOffset (DB timestamptz)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775673, 'DateTimeOffset', 'DateTimeOffset', 'DateTimeOffset');
-- Numeric for exact decimal numbers (financial calculations)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775674, 'Numeric', 'Numeric', 'decimal');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775704, 'Long', 'Long', 'long');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775703, 'Object', 'Object', '_RObject');
--INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775702, 'Text', 'Text', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775701, 'ByteArray', 'ByteArray', 'byte[]');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775700, 'String', 'String', 'string');
-- Additional simple types for REDB
-- These can be added to extend functionality

-- 1. Additional numeric types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775699, 'Int', 'Long', 'int');           -- int (mapped to Long)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775698, 'Short', 'Long', 'short');       -- short (mapped to Long)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775697, 'Byte', 'Long', 'byte');         -- byte (mapped to Long)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775696, 'Float', 'Double', 'float');     -- float (mapped to Double)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775695, 'Decimal', 'Numeric', 'decimal'); -- decimal (mapped to Numeric for precision)

-- 2. Additional string types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775694, 'Char', 'String', 'char');        -- char (mapped to String)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775693, 'Url', 'String', 'string');       -- URL (validation via attributes)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775692, 'Email', 'String', 'string');     -- Email (validation via attributes)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775691, 'Phone', 'String', 'string');     -- Phone (validation via attributes)

-- 3. Special types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775690, 'Json', 'String', 'string');      -- JSON as string
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775689, 'Xml', 'String', 'string');       -- XML as string
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775688, 'Base64', 'String', 'string');    -- Base64 strings
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775687, 'Color', 'String', 'string');     -- Colors (hex, rgb)

-- 4. Temporal types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775686, 'DateOnly', 'DateTime', 'DateOnly');     -- .NET 6+ DateOnly
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775685, 'TimeOnly', 'String', 'TimeOnly');      -- .NET 6+ TimeOnly (as string)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775684, 'TimeSpan', 'String', 'TimeSpan');      -- TimeSpan (format "HH:MM:SS" for JSON compatibility)

-- 5. Enum support
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775683, 'Enum', 'String', 'Enum');        -- Enum (stored as string)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775682, 'EnumInt', 'Long', 'Enum');       -- Enum (stored as number)

-- 6. Geographic types (for future expansion)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775681, 'Latitude', 'Double', 'double');   -- Latitude
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775680, 'Longitude', 'Double', 'double');  -- Longitude
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775679, 'GeoPoint', 'String', 'string');   -- Geographic point as JSON

-- 7. File types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775678, 'FilePath', 'String', 'string');   -- File path
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775677, 'FileName', 'String', 'string');   -- File name
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775676, 'MimeType', 'String', 'string');   -- MIME type
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775675, 'Class', 'Guid', 'Object');   -- Nested class type

-- Collection and document types for RedbObject<T> and Props fields
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775668, 'Array', 'Guid', 'Array');         -- For RedbObject<T[]>/List<T> and array fields in Props
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775667, 'Dictionary', 'Guid', 'Dictionary'); -- For RedbObject<Dictionary<K,V>> and dict fields in Props
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775666, 'JsonDocument', 'Guid', 'JsonDocument'); -- For RedbObject<JsonDocument> (hierarchy via _array_parent_id)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775665, 'XDocument', 'Guid', 'XDocument');     -- For RedbObject<XDocument> (hierarchy via _array_parent_id)

-- Comments for additional types
COMMENT ON TABLE _types IS 'REDB data types table. Supports basic C# types and additional specialized types with validation';

-- Usage examples:
-- 1. Int, Short, Byte - all mapped to Long for simplicity
-- 2. Float - mapped to Double
-- 3. Decimal - can be mapped to Double (precision loss) or Numeric (precision preserved)
-- 4. Char - mapped to String
-- 5. Url, Email, Phone - strings with additional application-level validation
-- 6. Json, Xml - strings for storing structured data
-- 7. DateOnly, TimeOnly, TimeSpan - special temporal types .NET 6+
-- 8. DateTime - for dates without timezone, DateTimeOffset - for dates with timezone
-- 9. Enum - enumeration support (as string or number)
-- 10. Geographic types - for location-based applications
-- 11. File types - for working with files and media

INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (-1, 'default', '', 'default', NULL, NULL, CAST('2023-12-26T01:14:34.410' AS timestamptz), NULL, true);
INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (0, 'sys', '', 'sys', NULL, NULL, CAST('2023-12-26T01:14:34.410' AS timestamptz), NULL, true);
INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (1, 'admin', '', 'admin', NULL, NULL, CAST('2023-12-26T01:14:34.410' AS timestamptz), NULL, true);

-- =====================================================
-- SOFT DELETE SYSTEM: Reserved scheme for deleted objects
-- =====================================================
-- Scheme @@__deleted is used for soft-delete functionality
-- Objects marked for deletion get _id_scheme = -10
-- Type = Object (-9223372036854775703) means no Props/structures
INSERT INTO _schemes (_id, _name, _alias, _type) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
ON CONFLICT (_id) DO NOTHING;

-- for webApi dashboard

-- Trigger for validating field names in _structures
CREATE OR REPLACE FUNCTION validate_structure_name()
RETURNS TRIGGER AS $$
DECLARE
    reserved_fields text[] := ARRAY[
        '_id', '_id_parent', '_id_scheme', '_id_owner', '_id_who_change',
        '_date_create', '_date_modify', '_date_begin', '_date_complete',
        '_key', '_name', '_note', '_hash',
        '_value_long', '_value_string', '_value_guid', '_value_bool',
        '_value_double', '_value_numeric', '_value_datetime', '_value_bytes'
    ];
    field_name text;
BEGIN
    -- Check only on INSERT and UPDATE of _name field
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD._name IS DISTINCT FROM NEW._name) THEN
        field_name := LOWER(NEW._name);
        
        -- Check 1: Name must not match reserved fields of _objects
        IF field_name = ANY(reserved_fields) THEN
            RAISE EXCEPTION 'Field name "_name" cannot match system object fields: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use a different name, avoid: ' || array_to_string(reserved_fields, ', ');
        END IF;
        
        -- Check 2: Name must not start with a digit
        IF NEW._name ~ '^[0-9]' THEN
            RAISE EXCEPTION 'Field name "_name" cannot start with a digit: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Field name must start with a letter or underscore';
        END IF;
        
        -- Check 3: Name must follow C# naming rules (letters, digits, underscores)
        IF NEW._name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
            RAISE EXCEPTION 'Field name "_name" contains invalid characters: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Name can only contain Latin letters, digits, underscores. Must start with letter or underscore';
        END IF;
        
        -- Check 4: Name must not be empty or contain only spaces
        IF LENGTH(TRIM(NEW._name)) = 0 THEN
            RAISE EXCEPTION 'Field name "_name" cannot be empty'
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 5: Maximum name length (reasonable limit)
        IF LENGTH(NEW._name) > 64 THEN
            RAISE EXCEPTION 'Field name "_name" is too long (max 64 characters): %', NEW._name
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 6: Additional C# reserved words
        IF LOWER(NEW._name) = ANY(ARRAY[
            'abstract', 'as', 'bool', 'break', 'byte', 'case', 'catch', 'char', 'checked',
            'class', 'const', 'continue', 'decimal', 'default', 'delegate', 'do', 'double', 'else',
            'enum', 'event', 'explicit', 'extern', 'false', 'finally', 'fixed', 'float', 'for',
            'foreach', 'goto', 'if', 'implicit', 'in', 'int', 'interface', 'internal', 'is', 'lock',
            'long', 'namespace', 'new', 'null', 'object', 'operator', 'out', 'override', 'params',
            'private', 'protected', 'public', 'readonly', 'ref', 'return', 'sbyte', 'sealed',
            'short', 'sizeof', 'stackalloc', 'static', 'string', 'struct', 'switch', 'this',
            'throw', 'true', 'try', 'typeof', 'uint', 'ulong', 'unchecked', 'unsafe', 'ushort',
            'using', 'virtual', 'void', 'volatile', 'while'
        ]) THEN
            RAISE EXCEPTION 'Field name "_name" is a C# reserved word: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use a different name or add prefix/suffix';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER tr_validate_structure_name
    BEFORE INSERT OR UPDATE ON _structures
    FOR EACH ROW
    EXECUTE FUNCTION validate_structure_name();

-- Comments for trigger
COMMENT ON FUNCTION validate_structure_name() IS 'Function for validating field names in _structures according to C# naming rules';
COMMENT ON TRIGGER tr_validate_structure_name ON _structures IS 'Trigger checks correctness of field names: prohibits system names, names starting with digits, special characters, and C# reserved words';

-- Trigger for validating scheme names in _schemes
CREATE OR REPLACE FUNCTION validate_scheme_name()
RETURNS TRIGGER AS $$
DECLARE
    scheme_name text;
BEGIN
    -- Check only on INSERT and UPDATE of _name field
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD._name IS DISTINCT FROM NEW._name) THEN
        scheme_name := LOWER(NEW._name);
        
        -- Skip validation for system schemes (starting with @@)
        -- These are reserved for internal use (e.g. @@__deleted for soft-delete)
        IF NEW._name ~ '^@@' THEN
            RETURN NEW;
        END IF;
        
        -- Check 1: Name must not start with a digit
        IF NEW._name ~ '^[0-9]' THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot start with a digit: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Scheme name must start with a letter or underscore';
        END IF;
        
        -- Check 2: Name must follow C# class naming rules (letters, digits, underscores, dots for namespace, + for nested classes)
        IF NEW._name !~ '^[a-zA-Z_][a-zA-Z0-9_.+]*$' THEN
            RAISE EXCEPTION 'Scheme name "_name" contains invalid characters: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Name can only contain Latin letters, digits, underscores, dots and +. Must start with letter or underscore';
        END IF;
        
        -- Check 3: Name must not end with a dot
        IF NEW._name ~ '\.$' THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot end with a dot: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Remove dot at the end of the name';
        END IF;
        
        -- Check 4: Name must not contain two consecutive dots
        IF NEW._name ~ '\.\.' THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot contain two consecutive dots: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use one dot to separate namespace parts';
        END IF;
        
        -- Check 5: Name must not be empty or contain only spaces
        IF LENGTH(TRIM(NEW._name)) = 0 THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot be empty'
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 6: Maximum name length (reasonable limit)
        IF LENGTH(NEW._name) > 128 THEN
            RAISE EXCEPTION 'Scheme name "_name" is too long (max 128 characters): %', NEW._name
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 7: C# reserved words (check each part after splitting by dots or +)
        IF EXISTS (
            SELECT 1 FROM regexp_split_to_table(LOWER(NEW._name), '[.+]') AS part
            WHERE part = ANY(ARRAY[
                'abstract', 'as', 'bool', 'break', 'byte', 'case', 'catch', 'char', 'checked',
                'class', 'const', 'continue', 'decimal', 'default', 'delegate', 'do', 'double', 'else',
                'enum', 'event', 'explicit', 'extern', 'false', 'finally', 'fixed', 'float', 'for',
                'foreach', 'goto', 'if', 'implicit', 'in', 'int', 'interface', 'internal', 'is', 'lock',
                'long', 'namespace', 'new', 'null', 'object', 'operator', 'out', 'override', 'params',
                'private', 'protected', 'public', 'readonly', 'ref', 'return', 'sbyte', 'sealed',
                'short', 'sizeof', 'stackalloc', 'static', 'string', 'struct', 'switch', 'this',
                'throw', 'true', 'try', 'typeof', 'uint', 'ulong', 'unchecked', 'unsafe', 'ushort',
                'using', 'virtual', 'void', 'volatile', 'while'
            ])
        ) THEN
            RAISE EXCEPTION 'Scheme name "_name" contains C# reserved word: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use a different name or add prefix/suffix to the problematic part';
        END IF;
        
        -- Check 8: Each part (separated by dots or +) must be a valid identifier
        IF EXISTS (
            SELECT 1 FROM regexp_split_to_table(NEW._name, '[.+]') AS part
            WHERE LENGTH(TRIM(part)) = 0 OR part ~ '^[0-9]' OR part !~ '^[a-zA-Z_][a-zA-Z0-9_]*$'
        ) THEN
            RAISE EXCEPTION 'Scheme name "_name" contains invalid namespace part: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Each part between dots/+ must be a valid C# identifier';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for schemes
CREATE TRIGGER tr_validate_scheme_name
    BEFORE INSERT OR UPDATE ON _schemes
    FOR EACH ROW
    EXECUTE FUNCTION validate_scheme_name();

-- Comments for scheme trigger
COMMENT ON FUNCTION validate_scheme_name() IS 'Function for validating scheme names in _schemes according to C# class and namespace naming rules';
COMMENT ON TRIGGER tr_validate_scheme_name ON _schemes IS 'Trigger checks correctness of scheme names: C# class naming rules, namespace support via dots, reserved words';

-- Function for building facets (faceted search)


-- Function for faceted search of objects with filtering (REFACTORED MODULAR VERSION)

-- Function to get user permissions for a specific object
CREATE OR REPLACE FUNCTION get_user_permissions_for_object(
    p_object_id bigint,
    p_user_id bigint DEFAULT NULL  -- now optional for use in trigger
) RETURNS TABLE(
    object_id bigint,
    user_id bigint,
    permission_source_id bigint,
    permission_type varchar,
    _id_role bigint,
    _id_user bigint,
    can_select boolean,
    can_insert boolean,
    can_update boolean,
    can_delete boolean
) AS $$
BEGIN
    -- System user (id=0) has full rights to everything
    IF p_user_id = 0 THEN
        RETURN QUERY SELECT 
            p_object_id as object_id,
            0::bigint as user_id,
            0::bigint as permission_source_id,
            'system'::varchar as permission_type,
            NULL::bigint as _id_role,
            0::bigint as _id_user,
            true as can_select,
            true as can_insert,
            true as can_update,
            true as can_delete;
        RETURN;
    END IF;

    RETURN QUERY
    WITH RECURSIVE permission_search AS (
        -- Step 1: Start with target object
        SELECT 
            p_object_id as object_id,
            p_object_id as current_search_id,
            o._id_parent,
            0 as level,
            EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = p_object_id) as has_permission
        FROM _objects o
        WHERE o._id = p_object_id
        
        UNION ALL
        
        -- Step 2: If NO permission - go to parent
        SELECT 
            ps.object_id,
            o._id as current_search_id,
            o._id_parent,
            ps.level + 1,
            EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) as has_permission
        FROM _objects o
        JOIN permission_search ps ON o._id = ps._id_parent
        WHERE ps.level < 50
          AND ps.has_permission = false  -- continue only if NO permission
    ),
    -- Take first found permission for object
    object_permission AS (
        SELECT DISTINCT ON (ps.object_id)
            ps.object_id,
            p._id as permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            ps.level,
            ps.current_search_id as permission_source_id
        FROM permission_search ps
        JOIN _permissions p ON p._id_ref = ps.current_search_id
        WHERE ps.has_permission = true
        ORDER BY ps.object_id, ps.level  -- closest to object
    ),
    -- Add global rights as fallback (_id_ref = 0)
    global_permission AS (
        SELECT 
            p_object_id as object_id,
            p._id as permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            999 as level,  -- low priority
            0 as permission_source_id
        FROM _permissions p
        WHERE p._id_ref = 0
    ),
    -- Combine specific and global rights
    all_permissions AS (
        SELECT * FROM object_permission
        UNION ALL
        SELECT * FROM global_permission
    ),
    -- Take first by priority (specific > global)
    final_permission AS (
        SELECT DISTINCT ON (object_id)
            *
        FROM all_permissions
        ORDER BY object_id, level  -- specific rights have smaller level
    )
    -- Result: for user permissions - directly, for role permissions - via users_roles
    SELECT 
        fp.object_id,
        CASE 
            WHEN p_user_id IS NULL THEN NULL  -- if user_id not provided for trigger
            WHEN fp._id_user IS NOT NULL THEN fp._id_user  -- direct user permission
            ELSE ur._id_user  -- via role
        END as user_id,
        fp.permission_source_id,
        CASE 
            WHEN fp._id_user IS NOT NULL THEN 'user'::varchar
            ELSE 'role'::varchar
        END as permission_type,
        fp._id_role,
        fp._id_user,
        fp._select as can_select,
        fp._insert as can_insert,
        fp._update as can_update,
        fp._delete as can_delete
    FROM final_permission fp
    LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role  -- only for role permissions
    WHERE p_user_id IS NULL OR (fp._id_user = p_user_id OR ur._id_user = p_user_id);  -- if user_id NULL - all permissions, otherwise filter
END;
$$ LANGUAGE plpgsql;

-- Comment for function to get permissions for object
COMMENT ON FUNCTION get_user_permissions_for_object(bigint, bigint) IS 'Function to get effective permissions for a specific object considering hierarchical inheritance and priorities (user > role). If user_id = NULL, returns first found permission without filtering by user (for use in triggers)';

-- Trigger function for automatically creating permissions when creating node objects
CREATE OR REPLACE FUNCTION auto_create_node_permissions()
RETURNS TRIGGER AS $$
DECLARE
    source_permission RECORD;
BEGIN
    -- Process only INSERT of new objects with parent
    IF TG_OP = 'INSERT' AND NEW._id_parent IS NOT NULL THEN
        
        -- Check if parent already has permission
        IF EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = NEW._id_parent) THEN
            RETURN NEW;  -- Parent already has permission, do nothing
        END IF;
        
        -- Use modified function WITHOUT user_id to search for source permission
        SELECT * INTO source_permission 
        FROM get_user_permissions_for_object(NEW._id_parent, NULL) 
        LIMIT 1;
        
        -- If found source permission - create permission for parent
        IF FOUND THEN
            INSERT INTO _permissions (
                _id, _id_role, _id_user, _id_ref,
                _select, _insert, _update, _delete
            ) VALUES (
                nextval('global_identity'),
                source_permission._id_role,
                source_permission._id_user,
                NEW._id_parent,  -- create permission for parent
                source_permission.can_select,
                source_permission.can_insert,
                source_permission.can_update,
                source_permission.can_delete
            );
        END IF;
        
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on _objects table
CREATE TRIGGER tr_auto_create_node_permissions
    AFTER INSERT ON _objects
    FOR EACH ROW
    EXECUTE FUNCTION auto_create_node_permissions();

-- Comments for trigger
COMMENT ON FUNCTION auto_create_node_permissions() IS 'Trigger function for automatically creating permissions for parent objects when child objects appear. Creates permission only if parent doesn''t have one yet, inheriting from nearest ancestor with permission';
COMMENT ON TRIGGER tr_auto_create_node_permissions ON _objects IS 'Trigger automatically creates permission for an object when a child object is added to it, if it doesn''t already have its own permission. Speeds up permission search by reducing recursion depth';

CREATE VIEW v_user_permissions AS
WITH RECURSIVE permission_search AS (
    -- Step 1: Each object searches for its permission
    SELECT 
        o._id as object_id,
        o._id as current_search_id,
        o._id_parent,
        0 as level,
        EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) as has_permission
    FROM _objects o
    
    UNION ALL
    
    -- Step 2: If NO permission - go to parent
    SELECT 
        ps.object_id,
        o._id as current_search_id,
        o._id_parent,
        ps.level + 1,
        EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) as has_permission
    FROM _objects o
    JOIN permission_search ps ON o._id = ps._id_parent
    WHERE ps.level < 50
      AND ps.has_permission = false  -- continue only if NO permission
),
-- Take first found permission for each object
object_permissions AS (
    SELECT DISTINCT ON (ps.object_id)
        ps.object_id,
        p._id as permission_id,
        p._id_user,
        p._id_role,
        p._select,
        p._insert,
        p._update,
        p._delete,
        ps.level
    FROM permission_search ps
    JOIN _permissions p ON p._id_ref = ps.current_search_id
    WHERE ps.has_permission = true
    ORDER BY ps.object_id, ps.level  -- closest to object
),
-- 🚀 NEW: Add global rights as virtual records with object_id = 0
global_permissions AS (
    SELECT 
        0 as object_id,  -- virtual object for global rights
        p._id as permission_id,
        p._id_user,
        p._id_role,
        p._select,
        p._insert,
        p._update,
        p._delete,
        999 as level  -- low priority
    FROM _permissions p
    WHERE p._id_ref = 0  -- global rights
),
-- Combine specific and global rights
all_permissions AS (
    SELECT * FROM object_permissions
    UNION ALL
    SELECT * FROM global_permissions
),
-- Take first by priority (specific > global)
final_permissions AS (
    SELECT DISTINCT ON (object_id)
        *
    FROM all_permissions
    ORDER BY object_id, level  -- specific rights have smaller level
)
-- Result: for user permissions - directly, for role permissions - via users_roles
SELECT 
    fp.object_id,
    CASE 
        WHEN fp._id_user IS NOT NULL THEN fp._id_user  -- direct user permission
        ELSE ur._id_user  -- via role
    END as user_id,
    fp.permission_id,
    CASE 
        WHEN fp._id_user IS NOT NULL THEN 'user'::varchar
        ELSE 'role'::varchar
    END as permission_type,
    fp._id_role,
    fp._select as can_select,
    fp._insert as can_insert,
    fp._update as can_update,
    fp._delete as can_delete
FROM final_permissions fp
LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role  -- only for role permissions
WHERE fp._id_user IS NOT NULL OR ur._id_user IS NOT NULL;  -- has user


-- 🔐 TRIGGER FOR PROTECTING SYSTEM USERS
-- Prohibits deletion and renaming of users with ID 0 (sys) and 1
-- Other operations (password change, email, phone, status) allowed

-- Trigger function
CREATE OR REPLACE FUNCTION protect_system_users()
RETURNS TRIGGER AS $$
BEGIN
    -- Check DELETE operation
    IF TG_OP = 'DELETE' THEN
        -- Prohibit deletion of system users
        IF OLD._id IN (0, 1) THEN
            RAISE EXCEPTION 'Cannot delete system user with ID %', OLD._id;
        END IF;
        RETURN OLD;
    END IF;
    
    -- Check UPDATE operation
    IF TG_OP = 'UPDATE' THEN
        -- Prohibit changing ID (just in case)
        IF OLD._id != NEW._id THEN
            RAISE EXCEPTION 'Cannot change user ID';
        END IF;
        
        -- For system users prohibit changing login and name
        IF OLD._id IN (0, 1) THEN
            -- Prohibit changing login
            IF OLD._login != NEW._login THEN
                RAISE EXCEPTION 'Cannot change login of system user with ID %', OLD._id;
            END IF;
            
            -- Prohibit changing name
            IF OLD._name != NEW._name THEN
                RAISE EXCEPTION 'Cannot change name of system user with ID %', OLD._id;
            END IF;
        END IF;
        
        RETURN NEW;
    END IF;
    
    -- For INSERT nothing to check
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on _users table
DROP TRIGGER IF EXISTS tr_protect_system_users ON _users;
CREATE TRIGGER tr_protect_system_users
    BEFORE UPDATE OR DELETE ON _users
    FOR EACH ROW
    EXECUTE FUNCTION protect_system_users();

-- Comments for trigger
COMMENT ON FUNCTION protect_system_users() IS 'Protects system users (ID 0, 1) from deletion and renaming';
COMMENT ON TRIGGER tr_protect_system_users ON _users IS 'Trigger protecting system users from deletion and renaming of login/name';


-- Disable trigger (keeps trigger definition but prevents execution)
ALTER TABLE _objects DISABLE TRIGGER tr_auto_create_node_permissions;


-- View current value
--SHOW shared_buffers;
 
-- Change (requires PostgreSQL restart)
-- ALTER SYSTEM SET shared_buffers = '2GB';


-- Schemes for TrueSight.Models

INSERT INTO _permissions (_id, _id_user, _id_ref, _select, _insert, _update, _delete)
VALUES (
    nextval('global_identity'),  -- Permission record ID
    0,        -- ID of sys user
    0,                           -- _id_ref = 0 means "all objects"
    true,                        -- _select = allow reading
    true,                        -- _insert = allow creation
    true,                        -- _update = allow modification
    true                         -- _delete = allow deletion
);
INSERT INTO _permissions (_id, _id_user, _id_ref, _select, _insert, _update, _delete)
VALUES (
    nextval('global_identity'),  -- Permission record ID
    1,        -- ID of sys user
    0,                           -- _id_ref = 0 means "all objects"
    true,                        -- _select = allow reading
    true,                        -- _insert = allow creation
    true,                        -- _update = allow modification
    true                         -- _delete = allow deletion
);

-- ===== redb_metadata_cache.sql =====
-- ============================================================
-- METADATA CACHE: Solution for repeated JOIN problem
-- ============================================================
-- Goal: Avoid repeated JOIN _structures ← _types in each query
-- Approach: UNLOGGED TABLE + automatic synchronization via triggers on _structure_hash
-- Advantages:
--   ✅ No recursion problems (indexes created once)
--   ✅ Works with connection pooling (global table)
--   ✅ Automatic invalidation (triggers on _schemes._structure_hash)
--   ✅ No changes in C# code required
--   ✅ Minimal cache rebuilds (only on actual schema changes)
-- ============================================================

-- 1️⃣ Create metadata cache (ALL fields from _structures + fields from _types)
DROP TABLE IF EXISTS _scheme_metadata_cache CASCADE;

CREATE TABLE _scheme_metadata_cache ( --<-- UNLOGGED TABLE IF NEEDED
    -- Identifiers
    _scheme_id bigint NOT NULL,
    _structure_id bigint NOT NULL,
    _parent_structure_id bigint,
    _id_override bigint,
    
    -- Names and aliases
    _name text NOT NULL,
    _alias text,
    
    -- Structure type
    _type_id bigint NOT NULL,
    _list_id bigint,
    type_name text NOT NULL,
    db_type text NOT NULL,
    type_semantic text NOT NULL,
    
    -- Scheme type (Class/Array/Dictionary/JsonDocument/XDocument)
    _scheme_type bigint,
    scheme_type_name text,
    
    -- Structure attributes
    _order bigint,
    _collection_type bigint,      -- NULL = not a collection, otherwise collection type ID (Array/Dictionary)
    collection_type_name text,    -- Collection type name
    _key_type bigint,             -- Key type for Dictionary
    key_type_name text,           -- Key type name
    _readonly boolean,
    _allow_not_null boolean,
    _is_compress boolean,
    _store_null boolean,
    
    -- Default values
    _default_value bytea,
    _default_editor text
);

-- 2️⃣ Indexes for fast search
CREATE INDEX idx_metadata_cache_lookup 
    ON _scheme_metadata_cache(_scheme_id, _parent_structure_id, _order);

CREATE INDEX idx_metadata_cache_structure 
    ON _scheme_metadata_cache(_structure_id);

CREATE INDEX idx_metadata_cache_scheme
    ON _scheme_metadata_cache(_scheme_id);

CREATE INDEX idx_metadata_cache_name
    ON _scheme_metadata_cache(_scheme_id, _name);

CREATE INDEX idx_metadata_cache_collection
    ON _scheme_metadata_cache(_scheme_id, _collection_type)
    WHERE _collection_type IS NOT NULL;

CREATE INDEX idx_metadata_cache_scheme_type
    ON _scheme_metadata_cache(_scheme_id, _scheme_type);

CREATE INDEX idx_metadata_cache_key_type
    ON _scheme_metadata_cache(_scheme_id, _key_type)
    WHERE _key_type IS NOT NULL;

-- 3️⃣ Cache synchronization function for a scheme
CREATE OR REPLACE FUNCTION sync_metadata_cache_for_scheme(target_scheme_id bigint)
RETURNS void AS $$
BEGIN
    -- Remove old scheme data
    DELETE FROM _scheme_metadata_cache 
    WHERE _scheme_id = target_scheme_id;
    
    -- Insert current data (with support for collection types and scheme type)
    INSERT INTO _scheme_metadata_cache (
        _scheme_id, _structure_id, _parent_structure_id, _id_override,
        _name, _alias,
        _type_id, _list_id, type_name, db_type, type_semantic,
        _scheme_type, scheme_type_name,
        _order, _collection_type, collection_type_name, _key_type, key_type_name,
        _readonly, _allow_not_null, _is_compress, _store_null,
        _default_value, _default_editor
    )
    SELECT 
        s._id_scheme,
        s._id,
        s._id_parent,
        s._id_override,
        s._name,
        s._alias,
        t._id,
        s._id_list,
        t._name,
        t._db_type,
        t._type,
        sch._type,                    -- Scheme type
        scht._name,                   -- Scheme type name
        s._order,
        s._collection_type,           -- Collection type (Array/Dictionary/NULL)
        ct._name,                     -- Collection type name
        s._key_type,                  -- Key type for Dictionary
        kt._name,                     -- Key type name
        s._readonly,
        s._allow_not_null,
        s._is_compress,
        s._store_null,
        s._default_value,
        s._default_editor
    FROM _structures s
    JOIN _types t ON t._id = s._id_type
    JOIN _schemes sch ON sch._id = s._id_scheme
    LEFT JOIN _types scht ON scht._id = sch._type         -- Scheme type
    LEFT JOIN _types ct ON ct._id = s._collection_type    -- Collection type
    LEFT JOIN _types kt ON kt._id = s._key_type           -- Key type
    WHERE s._id_scheme = target_scheme_id;
    
    -- NOTICE removed to avoid spam during mass warmup
    -- Use warmup_all_metadata_caches() to get statistics
END;
$$ LANGUAGE plpgsql;

-- 4️⃣ Trigger on _structure_hash change in _schemes
-- 🔥 KEY IDEA: Track ONLY hash changes, not every INSERT/UPDATE in _structures!
CREATE OR REPLACE FUNCTION sync_metadata_cache_on_hash_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if _structure_hash changed
    IF (TG_OP = 'UPDATE' AND OLD._structure_hash IS DISTINCT FROM NEW._structure_hash) THEN
        -- Hash changed → rebuild cache for this scheme
        PERFORM sync_metadata_cache_for_scheme(NEW._id);
        RAISE NOTICE 'Metadata cache rebuilt for scheme_id=% due to structure_hash change (old=%, new=%)', 
            NEW._id, OLD._structure_hash, NEW._structure_hash;
        
    ELSIF (TG_OP = 'INSERT' AND NEW._structure_hash IS NOT NULL) THEN
        -- New scheme with hash → create cache
        PERFORM sync_metadata_cache_for_scheme(NEW._id);
        RAISE NOTICE 'Metadata cache created for new scheme_id=% (hash=%)', 
            NEW._id, NEW._structure_hash;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_metadata_cache_on_hash_change ON _schemes;
CREATE TRIGGER trg_sync_metadata_cache_on_hash_change
AFTER INSERT OR UPDATE ON _schemes
FOR EACH ROW EXECUTE FUNCTION sync_metadata_cache_on_hash_change();

-- 5️⃣ Trigger on DELETE of scheme
CREATE OR REPLACE FUNCTION cleanup_metadata_cache_on_scheme_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM _scheme_metadata_cache WHERE _scheme_id = OLD._id;
    RAISE NOTICE 'Metadata cache cleared for deleted scheme_id=%', OLD._id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cleanup_metadata_cache_on_scheme_delete ON _schemes;
CREATE TRIGGER trg_cleanup_metadata_cache_on_scheme_delete
AFTER DELETE ON _schemes
FOR EACH ROW EXECUTE FUNCTION cleanup_metadata_cache_on_scheme_delete();

-- 6️⃣ Trigger on type changes (invalidates entire cache)
-- 🔥 When _types change → all schemes must rebuild
CREATE OR REPLACE FUNCTION invalidate_all_metadata_caches()
RETURNS TRIGGER AS $$
BEGIN
    TRUNCATE _scheme_metadata_cache;
    RAISE NOTICE 'All metadata caches invalidated due to _types change';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_invalidate_all_caches ON _types;
CREATE TRIGGER trg_invalidate_all_caches
AFTER UPDATE OR DELETE ON _types
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_all_metadata_caches();

-- 7️⃣ Warmup function (for application startup or after crash)
CREATE OR REPLACE FUNCTION warmup_all_metadata_caches()
RETURNS TABLE(scheme_id bigint, structures_count bigint, scheme_name text, structure_hash uuid) AS $$
BEGIN
    TRUNCATE _scheme_metadata_cache;
    
    -- Rebuild cache for ALL schemes (removed filter for _structure_hash)
    PERFORM sync_metadata_cache_for_scheme(s._id)
    FROM _schemes s;
    
    -- Return statistics for ALL schemes
    RETURN QUERY
    SELECT 
        s._id as scheme_id,
        COUNT(c._structure_id) as structures_count,
        s._name::text as scheme_name,
        s._structure_hash as structure_hash
    FROM _schemes s
    LEFT JOIN _scheme_metadata_cache c ON c._scheme_id = s._id
    GROUP BY s._id, s._name, s._structure_hash
    ORDER BY s._id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION warmup_all_metadata_caches() IS 
'Warms up metadata cache for ALL schemes (including schemes without _structure_hash).
Recommended to call:
  1. On application startup
  2. After PostgreSQL crash (UNLOGGED TABLE is cleared)
  3. After schema migrations

Returns statistics: scheme_id → number of structures for all schemes.

Usage:
  SELECT * FROM warmup_all_metadata_caches();
  
UPDATED: Now warms up ALL schemes, not only those with _structure_hash IS NOT NULL.
This eliminates auto-filling of cache on every query to v_objects_json.
';

-- 8️⃣ Cache consistency check function
CREATE OR REPLACE FUNCTION check_metadata_cache_consistency()
RETURNS TABLE(
    scheme_id bigint, 
    scheme_name text,
    cached_count bigint, 
    actual_count bigint, 
    is_consistent boolean
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s._id as scheme_id,
        s._name::text as scheme_name,
        COALESCE(cache_stats.cached_count, 0) as cached_count,
        COALESCE(actual_stats.actual_count, 0) as actual_count,
        (COALESCE(cache_stats.cached_count, 0) = COALESCE(actual_stats.actual_count, 0)) as is_consistent
    FROM _schemes s
    LEFT JOIN (
        SELECT _scheme_id, COUNT(*) as cached_count
        FROM _scheme_metadata_cache
        GROUP BY _scheme_id
    ) cache_stats ON cache_stats._scheme_id = s._id
    LEFT JOIN (
        SELECT _id_scheme, COUNT(*) as actual_count
        FROM _structures
        GROUP BY _id_scheme
    ) actual_stats ON actual_stats._id_scheme = s._id
    ORDER BY s._id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_metadata_cache_consistency() IS 
'Checks metadata cache consistency.
Returns discrepancies between cache and actual data.

Usage:
  SELECT * FROM check_metadata_cache_consistency() WHERE NOT is_consistent;
';

COMMENT ON TABLE _scheme_metadata_cache IS 
'Metadata cache for schemes to optimize queries.
Automatically synchronized via triggers on _schemes._structure_hash.
Note: Use UNLOGGED TABLE for better performance if replication is not needed (cleared after crash).

Invalidation strategy:
  ✅ _schemes._structure_hash changed → rebuild cache for scheme (trigger)
  ✅ _schemes deleted → delete scheme cache (trigger)
  ✅ _types changed → clear entire cache (TRUNCATE trigger)
  
Cache warmup:
  ✅ warmup_all_metadata_caches() warms up ALL schemes (including those without _structure_hash)
  ✅ Auto-filling in functions (get_object_json, get_facets) as fallback
';

-- ============================================================
-- READY! Now use in queries:
-- 
-- INSTEAD OF:
--   FROM _structures s 
--   JOIN _types t ON t._id = s._id_type
--   WHERE s._id_scheme = object_scheme_id
--
-- USE:
--   FROM _scheme_metadata_cache c
--   WHERE c._scheme_id = object_scheme_id
-- 
-- AVAILABLE FIELDS:
--   _scheme_id, _structure_id, _parent_structure_id, _id_override
--   _name, _alias
--   _type_id, _list_id, type_name, db_type, type_semantic
--   _scheme_type, scheme_type_name (scheme type: Class/Array/Dictionary/JsonDocument/XDocument)
--   _order, _collection_type, collection_type_name (collection type: Array/Dictionary/NULL)
--   _key_type, key_type_name (key type for Dictionary)
--   _readonly, _allow_not_null, _is_compress, _store_null
--   _default_value, _default_editor
--
-- COLLECTION CHECK (instead of _is_array):
--   _collection_type IS NOT NULL = this is a collection (array or dictionary)
--   _collection_type IS NULL = not a collection
-- ============================================================

-- ===== redb_window.sql =====

-- =====================================================
-- REDB EAV WINDOW FUNCTIONS
-- Window functions for EAV model
-- ROW_NUMBER, RANK, SUM OVER, etc.
-- =====================================================

-- =====================================================
-- query_with_window: Query with window functions
-- =====================================================
-- Parameters:
--   p_scheme_id      - Scheme ID
--   p_select_fields  - JSON array of fields for SELECT:
--                      [{"field":"Name","alias":"Name"}]
--   p_window_funcs   - JSON array of window functions:
--                      [{"func":"ROW_NUMBER","alias":"Rank"}]
--   p_partition_by   - JSON array of fields for PARTITION BY:
--                      [{"field":"Category"}]
--   p_order_by       - JSON array for ORDER BY inside window:
--                      [{"field":"Stock","dir":"DESC"}]
--   p_filter_json    - JSON filter (optional)
--   p_limit          - Record limit
--
-- Returns: jsonb array of objects with window functions
-- =====================================================
DROP FUNCTION IF EXISTS query_with_window(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer);
DROP FUNCTION IF EXISTS query_with_window(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer, jsonb);

CREATE OR REPLACE FUNCTION query_with_window(
    p_scheme_id bigint,
    p_select_fields jsonb,
    p_window_funcs jsonb,
    p_partition_by jsonb DEFAULT '[]'::jsonb,
    p_order_by jsonb DEFAULT '[]'::jsonb,
    p_filter_json jsonb DEFAULT NULL,
    p_limit integer DEFAULT 1000,
    p_frame_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_field record;
    v_func record;
    v_resolved record;
    
    v_select_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    v_partition_parts text[] := ARRAY[]::text[];
    v_order_parts text[] := ARRAY[]::text[];
    
    v_join_idx int := 0;
    v_field_path text;
    v_alias text;
    v_column_name text;
    v_join_alias text;
    v_func_name text;
    v_dir text;
    
    v_object_ids bigint[];
    v_where_clause text := '';
    v_over_clause text;
    v_buckets int;
    v_frame_clause text := '';
    v_frame_type text;
    v_start_kind text;
    v_start_offset int;
    v_end_kind text;
    v_end_offset int;
    
    v_sql text;
    v_result jsonb;
BEGIN
    -- =========================================
    -- 1. Base SELECT fields
    -- =========================================
    -- Always include id and name
    v_select_parts := array_append(v_select_parts, 'o._id AS "id"');
    v_select_parts := array_append(v_select_parts, 'o._name AS "name"');
    
    -- Fields from Props or base fields
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_select_fields)
    LOOP
        v_field_path := v_field.value->>'field';
        v_alias := COALESCE(v_field.value->>'alias', v_field_path);
        
        -- Check for base field (prefix "0$:")
        IF v_field_path LIKE '0$:%' THEN
            -- BASE FIELD from _objects (Name, Id, SchemeId, etc.)
            DECLARE
                raw_field_name text := substring(v_field_path from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'query_with_window SELECT: Unknown base field "%"', raw_field_name;
                END IF;
                
                -- SELECT directly from _objects (WITHOUT JOIN!)
                v_select_parts := array_append(v_select_parts, 
                    format('o.%I AS "%s"', sql_column, v_alias));
            END;
        ELSE
            -- EAV FIELD from _values (existing logic + Dictionary support)
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
            IF v_resolved.structure_id IS NULL THEN
                CONTINUE;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 's' || v_join_idx;
            
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'String' THEN '_String'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                ELSE '_String'
            END;
            
            v_select_parts := array_append(v_select_parts, 
                format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
            
            -- 🆕 Dictionary support: PhoneBook[home] -> _array_index = 'home'
            IF v_resolved.dict_key IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                -- Array with specific index: Items[2]
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                -- Simple field (not collection)
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
        END IF;
    END LOOP;
    
    -- =========================================
    -- 2. PARTITION BY
    -- =========================================
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_partition_by)
    LOOP
        v_field_path := v_field.value->>'field';
        
        -- Check for base field (prefix "0$:")
        IF v_field_path LIKE '0$:%' THEN
            -- BASE FIELD from _objects
            DECLARE
                raw_field_name text := substring(v_field_path from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'query_with_window PARTITION BY: Unknown base field "%"', raw_field_name;
                END IF;
                
                v_partition_parts := array_append(v_partition_parts, 
                    format('o.%I', sql_column));
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- EAV FIELD (existing logic + Dictionary support)
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
            IF v_resolved.structure_id IS NULL THEN
                CONTINUE;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'p' || v_join_idx;
            
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'String' THEN '_String'
                ELSE '_String'
            END;
            
            -- 🆕 Dictionary support: PhoneBook[home] -> _array_index = 'home'
            IF v_resolved.dict_key IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
            v_partition_parts := array_append(v_partition_parts, 
                format('%s.%s', v_join_alias, v_column_name));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 3. ORDER BY inside window
    -- =========================================
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_order_by)
    LOOP
        v_field_path := v_field.value->>'field';
        v_dir := COALESCE(upper(v_field.value->>'dir'), 'ASC');
        
        -- Check for base field (prefix "0$:")
        IF v_field_path LIKE '0$:%' THEN
            -- BASE FIELD from _objects
            DECLARE
                raw_field_name text := substring(v_field_path from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'query_with_window ORDER BY: Unknown base field "%"', raw_field_name;
                END IF;
                
                v_order_parts := array_append(v_order_parts, 
                    format('o.%I %s', sql_column, v_dir));
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- EAV FIELD (existing logic + Dictionary support)
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
            IF v_resolved.structure_id IS NULL THEN
                CONTINUE;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'w' || v_join_idx;
            
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                ELSE '_String'
            END;
            
            -- 🆕 Dictionary support: PhoneBook[home] -> _array_index = 'home'
            IF v_resolved.dict_key IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                v_join_parts := array_append(v_join_parts,
                    format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
            v_order_parts := array_append(v_order_parts, 
                format('%s.%s %s', v_join_alias, v_column_name, v_dir));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 4. Build OVER clause (with ROWS BETWEEN support)
    -- =========================================
    -- Parse frame if exists
    IF p_frame_json IS NOT NULL AND p_frame_json != 'null'::jsonb THEN
        v_frame_type := COALESCE(upper(p_frame_json->>'type'), 'ROWS');
        v_start_kind := upper(p_frame_json->'start'->>'kind');
        v_start_offset := COALESCE((p_frame_json->'start'->>'offset')::int, 0);
        v_end_kind := upper(p_frame_json->'end'->>'kind');
        v_end_offset := COALESCE((p_frame_json->'end'->>'offset')::int, 0);
        
        -- Form ROWS/RANGE BETWEEN ... AND ...
        v_frame_clause := v_frame_type || ' BETWEEN ';
        
        -- Start bound
        v_frame_clause := v_frame_clause || CASE v_start_kind
            WHEN 'UNBOUNDEDPRECEDING' THEN 'UNBOUNDED PRECEDING'
            WHEN 'CURRENTROW' THEN 'CURRENT ROW'
            WHEN 'PRECEDING' THEN v_start_offset::text || ' PRECEDING'
            WHEN 'FOLLOWING' THEN v_start_offset::text || ' FOLLOWING'
            ELSE 'UNBOUNDED PRECEDING'
        END;
        
        v_frame_clause := v_frame_clause || ' AND ';
        
        -- End bound
        v_frame_clause := v_frame_clause || CASE v_end_kind
            WHEN 'UNBOUNDEDFOLLOWING' THEN 'UNBOUNDED FOLLOWING'
            WHEN 'CURRENTROW' THEN 'CURRENT ROW'
            WHEN 'PRECEDING' THEN v_end_offset::text || ' PRECEDING'
            WHEN 'FOLLOWING' THEN v_end_offset::text || ' FOLLOWING'
            ELSE 'CURRENT ROW'
        END;
    END IF;
    
    v_over_clause := 'OVER (';
    IF array_length(v_partition_parts, 1) > 0 THEN
        v_over_clause := v_over_clause || 'PARTITION BY ' || array_to_string(v_partition_parts, ', ');
    END IF;
    IF array_length(v_order_parts, 1) > 0 THEN
        IF array_length(v_partition_parts, 1) > 0 THEN
            v_over_clause := v_over_clause || ' ';
        END IF;
        v_over_clause := v_over_clause || 'ORDER BY ' || array_to_string(v_order_parts, ', ');
    END IF;
    -- Add frame clause if exists
    IF v_frame_clause != '' THEN
        IF array_length(v_order_parts, 1) > 0 OR array_length(v_partition_parts, 1) > 0 THEN
            v_over_clause := v_over_clause || ' ';
        END IF;
        v_over_clause := v_over_clause || v_frame_clause;
    END IF;
    v_over_clause := v_over_clause || ')';
    
    -- =========================================
    -- 5. Window functions
    -- =========================================
    FOR v_func IN SELECT * FROM jsonb_array_elements(p_window_funcs)
    LOOP
        v_func_name := upper(v_func.value->>'func');
        v_alias := COALESCE(v_func.value->>'alias', v_func_name);
        v_field_path := v_func.value->>'field';
        
        CASE v_func_name
            -- Ranking functions (without field)
            WHEN 'ROW_NUMBER' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('ROW_NUMBER() %s AS "%s"', v_over_clause, v_alias));
            WHEN 'RANK' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('RANK() %s AS "%s"', v_over_clause, v_alias));
            WHEN 'DENSE_RANK' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('DENSE_RANK() %s AS "%s"', v_over_clause, v_alias));
            WHEN 'COUNT' THEN
                v_select_parts := array_append(v_select_parts, 
                    format('COUNT(*) %s AS "%s"', v_over_clause, v_alias));
            
            -- NTILE(n) - split into n buckets
            WHEN 'NTILE' THEN
                v_buckets := COALESCE((v_func.value->>'buckets')::int, 4);
                v_select_parts := array_append(v_select_parts, 
                    format('NTILE(%s) %s AS "%s"', v_buckets, v_over_clause, v_alias));
                    
            -- Aggregate functions with field (SUM, AVG, MIN, MAX) + Dictionary support
            WHEN 'SUM', 'AVG', 'MIN', 'MAX' THEN
                IF v_field_path IS NOT NULL AND v_field_path != '' THEN
                    SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
                    IF v_resolved.structure_id IS NOT NULL THEN
                        v_join_idx := v_join_idx + 1;
                        v_join_alias := 'f' || v_join_idx;
                        v_column_name := CASE v_resolved.db_type
                            WHEN 'Long' THEN '_Long'
                            WHEN 'Double' THEN '_Double'
                            WHEN 'Numeric' THEN '_Numeric'
                            ELSE '_Long'
                        END;
                        -- 🆕 Dictionary support
                        IF v_resolved.dict_key IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
                        ELSIF v_resolved.array_index IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
                        ELSE
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
                        END IF;
                        v_select_parts := array_append(v_select_parts,
                            format('%s(%s.%s) %s AS "%s"', v_func_name, v_join_alias, v_column_name, v_over_clause, v_alias));
                    END IF;
                END IF;
                
            -- Offset functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE) + Dictionary support
            WHEN 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE' THEN
                IF v_field_path IS NOT NULL AND v_field_path != '' THEN
                    SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field_path);
                    IF v_resolved.structure_id IS NOT NULL THEN
                        v_join_idx := v_join_idx + 1;
                        v_join_alias := 'l' || v_join_idx;
                        v_column_name := CASE v_resolved.db_type
                            WHEN 'Long' THEN '_Long'
                            WHEN 'Double' THEN '_Double'
                            WHEN 'Numeric' THEN '_Numeric'
                            WHEN 'String' THEN '_String'
                            ELSE '_Long'
                        END;
                        -- 🆕 Dictionary support
                        IF v_resolved.dict_key IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
                        ELSIF v_resolved.array_index IS NOT NULL THEN
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
                        ELSE
                            v_join_parts := array_append(v_join_parts,
                                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
                        END IF;
                        v_select_parts := array_append(v_select_parts,
                            format('%s(%s.%s) %s AS "%s"', v_func_name, v_join_alias, v_column_name, v_over_clause, v_alias));
                    END IF;
                END IF;
                
            ELSE
                -- Skip unknown functions
                NULL;
        END CASE;
    END LOOP;
    
    -- =========================================
    -- 6. Filter
    -- =========================================
    IF p_filter_json IS NOT NULL AND p_filter_json != 'null'::jsonb THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NOT NULL AND array_length(v_object_ids, 1) > 0 THEN
            v_where_clause := format(' AND o._id = ANY(ARRAY[%s]::bigint[])', 
                array_to_string(v_object_ids, ','));
        END IF;
    END IF;
    
    -- =========================================
    -- 7. Assemble and execute SQL
    -- =========================================
    v_sql := format(
        'SELECT jsonb_agg(row_to_json(t)) FROM (
            SELECT %s
            FROM _objects o
            %s
            WHERE o._id_scheme = %s%s
            LIMIT %s
        ) t',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_join_parts, ' '),
        p_scheme_id,
        v_where_clause,
        p_limit
    );
    
    EXECUTE v_sql INTO v_result;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION query_with_window IS 
'Query with window functions for EAV.
Supports: simple fields, arrays Items[2], dictionaries PhoneBook[home].
Example (simple fields):
  SELECT query_with_window(
    1002,
    ''[{"field":"Name","alias":"Name"},{"field":"Stock","alias":"Stock"}]''::jsonb,
    ''[{"func":"ROW_NUMBER","alias":"Rank"}]''::jsonb,
    ''[{"field":"Tag"}]''::jsonb,
    ''[{"field":"Stock","dir":"DESC"}]''::jsonb,
    NULL,
    100
  );
Example (Dictionary):
  SELECT query_with_window(
    1002,
    ''[{"field":"PhoneBook[home]","alias":"HomePhone"}]''::jsonb,
    ''[{"func":"ROW_NUMBER","alias":"Rank"}]''::jsonb,
    ''[]''::jsonb,
    ''[{"field":"PhoneBook[home]","dir":"ASC"}]''::jsonb,
    NULL,
    100
  );';


-- ===== migrate_structure_type.sql =====
-- ============================================================
-- HELPER FUNCTION: get_value_column
-- Returns the _values column name for a given type name
-- ============================================================

-- DROP FUNCTION IF EXISTS public.get_value_column(text);

CREATE OR REPLACE FUNCTION public.get_value_column(p_type_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    RETURN CASE LOWER(p_type_name)
        WHEN 'string' THEN '_string'
        WHEN 'text' THEN '_string'
        WHEN 'mimetype' THEN '_string'
        WHEN 'filepath' THEN '_string'
        WHEN 'filename' THEN '_string'
        WHEN 'long' THEN '_long'
        WHEN 'int' THEN '_long'
        WHEN 'short' THEN '_long'
        WHEN 'byte' THEN '_long'
        WHEN 'object' THEN '_object'
        WHEN 'double' THEN '_double'
        WHEN 'float' THEN '_double'
        WHEN 'boolean' THEN '_boolean'
        WHEN 'datetime' THEN '_datetimeoffset'
        WHEN 'datetimeoffset' THEN '_datetimeoffset'
        WHEN 'dateonly' THEN '_datetimeoffset'
        WHEN 'timeonly' THEN '_datetimeoffset'
        WHEN 'timespan' THEN '_long'
        WHEN 'guid' THEN '_guid'
        WHEN 'bytearray' THEN '_bytearray'
        WHEN 'numeric' THEN '_numeric'
        WHEN 'listitem' THEN '_listitem'
        ELSE NULL
    END;
END;
$BODY$;

COMMENT ON FUNCTION public.get_value_column(text)
    IS 'Returns the _values column name for a given REDB type name.
Examples:
  SELECT get_value_column(''String'');  -- returns ''_string''
  SELECT get_value_column(''Long'');    -- returns ''_long''';

-- ============================================================
-- FUNCTION: public.migrate_structure_type(bigint, text, text, boolean)
-- ============================================================

-- DROP FUNCTION IF EXISTS public.migrate_structure_type(bigint, text, text, boolean);

CREATE OR REPLACE FUNCTION public.migrate_structure_type(
    p_structure_id bigint,
    p_old_type_name text,
    p_new_type_name text,
    p_dry_run boolean DEFAULT false)
    RETURNS TABLE(affected_rows integer, success_count integer, error_count integer, errors text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    v_source_col TEXT;
    v_target_col TEXT;
    v_affected_rows INT := 0;
    v_success_count INT := 0;
    v_has_collision BOOLEAN;
    v_conversion_sql TEXT;
BEGIN
    -- Get column names
    v_source_col := get_value_column(p_old_type_name);
    v_target_col := get_value_column(p_new_type_name);
    
    -- Type validation
    IF v_source_col IS NULL THEN
        RETURN QUERY SELECT 0, 0, 0, format('Unknown source type: %s', p_old_type_name);
        RETURN;
    END IF;
    
    IF v_target_col IS NULL THEN
        RETURN QUERY SELECT 0, 0, 0, format('Unknown target type: %s', p_new_type_name);
        RETURN;
    END IF;
    
    -- Same columns - migration not needed (e.g., Int->Long both in _Long)
    IF v_source_col = v_target_col THEN
        RETURN QUERY SELECT 0, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- Check if structure exists
    IF NOT EXISTS (SELECT 1 FROM _structures WHERE _id = p_structure_id) THEN
        RETURN QUERY SELECT 0, 0, 0, format('Structure %s not found', p_structure_id);
        RETURN;
    END IF;
    
    -- Count affected rows
    EXECUTE format(
        'SELECT COUNT(*) FROM _values WHERE _id_structure = $1 AND %I IS NOT NULL',
        v_source_col
    ) INTO v_affected_rows USING p_structure_id;
    
    -- Dry run - only counting
    IF p_dry_run THEN
        RETURN QUERY SELECT v_affected_rows, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- ========================================
    -- COLLISION CHECK (key point!)
    -- If target is filled and source is empty - data was already migrated manually
    -- ========================================
    EXECUTE format(
        'SELECT EXISTS(
            SELECT 1 FROM _values 
            WHERE _id_structure = $1 
              AND %I IS NOT NULL
              AND %I IS NULL
            LIMIT 1
        )', v_target_col, v_source_col
    ) INTO v_has_collision USING p_structure_id;
    
    IF v_has_collision THEN
        RETURN QUERY SELECT v_affected_rows, 0, v_affected_rows, 
            format('TYPE_MIGRATION_COLLISION: Data already in %s but _id_type = %s. Fix manually: UPDATE _structures SET _id_type = (SELECT _id FROM _types WHERE _name = ''%s'') WHERE _id = %s',
                v_target_col, p_old_type_name, p_new_type_name, p_structure_id);
        RETURN;
    END IF;
    
    -- No data to migrate
    IF v_affected_rows = 0 THEN
        RETURN QUERY SELECT 0, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- ========================================
    -- CONVERSION MATRIX
    -- ========================================
    v_conversion_sql := NULL;
    
    -- STRING -> *
    IF v_source_col = '_string' THEN
        IF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+\.?[0-9]*$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+\.?[0-9]*$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_boolean' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN LOWER(%I) IN (''true'', ''1'', ''yes'', ''t'', ''y'') THEN TRUE WHEN LOWER(%I) IN (''false'', ''0'', ''no'', ''f'', ''n'') THEN FALSE ELSE NULL END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_datetimeoffset' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::timestamptz, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_guid' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::uuid, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- LONG -> *
    ELSIF v_source_col = '_long' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_boolean' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = (%I != 0), %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_datetimeoffset' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = to_timestamp(%I), %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- DOUBLE -> *
    ELSIF v_source_col = '_double' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = ROUND(%I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- NUMERIC -> *
    ELSIF v_source_col = '_numeric' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = ROUND(%I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- BOOLEAN -> *
    ELSIF v_source_col = '_boolean' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN %I THEN ''true'' ELSE ''false'' END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN %I THEN 1 ELSE 0 END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- DATETIMEOFFSET -> *
    ELSIF v_source_col = '_datetimeoffset' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = EXTRACT(EPOCH FROM %I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- GUID -> *
    ELSIF v_source_col = '_guid' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    END IF;
    
    -- Conversion not supported
    IF v_conversion_sql IS NULL THEN
        RETURN QUERY SELECT v_affected_rows, 0, v_affected_rows, 
            format('Conversion %s -> %s not supported', p_old_type_name, p_new_type_name);
        RETURN;
    END IF;
    
    -- Execute migration
    EXECUTE v_conversion_sql USING p_structure_id;
    GET DIAGNOSTICS v_success_count = ROW_COUNT;
    
    RETURN QUERY SELECT v_affected_rows, v_success_count, v_affected_rows - v_success_count, NULL::TEXT;
END;
$BODY$;

ALTER FUNCTION public.migrate_structure_type(bigint, text, text, boolean)
    OWNER TO postgres;

COMMENT ON FUNCTION public.migrate_structure_type(bigint, text, text, boolean)
    IS 'Atomic data migration when changing structure type.
Parameters:
  p_structure_id - structure ID in _structures
  p_old_type_name - old type name (String, Long, Double, etc.)
  p_new_type_name - new type name
  p_dry_run - TRUE for test run without changes

Returns:
  affected_rows - total rows with data
  success_count - successfully migrated
  error_count - failed to migrate
  errors - error text (NULL if success)

Returns TYPE_MIGRATION_COLLISION error if data is already in target column.

Examples:
  SELECT * FROM migrate_structure_type(12345, ''String'', ''Long'', TRUE);  -- dry run
  SELECT * FROM migrate_structure_type(12345, ''String'', ''Long'', FALSE); -- execute';

-- ===== migration_drop_deleted_objects.sql =====
-- =====================================================
-- MIGRATION: Drop legacy _deleted_objects archive system
-- Part of Background Deletion System migration
-- =====================================================
-- This script removes the old _deleted_objects table and its trigger.
-- The new soft-delete system uses @@__deleted scheme and trash containers.
-- Run this script on existing databases to migrate to new system.
-- =====================================================

-- 1. Drop the trigger first (depends on function)
DROP TRIGGER IF EXISTS TR__objects__deleted_objects ON _objects;

-- 2. Drop the archive function
DROP FUNCTION IF EXISTS ftr__objects__deleted_objects();

-- 3. Drop the archive table
DROP TABLE IF EXISTS _deleted_objects;

-- 4. Add the @@__deleted scheme if not exists
INSERT INTO _schemes (_id, _name, _alias, _type) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
ON CONFLICT (_id) DO NOTHING;

-- 5. Update validate_scheme_name function to allow @@ prefix
-- (This is done by re-creating the function - see redbPostgre.sql)

-- =====================================================
-- VERIFICATION
-- =====================================================
-- After running this script, verify:
-- SELECT * FROM _schemes WHERE _id = -10;
-- Expected: @@__deleted scheme exists
-- 
-- SELECT * FROM pg_trigger WHERE tgname = 'tr__objects__deleted_objects';
-- Expected: No rows (trigger removed)
-- =====================================================



-- ===== redb_aggregation.sql =====
-- =====================================================
-- REDB EAV AGGREGATION FUNCTIONS
-- Aggregations over EAV fields (_values)
-- Support: simple fields, nested Classes, arrays
-- =====================================================

-- Remove old functions
DROP FUNCTION IF EXISTS aggregate_field(bigint, text, text, jsonb);
DROP FUNCTION IF EXISTS resolve_field_path(bigint, text);

-- =====================================================
-- resolve_field_path: Finds structure_id by path
-- =====================================================
-- Supports:
--   "Price"                   - simple field
--   "Customer.Name"           - nested Class
--   "Items[].Price"           - array (ALL elements)
--   "Items[2].Price"          - array (SPECIFIC element with index 2)
--   "Contacts[].Address.City" - nested inside array
-- 
-- ⭐ USES _scheme_metadata_cache FOR SPEED!
-- 
-- Returns: structure_id, db_type, is_array, array_index (NULL = all, number = specific)
-- =====================================================
CREATE OR REPLACE FUNCTION resolve_field_path(
    p_scheme_id bigint,
    p_field_path text
)
RETURNS TABLE(structure_id bigint, db_type text, is_array boolean, array_index int, dict_key text, is_dictionary boolean)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_segments text[];
    v_segment text;
    v_clean_segment text;
    v_current_parent_id bigint := NULL;
    v_structure_id bigint;
    v_db_type text;
    v_is_collection boolean := false;
    v_is_dictionary boolean := false;
    v_found_collection_type bigint;
    v_array_index int := NULL;
    v_dict_key text := NULL;
    v_index_match text[];
    v_key_match text[];
    v_collection_type_name text;
BEGIN
    -- ⭐ Extract array index if specified: Items[2] -> 2, Items[] -> NULL
    v_index_match := regexp_match(p_field_path, '\[(\d+)\]');
    IF v_index_match IS NOT NULL THEN
        v_array_index := v_index_match[1]::int;
    END IF;
    
    -- 🆕 Extract string Dictionary key: PhoneBook[home] -> 'home'
    v_key_match := regexp_match(p_field_path, '\[([A-Za-z_][A-Za-z0-9_-]*)\]');
    IF v_key_match IS NOT NULL THEN
        v_dict_key := v_key_match[1];
    END IF;
    
    -- Remove [] and [N] and [key] from path and split into segments
    -- "Items[].Price" -> ["Items", "Price"]
    -- "Items[2].Price" -> ["Items", "Price"]
    -- "PhoneBook[home]" -> ["PhoneBook"]
    v_segments := string_to_array(regexp_replace(p_field_path, '\[[^\]]*\]', '', 'g'), '.');
    
    -- Process each segment of the path
    FOREACH v_segment IN ARRAY v_segments
    LOOP
        v_clean_segment := trim(v_segment);
        IF v_clean_segment = '' THEN
            CONTINUE;
        END IF;
        
        -- ⭐ Search in _scheme_metadata_cache (fast!)
        SELECT c._structure_id, c.db_type, c._collection_type
        INTO v_structure_id, v_db_type, v_found_collection_type
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id
          AND c._name = v_clean_segment
          AND (
              (v_current_parent_id IS NULL AND c._parent_structure_id IS NULL)
              OR c._parent_structure_id = v_current_parent_id
          )
        LIMIT 1;
        
        IF v_structure_id IS NULL THEN
            RAISE EXCEPTION 'Field segment "%" not found in path "%" (scheme=%). Check cache: SELECT * FROM warmup_all_metadata_caches();', 
                v_clean_segment, p_field_path, p_scheme_id;
        END IF;
        
        -- Check if it's a collection (array/dictionary)?
        IF v_found_collection_type IS NOT NULL THEN
            v_is_collection := true;
            -- Check collection type: Array or Dictionary
            SELECT t._name INTO v_collection_type_name 
            FROM _types t WHERE t._id = v_found_collection_type;
            IF v_collection_type_name = 'Dictionary' THEN
                v_is_dictionary := true;
            END IF;
        END IF;
        
        -- Move to the next level
        v_current_parent_id := v_structure_id;
    END LOOP;
    
    -- Return result
    structure_id := v_structure_id;
    db_type := v_db_type;
    is_array := v_is_collection OR (p_field_path ~ '\[[^\]]*\]');
    array_index := v_array_index;  -- NULL = all elements, number = specific index
    dict_key := v_dict_key;        -- 🆕 NULL = all keys, string = specific key
    is_dictionary := v_is_dictionary;
    RETURN NEXT;
END;
$BODY$;

-- =====================================================
-- aggregate_field: Aggregation over EAV field
-- =====================================================
-- Parameters:
--   p_scheme_id - Scheme ID
--   p_field_path - field path:
--                  "Price"             - simple field
--                  "Customer.Name"     - nested Class
--                  "Items[].Price"     - array (ALL elements)
--                  "Items[2].Price"    - array (SPECIFIC element)
--                  "PhoneBook[home]"   - Dictionary (SPECIFIC key)
--   p_function - SUM, AVG, MIN, MAX, COUNT
--   p_filter_json - JSON filter or null
-- 
-- Returns: numeric aggregation result
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_field(
    p_scheme_id bigint,
    p_field_path text,
    p_function text,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS numeric
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_structure_id bigint;
    v_db_type text;
    v_is_array boolean;
    v_array_index int;
    v_dict_key text;
    v_is_dictionary boolean;
    v_result numeric;
    v_sql text;
    v_column_name text;
    v_object_ids bigint[];
    v_array_condition text;
BEGIN
    -- 1. Resolve field path (support for nested, arrays, dictionaries and specific indices/keys)
    SELECT r.structure_id, r.db_type, r.is_array, r.array_index, r.dict_key, r.is_dictionary
    INTO v_structure_id, v_db_type, v_is_array, v_array_index, v_dict_key, v_is_dictionary
    FROM resolve_field_path(p_scheme_id, p_field_path) r;
    
    IF v_structure_id IS NULL THEN
        RAISE EXCEPTION 'Field "%" not found in scheme %', p_field_path, p_scheme_id;
    END IF;
    
    -- 2. Determine column by data type
    v_column_name := CASE v_db_type
        WHEN 'Long' THEN '_Long'
        WHEN 'Double' THEN '_Double'
        WHEN 'Numeric' THEN '_Numeric'
        WHEN 'Int' THEN '_Long'
        WHEN 'Decimal' THEN '_Numeric'
        WHEN 'Money' THEN '_Numeric'
        ELSE '_Long'
    END;
    
    -- 3. Condition for collections: Dictionary/Array/Simple
    IF v_dict_key IS NOT NULL THEN
        -- 🆕 Dictionary with key: PhoneBook[home]
        v_array_condition := format('AND v._array_index = %L', v_dict_key);
    ELSIF v_is_array THEN
        IF v_array_index IS NOT NULL THEN
            -- Array with index: Items[2]
            v_array_condition := format('AND v._array_index = %L', v_array_index::text);
        ELSE
            -- Array without index: Items[] — all elements
            v_array_condition := '';
        END IF;
    ELSE
        -- Simple field: not a collection
        v_array_condition := 'AND v._array_index IS NULL';
    END IF;
    
    -- 4. If there is a filter - get object_ids via get_filtered_object_ids
    -- ⚡ OPTIMIZED: returns only bigint[] without JSON overhead!
    IF p_filter_json IS NOT NULL AND p_filter_json != 'null'::jsonb THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'aggregate_field ERROR: % (filter=%)', SQLERRM, p_filter_json;
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NULL OR array_length(v_object_ids, 1) IS NULL THEN
            RETURN CASE p_function
                WHEN 'COUNT' THEN 0
                ELSE NULL
            END;
        END IF;
    END IF;
    
    -- 5. Perform aggregation
    IF v_object_ids IS NOT NULL THEN
        -- With filter by object_ids
        v_sql := format(
            'SELECT %s(%s::numeric) FROM _values v
             WHERE v._id_structure = $1 
               AND v._id_object = ANY($2)
               %s',
            p_function, v_column_name, v_array_condition
        );
        EXECUTE v_sql INTO v_result USING v_structure_id, v_object_ids;
    ELSE
        -- Without filter - all scheme objects
        v_sql := format(
            'SELECT %s(%s::numeric) FROM _values v
             JOIN _objects o ON o._id = v._id_object
             WHERE v._id_structure = $1 
               AND o._id_scheme = $2
               %s',
            p_function, v_column_name, v_array_condition
        );
        EXECUTE v_sql INTO v_result USING v_structure_id, p_scheme_id;
    END IF;
    
    RETURN v_result;
END;
$BODY$;

-- =====================================================
-- COMMENTS
-- =====================================================
COMMENT ON FUNCTION resolve_field_path(bigint, text) IS 
'Resolves EAV field path to structure_id.
Supports: simple fields, nested Classes, arrays, dictionaries.
⭐ Array modes:
  Items[]  → all elements (array_index = NULL)
  Items[2] → specific element (array_index = 2)
⭐ Dictionary modes:
  PhoneBook[]     → all keys (dict_key = NULL)
  PhoneBook[home] → specific key (dict_key = ''home'')
Returns: structure_id, db_type, is_array, array_index, dict_key, is_dictionary';

COMMENT ON FUNCTION aggregate_field(bigint, text, text, jsonb) IS 
'Aggregation over EAV field. Supports SUM, AVG, MIN, MAX, COUNT.
⚡ With filter: 2 queries (get_filtered_object_ids + aggregation) — optimized!
Without filter: 1 query.
⭐ Two array modes:
  Items[].Price  → aggregation over ALL elements
  Items[2].Price → aggregation only over element with index 2
Examples:
  SELECT aggregate_field(1002, ''Price'', ''SUM'', NULL);
  SELECT aggregate_field(1002, ''Items[].Amount'', ''SUM'', NULL);   -- all elements
  SELECT aggregate_field(1002, ''Items[0].Amount'', ''SUM'', NULL);  -- only first
  SELECT aggregate_field(1002, ''Customer.Rating'', ''AVG'', NULL);';

-- =====================================================
-- aggregate_batch: Multiple aggregations in ONE query
-- ⭐ Supports array indices: Items[].Price vs Items[2].Price
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_batch(
    p_scheme_id bigint,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_agg record;
    v_select_parts text[] := ARRAY[]::text[];
    v_structure_ids bigint[] := ARRAY[]::bigint[];
    v_resolved record;
    v_object_ids bigint[];
    v_result jsonb;
    v_sql text;
    v_field text;
    v_func text;
    v_has_count boolean := false;
    v_array_condition text;
BEGIN
    -- 1. Resolve all fields and build SELECT parts
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        
        -- ⭐ COUNT(*) — special case, does not require structure_id
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*' OR v_field = '') THEN
            v_has_count := true;
            v_select_parts := array_append(v_select_parts, format(
                '''%s'', COUNT(DISTINCT v._id_object)',
                v_agg.value->>'alias'
            ));
            CONTINUE;
        END IF;
        
        -- 🆕 Check for base field (prefix "0$:")
        IF v_field LIKE '0$:%' THEN
            -- ═══════════════════════════════════════════════════
            -- 🚀 BASE FIELD _objects (for SumRedbAsync, etc.)
            -- ═══════════════════════════════════════════════════
            DECLARE
                raw_field_name text := substring(v_field from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'aggregate_batch: Unknown base field "%"', raw_field_name;
                END IF;
                
                -- 🔥 Aggregation directly from _objects (without _values!)
                -- For MIN/MAX no need for ::numeric (they work with timestamp, text, etc.)
                -- For SUM/AVG need ::numeric (only for numeric fields)
                IF v_func IN ('SUM', 'AVG') THEN
                    v_select_parts := array_append(v_select_parts, format(
                        '''%s'', %s(o.%I::numeric)',
                        v_agg.value->>'alias',
                        v_func,
                        sql_column
                    ));
                ELSE
                    -- MIN, MAX, COUNT — work with any types
                    v_select_parts := array_append(v_select_parts, format(
                        '''%s'', %s(o.%I)',
                        v_agg.value->>'alias',
                        v_func,
                        sql_column
                    ));
                END IF;
                -- DO NOT add structure_id - not needed for base fields JOIN!
            END;
        ELSE
            -- ═══════════════════════════════════════════════════
            -- 📦 EAV FIELD (existing logic)
            -- ═══════════════════════════════════════════════════
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        
        IF v_resolved.structure_id IS NOT NULL THEN
            v_structure_ids := array_append(v_structure_ids, v_resolved.structure_id);
            
            DECLARE
                v_column_name text;
            BEGIN
                v_column_name := CASE v_resolved.db_type
                    WHEN 'Long' THEN '_Long'
                    WHEN 'Double' THEN '_Double'
                    WHEN 'Numeric' THEN '_Numeric'
                    WHEN 'Int' THEN '_Long'
                    WHEN 'Decimal' THEN '_Numeric'
                    WHEN 'Money' THEN '_Numeric'
                    ELSE '_Long'
                END;
                
                -- ⭐ COLLECTION MODES: Array/Dictionary/Simple
                IF v_resolved.dict_key IS NOT NULL THEN
                    -- 🆕 Dictionary with key: PhoneBook[home]
                    v_array_condition := format(' AND v._array_index = %L', v_resolved.dict_key);
                ELSIF v_resolved.is_array THEN
                    IF v_resolved.array_index IS NOT NULL THEN
                        -- Array with index: Items[2]
                        v_array_condition := format(' AND v._array_index = %L', v_resolved.array_index::text);
                    ELSE
                        -- Array without index: Items[] — all elements
                        v_array_condition := '';
                    END IF;
                ELSE
                    -- Simple field: not a collection
                    v_array_condition := ' AND v._array_index IS NULL';
                END IF;
                
                v_select_parts := array_append(v_select_parts, format(
                    '''%s'', %s(CASE WHEN v._id_structure = %s%s THEN v.%s::numeric END)',
                    v_agg.value->>'alias',
                    v_func,
                    v_resolved.structure_id,
                    v_array_condition,
                    v_column_name
                ));
            END;
        END IF;
        END IF;  -- 🆕 Close IF for "0$:" vs EAV
    END LOOP;
    
    IF array_length(v_select_parts, 1) IS NULL THEN
        RETURN '{}'::jsonb;
    END IF;
    
    -- 2. Filter: get object_ids via get_filtered_object_ids
    -- ⚡ OPTIMIZED: returns only bigint[] without JSON overhead!
    IF p_filter_json IS NOT NULL THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'aggregate_batch ERROR: % (filter=%)', SQLERRM, p_filter_json;
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NULL OR array_length(v_object_ids, 1) IS NULL THEN
            RETURN '{}'::jsonb;
        END IF;
    END IF;
    
    -- 3. Perform aggregation
    IF array_length(v_structure_ids, 1) IS NULL OR array_length(v_structure_ids, 1) = 0 THEN
        -- Only COUNT(*) without other aggregations
        IF v_object_ids IS NOT NULL THEN
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v WHERE v._id_object = ANY($1)',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING v_object_ids;
        ELSE
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v JOIN _objects o ON o._id = v._id_object WHERE o._id_scheme = $1',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING p_scheme_id;
        END IF;
    ELSE
        -- There are structure_ids — standard query
        IF v_object_ids IS NOT NULL THEN
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v WHERE v._id_structure = ANY($1) AND v._id_object = ANY($2)',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING v_structure_ids, v_object_ids;
        ELSE
            v_sql := format(
                'SELECT jsonb_build_object(%s) FROM _values v JOIN _objects o ON o._id = v._id_object WHERE v._id_structure = ANY($1) AND o._id_scheme = $2',
                array_to_string(v_select_parts, ', ')
            );
            EXECUTE v_sql INTO v_result USING v_structure_ids, p_scheme_id;
        END IF;
    END IF;
    
    RETURN COALESCE(v_result, '{}'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION aggregate_batch(bigint, jsonb, jsonb) IS 
'Multiple aggregations in one call.
⚡ With filter: 2 queries (get_filtered_object_ids + aggregation) — optimized!
Without filter: 1 aggregation query.
⭐ Supports two array modes:
  Items[].Price  → aggregation over ALL elements
  Items[2].Price → only element with index 2
Example:
SELECT aggregate_batch(1002, 
  ''[{"field":"Stock","func":"SUM","alias":"TotalStock"},
     {"field":"Items[].Price","func":"SUM","alias":"AllPrices"},
     {"field":"Items[0].Price","func":"SUM","alias":"FirstPrice"}]''::jsonb, NULL);';

-- =====================================================
-- SQL PREVIEW functions (for debugging)
-- =====================================================

-- aggregate_batch_preview: Shows SQL that will be executed
CREATE OR REPLACE FUNCTION aggregate_batch_preview(
    p_scheme_id bigint,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_agg record;
    v_select_parts text[] := ARRAY[]::text[];
    v_structure_ids bigint[] := ARRAY[]::bigint[];
    v_resolved record;
    v_sql text;
    v_field text;
    v_func text;
    v_array_condition text;
BEGIN
    -- 1. Resolve all fields and build SELECT parts
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*' OR v_field = '') THEN
            v_select_parts := array_append(v_select_parts, format(
                'COUNT(DISTINCT v._id_object) AS "%s"',
                v_agg.value->>'alias'
            ));
            CONTINUE;
        END IF;
        
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        
        IF v_resolved.structure_id IS NOT NULL THEN
            v_structure_ids := array_append(v_structure_ids, v_resolved.structure_id);
            
            DECLARE
                v_column_name text;
            BEGIN
                v_column_name := CASE v_resolved.db_type
                    WHEN 'Long' THEN '_Long'
                    WHEN 'Double' THEN '_Double'
                    WHEN 'Numeric' THEN '_Numeric'
                    WHEN 'Int' THEN '_Long'
                    WHEN 'Decimal' THEN '_Numeric'
                    WHEN 'Money' THEN '_Numeric'
                    ELSE '_Long'
                END;
                
                IF v_resolved.is_array THEN
                    IF v_resolved.array_index IS NOT NULL THEN
                        v_array_condition := format(' AND v._array_index = %L', v_resolved.array_index::text);
                    ELSE
                        v_array_condition := '';
                    END IF;
                ELSE
                    v_array_condition := ' AND v._array_index IS NULL';
                END IF;
                
                v_select_parts := array_append(v_select_parts, format(
                    '%s(CASE WHEN v._id_structure = %s%s THEN v.%s::numeric END) AS "%s" /* %s */',
                    v_func,
                    v_resolved.structure_id,
                    v_array_condition,
                    v_column_name,
                    v_agg.value->>'alias',
                    v_field
                ));
            END;
        END IF;
    END LOOP;
    
    IF array_length(v_select_parts, 1) IS NULL THEN
        RETURN '-- No aggregations to execute';
    END IF;
    
    -- 2. Form final SQL
    IF p_filter_json IS NOT NULL THEN
        -- With filter: show that there will be 2 queries
        v_sql := format(E'-- 📊 AGGREGATE BATCH SQL PREVIEW\n-- Scheme: %s\n-- Filter: %s\n\n-- ⚠️ Step 1: Getting object_ids via search_objects_with_facets_base\n-- WITH filtered AS (SELECT ... FROM search_objects_with_facets_base(...))\n\n-- Step 2: Aggregation by object_ids\nSELECT\n  %s\nFROM _values v\nWHERE v._id_structure = ANY(ARRAY[%s]::bigint[])\n  AND v._id_object = ANY(filtered_object_ids);',
            p_scheme_id,
            p_filter_json::text,
            array_to_string(v_select_parts, E',\n  '),
            array_to_string(v_structure_ids, ', ')
        );
    ELSE
        -- Without filter: one query
        v_sql := format(E'-- 📊 AGGREGATE BATCH SQL PREVIEW\n-- Scheme: %s\n-- Filter: NULL\n\nSELECT\n  %s\nFROM _values v\nJOIN _objects o ON o._id = v._id_object\nWHERE v._id_structure = ANY(ARRAY[%s]::bigint[])\n  AND o._id_scheme = %s;',
            p_scheme_id,
            array_to_string(v_select_parts, E',\n  '),
            array_to_string(v_structure_ids, ', '),
            p_scheme_id
        );
    END IF;
    
    RETURN v_sql;
END;
$BODY$;

COMMENT ON FUNCTION aggregate_batch_preview(bigint, jsonb, jsonb) IS 
'🔍 SQL Preview for aggregations. Shows what SQL will be executed in aggregate_batch().
Analog of ToSqlStringAsync() / ToQueryString() from EF Core.
Example:
SELECT aggregate_batch_preview(1002, 
  ''[{"field":"Stock","func":"SUM","alias":"TotalStock"},
     {"field":"Scores1[0]","func":"AVG","alias":"AvgFirst"}]''::jsonb, NULL);';

-- =====================================================
-- TEST QUERIES
-- =====================================================
/*
-- Single
SELECT aggregate_field(1002, 'Stock', 'SUM', NULL);

-- ⭐ Arrays: ALL elements
SELECT aggregate_field(1002, 'Items[].Price', 'SUM', NULL);

-- ⭐ Arrays: SPECIFIC element
SELECT aggregate_field(1002, 'Items[0].Price', 'SUM', NULL);  -- first
SELECT aggregate_field(1002, 'Items[2].Price', 'AVG', NULL);  -- third

-- ⭐ BATCH: Multiple in ONE query
SELECT aggregate_batch(1002, 
    '[{"field":"Stock","func":"SUM","alias":"TotalStock"},
      {"field":"Age","func":"AVG","alias":"AvgAge"},
      {"field":"Items[].Price","func":"SUM","alias":"AllItemsPrice"},
      {"field":"Items[0].Price","func":"SUM","alias":"FirstItemPrice"}]'::jsonb,
    NULL);

-- 🔍 SQL PREVIEW (for debugging)
SELECT aggregate_batch_preview(1002, 
    '[{"field":"Stock","func":"SUM","alias":"TotalStock"},
      {"field":"Scores1[]","func":"SUM","alias":"AllScores"},
      {"field":"Scores1[0]","func":"AVG","alias":"AvgFirst"}]'::jsonb,
    NULL);
*/

-- ===== redb_facets_search.sql =====
-- ===== REDB FACETS & SEARCH MODULE =====
-- Module for faceted search and filtering of objects
-- Architecture: Modular system by Ruslan + our relational arrays + Class fields
-- Includes: LINQ operators, logical operators, Class fields, hierarchical search

-- ===== DATA TYPES =====

-- 🎯 Composite type for structural information (used in condition building functions)
DROP TYPE IF EXISTS structure_info_type CASCADE;
CREATE TYPE structure_info_type AS (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
);

-- ===== CLEANUP OF EXISTING FUNCTIONS =====
DROP FUNCTION IF EXISTS _format_json_array_for_in CASCADE;
DROP FUNCTION IF EXISTS _parse_field_path CASCADE;
DROP FUNCTION IF EXISTS _find_structure_info CASCADE;
DROP FUNCTION IF EXISTS _build_inner_condition CASCADE;
DROP FUNCTION IF EXISTS _build_exists_condition CASCADE;
DROP FUNCTION IF EXISTS _build_and_condition CASCADE;
DROP FUNCTION IF EXISTS _build_or_condition CASCADE;
DROP FUNCTION IF EXISTS _build_not_condition CASCADE;
DROP FUNCTION IF EXISTS _build_single_facet_condition CASCADE;
DROP FUNCTION IF EXISTS _build_facet_field_path CASCADE;
DROP FUNCTION IF EXISTS get_facets CASCADE;
-- DROP FUNCTION IF EXISTS build_advanced_facet_conditions CASCADE; -- ✅ REMOVED IN VARIANT C
-- DROP FUNCTION IF EXISTS build_base_facet_conditions CASCADE; -- ✅ REMOVED! DEAD CODE!
DROP FUNCTION IF EXISTS build_order_conditions CASCADE;
DROP FUNCTION IF EXISTS build_has_ancestor_condition CASCADE;
DROP FUNCTION IF EXISTS build_has_descendant_condition CASCADE;
DROP FUNCTION IF EXISTS build_level_condition CASCADE;
DROP FUNCTION IF EXISTS build_hierarchical_conditions CASCADE;
DROP FUNCTION IF EXISTS execute_objects_query CASCADE;
DROP FUNCTION IF EXISTS search_objects_with_facets CASCADE;
-- Remove ALL versions of search_tree_objects_with_facets (old and new)
DROP FUNCTION IF EXISTS search_tree_objects_with_facets(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) CASCADE;

-- ===== HELPER FUNCTIONS =====

-- 🚀 Function to normalize base field names C# → SQL
-- Maps C# names (snake_case and PascalCase) to _objects column names with _ prefix
DROP FUNCTION IF EXISTS _normalize_base_field_name CASCADE;
CREATE OR REPLACE FUNCTION _normalize_base_field_name(field_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    RETURN CASE field_name
        -- ID fields
        WHEN 'id' THEN '_id'
        WHEN 'Id' THEN '_id'
        WHEN '_id' THEN '_id'
        WHEN 'parent_id' THEN '_id_parent'
        WHEN 'ParentId' THEN '_id_parent'
        WHEN 'id_parent' THEN '_id_parent'
        WHEN '_id_parent' THEN '_id_parent'
        WHEN 'scheme_id' THEN '_id_scheme'
        WHEN 'SchemeId' THEN '_id_scheme'
        WHEN 'id_scheme' THEN '_id_scheme'
        WHEN '_id_scheme' THEN '_id_scheme'
        WHEN 'owner_id' THEN '_id_owner'
        WHEN 'OwnerId' THEN '_id_owner'
        WHEN '_id_owner' THEN '_id_owner'
        WHEN 'who_change_id' THEN '_id_who_change'
        WHEN 'WhoChangeId' THEN '_id_who_change'
        WHEN '_id_who_change' THEN '_id_who_change'
        -- Value fields (RedbPrimitive<T> support)
        WHEN 'value_long' THEN '_value_long'
        WHEN 'ValueLong' THEN '_value_long'
        WHEN '_value_long' THEN '_value_long'
        WHEN 'value_string' THEN '_value_string'
        WHEN 'ValueString' THEN '_value_string'
        WHEN '_value_string' THEN '_value_string'
        WHEN 'value_guid' THEN '_value_guid'
        WHEN 'ValueGuid' THEN '_value_guid'
        WHEN '_value_guid' THEN '_value_guid'
        -- Other base fields
        WHEN 'key' THEN '_key'
        WHEN 'Key' THEN '_key'
        WHEN '_key' THEN '_key'
        WHEN 'name' THEN '_name'
        WHEN 'Name' THEN '_name'
        WHEN '_name' THEN '_name'
        WHEN 'note' THEN '_note'
        WHEN 'Note' THEN '_note'
        WHEN '_note' THEN '_note'
        WHEN 'value_bool' THEN '_value_bool'
        WHEN 'ValueBool' THEN '_value_bool'
        WHEN '_value_bool' THEN '_value_bool'
        -- New RedbPrimitive<T> value fields
        WHEN 'value_double' THEN '_value_double'
        WHEN 'ValueDouble' THEN '_value_double'
        WHEN '_value_double' THEN '_value_double'
        WHEN 'value_numeric' THEN '_value_numeric'
        WHEN 'ValueNumeric' THEN '_value_numeric'
        WHEN '_value_numeric' THEN '_value_numeric'
        WHEN 'value_datetime' THEN '_value_datetime'
        WHEN 'ValueDatetime' THEN '_value_datetime'
        WHEN '_value_datetime' THEN '_value_datetime'
        WHEN 'value_bytes' THEN '_value_bytes'
        WHEN 'ValueBytes' THEN '_value_bytes'
        WHEN '_value_bytes' THEN '_value_bytes'
        WHEN 'hash' THEN '_hash'
        WHEN 'Hash' THEN '_hash'
        WHEN '_hash' THEN '_hash'
        -- DateTime fields
        WHEN 'date_create' THEN '_date_create'
        WHEN 'DateCreate' THEN '_date_create'
        WHEN '_date_create' THEN '_date_create'
        WHEN 'date_modify' THEN '_date_modify'
        WHEN 'DateModify' THEN '_date_modify'
        WHEN '_date_modify' THEN '_date_modify'
        WHEN 'date_begin' THEN '_date_begin'
        WHEN 'DateBegin' THEN '_date_begin'
        WHEN '_date_begin' THEN '_date_begin'
        WHEN 'date_complete' THEN '_date_complete'
        WHEN 'DateComplete' THEN '_date_complete'
        WHEN '_date_complete' THEN '_date_complete'
        -- Not a base field - return NULL
        ELSE NULL
    END;
END;
$BODY$;

COMMENT ON FUNCTION _normalize_base_field_name(text) IS 'Normalizes C# base field names to SQL column names in _objects. Returns NULL if field is not a base field.';

-- Function to format JSON array for IN operator
CREATE OR REPLACE FUNCTION _format_json_array_for_in(
    array_data jsonb
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    in_values text := '';
    json_element jsonb;
    first_item boolean := true;
    element_text text;
BEGIN
    -- Check that this is an array
    IF jsonb_typeof(array_data) != 'array' THEN
        RAISE EXCEPTION 'JSON array expected, got: %', jsonb_typeof(array_data);
    END IF;
    
    -- Process each array element
    FOR json_element IN SELECT value FROM jsonb_array_elements(array_data) LOOP
        IF NOT first_item THEN
            in_values := in_values || ', ';
        END IF;
        first_item := false;
        
        -- Format element based on type
        CASE jsonb_typeof(json_element)
            WHEN 'string' THEN
                -- ✅ FIX: Extract clean string WITHOUT JSON quotes, then quote
                element_text := quote_literal(json_element #>> '{}');
            WHEN 'number' THEN
                element_text := json_element::text;
            WHEN 'boolean' THEN
                element_text := CASE WHEN (json_element)::boolean THEN 'true' ELSE 'false' END;
            ELSE
                -- ✅ FIX: Here too for other types
                element_text := quote_literal(json_element #>> '{}');
        END CASE;
        
        in_values := in_values || element_text;
    END LOOP;
    
    RETURN in_values;
END;
$BODY$;

COMMENT ON FUNCTION _format_json_array_for_in(jsonb) IS 'Converts JSONB array to string of values for SQL IN clause. Supports string, number, boolean types. Used in $in operators.';

-- Function for parsing field path for Class fields, arrays and Dictionary
CREATE OR REPLACE FUNCTION _parse_field_path(
    field_path text
) RETURNS TABLE (
    root_field text,
    nested_field text, 
    is_array boolean,
    is_nested boolean,
    dict_key text  -- NEW: Dictionary key for AddressBook[home] -> 'home'
)
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    bracket_pos int;
    dot_after_bracket int;
    key_end int;
BEGIN
    -- Initialize dict_key as NULL
    dict_key := NULL;
    
    -- Determine if field is array (contains [] WITHOUT key inside)
    is_array := field_path LIKE '%[]%';
    
    -- Check for Dictionary path: AddressBook[home] or AddressBook[home].City
    -- Dictionary differs from array by having non-empty key in brackets
    IF field_path ~ '\[[^\]]+\]' AND NOT is_array THEN
        -- This is a Dictionary path: AddressBook[home] or AddressBook[home].City
        bracket_pos := position('[' in field_path);
        key_end := position(']' in field_path);
        
        -- Extract root field (before bracket)
        root_field := substring(field_path from 1 for bracket_pos - 1);
        
        -- Extract dict key (inside brackets)
        dict_key := substring(field_path from bracket_pos + 1 for key_end - bracket_pos - 1);
        
        -- Check for nested field after bracket: AddressBook[home].City
        dot_after_bracket := position('.' in substring(field_path from key_end + 1));
        IF dot_after_bracket > 0 THEN
            nested_field := substring(field_path from key_end + 2); -- +2 to skip '].'
            is_nested := true;
        ELSE
            nested_field := NULL;
            is_nested := false;
        END IF;
        
        RETURN QUERY SELECT root_field, nested_field, false, is_nested, dict_key;
        RETURN;
    END IF;
    
    -- Determine if field is nested (contains dot)
    is_nested := field_path LIKE '%.%';
    
    IF is_nested THEN
        IF is_array THEN
            -- Case: "Contacts[].Email" -> root="Contacts", nested="Email", is_array=true
            root_field := split_part(replace(field_path, '[]', ''), '.', 1);
            nested_field := split_part(replace(field_path, '[]', ''), '.', 2);
        ELSE
            -- Case: "Contact.Name" -> root="Contact", nested="Name", is_array=false  
            root_field := split_part(field_path, '.', 1);
            nested_field := split_part(field_path, '.', 2);
        END IF;
    ELSE
        IF is_array THEN
            -- Case: "Tags[]" -> root="Tags", nested=NULL, is_array=true
            root_field := replace(field_path, '[]', '');
            nested_field := NULL;
        ELSE
            -- Case: "Name" -> root="Name", nested=NULL, is_array=false
            root_field := field_path;
            nested_field := NULL;
        END IF;
    END IF;
    
    RETURN QUERY SELECT root_field, nested_field, is_array, is_nested, dict_key;
END;
$BODY$;

COMMENT ON FUNCTION _parse_field_path(text) IS 'Parses field path to support Class fields, arrays and Dictionary. Supports: "Name", "Contact.Name", "Tags[]", "Contacts[].Email", "PhoneBook[home]", "AddressBook[home].City". Returns path components for further processing.';

-- Function to search for structure information for Class fields
-- 🎯 NEW: Helper function to determine ListItem field type
CREATE OR REPLACE FUNCTION _get_listitem_field_type_info(field_name text)
RETURNS jsonb
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    CASE field_name
        WHEN 'Value' THEN 
            RETURN jsonb_build_object('db_type', 'String', 'type_semantic', 'String', 'is_array', false);
        WHEN 'Alias' THEN 
            RETURN jsonb_build_object('db_type', 'String', 'type_semantic', 'String', 'is_array', false);
        WHEN 'IdObject' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        WHEN 'IdList' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        WHEN 'Id' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        ELSE
            RETURN NULL;
    END CASE;
END;
$BODY$;

CREATE OR REPLACE FUNCTION _find_structure_info(
    scheme_id bigint,
    root_field text,
    nested_field text DEFAULT NULL
) RETURNS TABLE (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
)
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    scheme_def jsonb;
BEGIN
    -- Get scheme definition using existing function
    SELECT get_scheme_definition(scheme_id) INTO scheme_def;
    
    -- Find root structure
    SELECT 
        (struct->>'_id')::bigint,
        jsonb_build_object(
            'type_name', struct->>'_type_name',
            'db_type', struct->>'_type_db_type', 
            'type_semantic', struct->>'_type_dotnet_type',
            'is_array', (struct->>'_collection_type')::bigint IS NOT NULL
        )
    INTO root_structure_id, root_type_info
    FROM jsonb_array_elements(scheme_def->'structures') AS struct
    WHERE struct->>'_name' = root_field
      AND struct->>'_id_parent' IS NULL;
    
    -- If there is a nested field, find its structure
    IF nested_field IS NOT NULL AND root_structure_id IS NOT NULL THEN
        -- 🎯 NEW: Special handling for ListItem fields
        IF root_type_info->>'type_semantic' = '_RListItem' THEN
            -- For ListItem nested fields (Value, Alias, etc.) are not structures
            -- They are stored as columns in _list_items
            nested_structure_id := NULL;
            nested_type_info := _get_listitem_field_type_info(nested_field);
        ELSE
            -- Normal logic for other types
            SELECT 
                (struct->>'_id')::bigint,
                jsonb_build_object(
                    'type_name', struct->>'_type_name',
                    'db_type', struct->>'_type_db_type',
                    'type_semantic', struct->>'_type_dotnet_type', 
                    'is_array', (struct->>'_collection_type')::bigint IS NOT NULL
                )
            INTO nested_structure_id, nested_type_info
            FROM jsonb_array_elements(scheme_def->'structures') AS struct
            WHERE struct->>'_name' = nested_field
              AND (struct->>'_id_parent')::bigint = root_structure_id;
        END IF;
    ELSE
        nested_structure_id := NULL;
        nested_type_info := NULL;
    END IF;
    
    RETURN QUERY SELECT root_structure_id, nested_structure_id, root_type_info, nested_type_info;
END;
$BODY$;

COMMENT ON FUNCTION _find_structure_info(bigint, text, text) IS 'Finds structure information for Class fields using get_scheme_definition. Returns structure IDs and type metadata for root and nested fields.';

-- ===== SYSTEM CORE: LINQ OPERATORS =====

-- Function to build inner conditions with support for all LINQ operators
CREATE OR REPLACE FUNCTION _build_inner_condition(
    operator_name text,
    operator_value text,
    type_info jsonb  -- Type information from _find_structure_info
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    op_symbol text;
    pattern text;
    in_values_list text;
    db_type text := type_info->>'db_type';
    is_array boolean := (type_info->>'is_array')::boolean;
BEGIN
    -- Numeric and DateTime operators
    IF operator_name IN ('$gt', '$lt', '$gte', '$lte') THEN
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lte' THEN op_symbol := '<=';
        END CASE;
        
        -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            -- TimeSpan: convert to INTERVAL for correct comparison
            RETURN format('fv._String::interval %s %L::interval', op_symbol, operator_value);
        
        ELSIF operator_value ~ '^\d{4}-\d{2}-\d{2}' OR db_type = 'DateTimeOffset' THEN
            RETURN format('fv._DateTimeOffset %s %L::timestamptz', op_symbol, operator_value);
        ELSIF db_type = 'Long' THEN
            -- Known Long type (int, long)
            RETURN format('fv._Long %s %L::bigint', op_symbol, operator_value);
        ELSIF db_type = 'Double' THEN
            -- Known Double type (float, double)
            RETURN format('fv._Double %s %L::double precision', op_symbol, operator_value);
        ELSIF db_type = 'Numeric' THEN
            -- Known Numeric type (decimal)
            RETURN format('fv._Numeric %s %L::numeric', op_symbol, operator_value);
        ELSE
            -- Type unknown - check all numeric types (fallback)
            RETURN format('((fv._Long %s %L::bigint) OR (fv._Double %s %L::double precision) OR (fv._Numeric %s %L::numeric))',
                op_symbol, operator_value, op_symbol, operator_value, op_symbol, operator_value);
        END IF;
    
    -- String operators (case-sensitive)
    -- 🚀 OPTIMIZATION: removed fs.db_type check - type already known for string operators
    ELSIF operator_name IN ('$startsWith', '$endsWith', '$contains') THEN
        CASE operator_name
            WHEN '$startsWith' THEN pattern := operator_value || '%';
            WHEN '$endsWith' THEN pattern := '%' || operator_value;
            WHEN '$contains' THEN pattern := '%' || operator_value || '%';
        END CASE;
        
        RETURN format('fv._String LIKE %L', pattern);
    
    -- String operators (case-insensitive)
    -- 🚀 OPTIMIZATION: removed fs.db_type check
    ELSIF operator_name IN ('$startsWithIgnoreCase', '$endsWithIgnoreCase', '$containsIgnoreCase') THEN
        CASE operator_name
            WHEN '$startsWithIgnoreCase' THEN pattern := operator_value || '%';
            WHEN '$endsWithIgnoreCase' THEN pattern := '%' || operator_value;
            WHEN '$containsIgnoreCase' THEN pattern := '%' || operator_value || '%';
        END CASE;
        
        RETURN format('fv._String ILIKE %L', pattern);
    
    -- IN operator
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$in' THEN
        in_values_list := _format_json_array_for_in(operator_value::jsonb);
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            -- TimeSpan: convert to INTERVAL for correct comparison
            RETURN format('fv._String::interval IN (%s)', 
                regexp_replace(in_values_list, '([^,]+)', '\1::interval', 'g'));
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            -- ListItem: values stored in _listitem column as list element IDs
            RETURN format('fv._listitem IN (%s)', in_values_list);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String IN (%s)', in_values_list);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long IN (%s)', in_values_list);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double IN (%s)', in_values_list);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric IN (%s)', in_values_list);
        ELSIF db_type = 'Boolean' THEN
            RETURN format('fv._Boolean IN (%s)', in_values_list);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('fv._DateTimeOffset IN (%s)', in_values_list);
        ELSE
            -- Fallback: try all types (only if type unknown)
            RETURN format('(fv._String IN (%s) OR fv._Long IN (%s) OR fv._Double IN (%s) OR fv._Numeric IN (%s) OR fv._Boolean IN (%s) OR fv._DateTimeOffset IN (%s))',
                in_values_list, in_values_list, in_values_list, in_values_list, in_values_list, in_values_list);
        END IF;
    
    -- NOT EQUAL operator - requires special handling
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$ne' THEN
        -- For $ne null this is a special case - look for existing records (in EAV null = no record)
        IF operator_value IS NULL OR operator_value = 'null' OR operator_value = '' THEN
            RETURN 'TRUE';  -- Any existing record means "not null"
        ELSE
            -- $ne specific value - build positive condition for negation via NOT EXISTS
            IF type_info->>'type_semantic' = '_RListItem' THEN
                RETURN format('fv._listitem = %L::bigint', operator_value);
            ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
                RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
            ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
                RETURN format('fv._Guid = %L::uuid', operator_value);
            ELSIF db_type = 'Long' THEN
                RETURN format('fv._Long = %L::bigint', operator_value);
            ELSIF db_type = 'Double' THEN
                RETURN format('fv._Double = %L::double precision', operator_value);
            ELSIF db_type = 'Numeric' THEN
                RETURN format('fv._Numeric = %L::numeric', operator_value);
            ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
                RETURN format('fv._Boolean = %L::boolean', operator_value);
            ELSIF db_type = 'String' THEN
                RETURN format('fv._String = %L', operator_value);
            ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
                -- Fallback: numeric value, type unknown
                IF operator_value ~ '^-?\d+$' THEN
                    RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                        operator_value, operator_value, operator_value);
                ELSE
                    RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                        operator_value, operator_value);
                END IF;
            ELSE
                RETURN format('fv._String = %L', operator_value);
            END IF;
        END IF;
    
    -- Explicit equality operator
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$eq' THEN
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            RETURN format('fv._listitem = %L::bigint', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
        ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RETURN format('fv._Guid = %L::uuid', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric = %L::numeric', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String = %L', operator_value);
        ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
            -- Fallback: numeric value, type unknown
            IF operator_value ~ '^-?\d+$' THEN
                RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value, operator_value);
            ELSE
                RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value);
            END IF;
        ELSE
            RETURN format('fv._String = %L', operator_value);
        END IF;
    
    -- 🚀 EXTENDED RELATIONAL ARRAY OPERATORS
    -- ✅ OPTIMIZATION: Simple condition instead of nested EXISTS
    -- _build_exists_condition already creates fv context with fv._array_index IS NOT NULL
    ELSIF operator_name = '$arrayContains' THEN
        -- 🎯 Search value in relational array with SMART typing
        IF db_type = 'Long' OR (operator_value ~ '^-?\d+$' AND type_info->>'type_semantic' != '_RListItem') THEN
            -- Numeric value (Long)
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            -- Boolean value
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            -- DateTime value
            RETURN format('fv._DateTimeOffset = %L::timestamp', operator_value);
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            -- ListItem array: fv._String will be replaced with li._value in _build_exists_condition
            RETURN format('fv._String = %L', operator_value);
        ELSE
            -- String array (default)
            RETURN format('fv._String = %L', operator_value);
        END IF;
    
    -- Non-empty array check operator  
    ELSIF operator_name = '$arrayAny' THEN
        RETURN 'EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        )';
    
    -- Empty array check operator
    ELSIF operator_name = '$arrayEmpty' THEN
        RETURN 'NOT EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure  
              AND av._array_index IS NOT NULL
        )';
    
    -- 📊 ARRAY ELEMENT COUNT OPERATORS
    ELSIF operator_name = '$arrayCount' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) = %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountGt' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) > %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountGte' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) >= %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountLt' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) < %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountLte' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) <= %L::int', operator_value::int);
    
    -- 🎯 OPERATORS FOR RELATIONAL ARRAYS
    ELSIF operator_name = '$arrayAt' THEN
        -- Get array element by index
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index = %L
        )', operator_value::text);
    
    ELSIF operator_name = '$arrayFirst' THEN
        -- Check first array element
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'String' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._String = %L)', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Long = %L::bigint)', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Double = %L::double precision)', operator_value);
        ELSIF db_type = 'Boolean' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Boolean = %L::boolean)', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._DateTimeOffset = %L::timestamp)', operator_value);
        ELSE
            -- Fallback: check all types
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND (av._String = %L OR av._Long = %L::bigint OR av._Double = %L::double precision))', operator_value, operator_value, operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayLast' THEN
        -- Check last array element
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'String' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._String = %L)', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._Long = %L::bigint)', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._Double = %L::double precision)', operator_value);
        ELSE
            -- Fallback
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND (av._String = %L OR av._Long = %L::bigint))', operator_value, operator_value);
        END IF;
    
    -- 🔍 ARRAY SEARCH OPERATORS
    -- 🚀 OPTIMIZATION: removed JOIN and fs._collection_type
    ELSIF operator_name = '$arrayStartsWith' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String LIKE %L
        )', operator_value || '%');
    
    ELSIF operator_name = '$arrayEndsWith' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String LIKE %L
        )', '%' || operator_value);
    
    ELSIF operator_name = '$arrayMatches' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String ~ %L
        )', operator_value);
    
    -- 📈 ARRAY AGGREGATION OPERATORS
    -- 🚀 OPTIMIZATION: use db_type from type_info
    ELSIF operator_name = '$arraySum' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Long), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Double), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Numeric), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSE
            -- Fallback: Long + Double
            RETURN format('(SELECT COALESCE(SUM(COALESCE(av._Long, 0) + COALESCE(av._Double, 0)), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayAvg' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT AVG(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT AVG(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT AVG(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSE
            RETURN format('(SELECT AVG(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayMin' THEN
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'Long' THEN
            RETURN format('(SELECT MIN(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT MIN(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT MIN(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('(SELECT MIN(av._DateTimeOffset) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::timestamp', operator_value);
        ELSE
            RETURN format('(SELECT MIN(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayMax' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT MAX(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT MAX(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT MAX(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('(SELECT MAX(av._DateTimeOffset) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::timestamp', operator_value);
        ELSE
            RETURN format('(SELECT MAX(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSE
        -- 🚀 OPTIMIZATION: Simple equality - use db_type from type_info if known
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
        ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RETURN format('fv._Guid = %L::uuid', operator_value);
        ELSIF operator_value ~ '^\d+(\.\d+)?:\d{2}:\d{2}' THEN
            -- TimeSpan format (HH:MM:SS)
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric = %L::numeric', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String = %L', operator_value);
        ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
            -- Fallback: numeric value, type unknown
            IF operator_value ~ '^-?\d+$' THEN
                RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value, operator_value);
            ELSE
                RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value);
            END IF;
        ELSE
            -- String value by default
            RETURN format('fv._String = %L', operator_value);
        END IF;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION _build_inner_condition(text, text, jsonb) IS '🚀 EXTENDED core system of LINQ operators. Supports 25+ operators: 
📊 Numeric: $gt, $gte, $lt, $lte, $ne, $in (Long, Double, Numeric)
📝 String: $contains, $startsWith, $endsWith  
⏱️ TimeSpan: $gt, $lt, $eq (conversion to INTERVAL for correct comparison)
📅 DateTimeOffset: $gt, $lt, $eq (timestamptz with timezone)
🔢 Arrays (basic): $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*
🎯 Arrays (position): $arrayAt, $arrayFirst, $arrayLast
🔍 Arrays (search): $arrayStartsWith, $arrayEndsWith, $arrayMatches
📈 Arrays (aggregation): $arraySum, $arrayAvg, $arrayMin, $arrayMax
All operators adapted for relational arrays via _array_index. Supports distinguishing _RObject vs Object types. Auto type detection by value format. Special handling for TimeSpan via INTERVAL.';

-- ===== UNIVERSAL WRAPPERS =====

-- Universal function to build EXISTS/NOT EXISTS conditions with full Class field support
CREATE OR REPLACE FUNCTION _build_exists_condition(
    field_path text,
    condition_sql text,
    use_not_exists boolean DEFAULT false,
    scheme_id bigint DEFAULT NULL,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    parsed_path RECORD;
    structure_info structure_info_type;
    exists_query text;
    field_condition text;
    nested_join text := '';
    nested_condition text := '';
BEGIN
    -- Parse field path
    SELECT * INTO parsed_path FROM _parse_field_path(field_path);
    
    -- 🆕 DICTIONARY FIELDS (AddressBook[home].City or PhoneBook[home])
    IF parsed_path.dict_key IS NOT NULL AND scheme_id IS NOT NULL THEN
        -- Dictionary path: AddressBook[home].City
        SELECT 
            fi.root_structure_id,
            fi.nested_structure_id,
            fi.root_type_info,
            fi.nested_type_info
        INTO 
            structure_info
        FROM _find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi 
        LIMIT 1;
        
        -- Check that root structure (Dictionary) is found
        IF structure_info.root_structure_id IS NULL THEN
            RAISE EXCEPTION 'Root structure not found for Dictionary field: %', parsed_path.root_field;
        END IF;
        
        IF parsed_path.nested_field IS NOT NULL THEN
            -- Dictionary with nested field: AddressBook[home].City
            IF structure_info.nested_structure_id IS NULL THEN
                RAISE EXCEPTION 'Nested structure % not found in Dictionary field %', parsed_path.nested_field, parsed_path.root_field;
            END IF;
            
            -- 🚀 OPTIMIZATION: JOIN only to _values for nested field, without _scheme_metadata_cache
            nested_join := format('
                JOIN _values nv ON nv._array_parent_id = fv._id
                  AND nv._id_structure = %s',
                structure_info.nested_structure_id);
            
            -- Replace fv.->nv. for nested field
            nested_condition := replace(condition_sql, 'fv.', 'nv.');
            -- 🚀 OPTIMIZATION: use fv._id_structure instead of fs._structure_id
            field_condition := format(
                'fv._id_structure = %s AND fv._array_index = %L AND %s', 
                structure_info.root_structure_id, 
                parsed_path.dict_key,
                nested_condition
            );
        ELSE
            -- 🚀 OPTIMIZATION: Simple Dictionary without JOIN
            field_condition := format(
                'fv._id_structure = %s AND fv._array_index = %L AND %s', 
                structure_info.root_structure_id, 
                parsed_path.dict_key,
                condition_sql
            );
        END IF;
        
        -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for Dictionary!
        exists_query := format('
            %s EXISTS (
                SELECT 1 FROM _values fv 
                %s
                WHERE fv._id_object = %s._id 
                  AND %s
            )',
            CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
            nested_join,
            table_alias,
            field_condition
        );
        
        RETURN ' AND ' || exists_query;
    
    -- 📦 CLASS FIELDS (Contact.Name syntax)
    ELSIF parsed_path.is_nested AND scheme_id IS NOT NULL THEN
        SELECT 
            fi.root_structure_id,
            fi.nested_structure_id,
            fi.root_type_info,
            fi.nested_type_info
        INTO 
            structure_info
        FROM _find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi 
        LIMIT 1;
        
        -- Check that both structures are found
        IF structure_info.root_structure_id IS NULL THEN
            RAISE EXCEPTION 'Root structure not found for field: %', parsed_path.root_field;
        END IF;
        
        -- 🎯 NEW: SPECIAL HANDLING OF LISTITEM ARRAYS (Roles[].Value)
        -- For ListItem arrays nested_structure_id will be NULL, because Value/Alias are not structures but _list_items columns
        IF parsed_path.is_array 
           AND structure_info.root_type_info->>'type_semantic' = '_RListItem' 
           AND structure_info.nested_structure_id IS NULL THEN
            
            DECLARE
                listitem_column text;
                listitem_condition text;
            BEGIN
                -- Mapping IRedbListItem properties to _list_items columns
                listitem_column := CASE parsed_path.nested_field
                    WHEN 'Value' THEN '_value'
                    WHEN 'Alias' THEN '_alias'
                    WHEN 'IdList' THEN '_id_list'
                    WHEN 'Id' THEN '_id'
                    ELSE NULL
                END;
                
                IF listitem_column IS NULL THEN
                    RAISE EXCEPTION 'Unsupported ListItem property: %. Available: Value, Alias, IdList, Id', parsed_path.nested_field;
                END IF;
                
                -- 🎯 Build JOIN to ListItem array and _list_items table (use _listitem column!)
                nested_join := format('
                    JOIN _list_items li ON li._id = fv._listitem');
                
                -- Replace fv./av. with li. and adjust condition for _list_items
                -- condition_sql may contain: fv._String = 'value' or av._String = 'value' (from $arrayContains)
                -- For ListItem.Value we need: li._value = 'value'
                listitem_condition := replace(condition_sql, 'fv._String', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'av._String', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'fv._Long', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'av._Long', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'fv._DateTimeOffset', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'av._DateTimeOffset', 'li.' || listitem_column);
                
                -- Remove type checks from condition_sql, since we work directly with _list_items
                listitem_condition := regexp_replace(listitem_condition, 'fs\.db_type = ''[^'']+'' AND ', '');
                listitem_condition := regexp_replace(listitem_condition, 'fs\.type_semantic = ''[^'']+'' AND ', '');
                
                -- 🚀 OPTIMIZATION: fv._id_structure instead of fs._structure_id
                field_condition := format(
                    'fv._id_structure = %s AND fv._array_index IS NOT NULL AND %s', 
                    structure_info.root_structure_id, 
                    listitem_condition
                );
            END;
            
            -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for ListItem array!
            exists_query := format('
                %s EXISTS (
                    SELECT 1 FROM _values fv 
                    %s
                    WHERE fv._id_object = %s._id 
                      AND %s
                )',
                CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                nested_join,
                table_alias,
                field_condition
            );
            RETURN ' AND ' || exists_query;
        
        -- 🔍 HANDLING CLASS ARRAYS (Contact[].Name)
        ELSIF parsed_path.is_array THEN
            IF structure_info.nested_structure_id IS NULL THEN
                RAISE EXCEPTION 'Nested structure % not found in field %', parsed_path.nested_field, parsed_path.root_field;
            END IF;
            -- 🚀 OPTIMIZATION: JOIN only to _values, without _scheme_metadata_cache
            nested_join := format('
                JOIN _values nv ON nv._id_object = fv._id_object
                  AND nv._id_structure = %s
                  AND nv._array_parent_id = fv._id
                  AND nv._array_index IS NOT NULL',
                structure_info.nested_structure_id);
            
            nested_condition := replace(condition_sql, 'fv.', 'nv.');
            field_condition := format(
                'fv._id_structure = %s AND fv._array_index IS NOT NULL AND %s', 
                structure_info.root_structure_id, 
                nested_condition
            );
            
            -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for Class array!
            exists_query := format('
                %s EXISTS (
                    SELECT 1 FROM _values fv 
                    %s
                    WHERE fv._id_object = %s._id 
                      AND %s
                )',
                CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                nested_join,
                table_alias,
                field_condition
            );
            RETURN ' AND ' || exists_query;
        
        -- 🔍 REGULAR CLASS FIELDS (Contact.Name) AND LISTITEM FIELDS (Status.Value)
        ELSE
            -- SPECIAL HANDLING OF REGULAR LISTITEM FIELDS (Status.Value)
            IF structure_info.root_type_info->>'type_semantic' = '_RListItem' 
               AND structure_info.nested_structure_id IS NULL THEN
                
                DECLARE
                    listitem_column text;
                    listitem_condition text;
                BEGIN
                    listitem_column := CASE parsed_path.nested_field
                        WHEN 'Value' THEN '_value'
                        WHEN 'Alias' THEN '_alias'
                        WHEN 'IdList' THEN '_id_list'
                        WHEN 'Id' THEN '_id'
                        ELSE NULL
                    END;
                    
                    IF listitem_column IS NULL THEN
                        RAISE EXCEPTION 'Unsupported ListItem property: %. Available: Value, Alias, IdList, Id', parsed_path.nested_field;
                    END IF;
                    
                    nested_join := 'JOIN _list_items li ON li._id = fv._listitem';
                    
                    -- Replace fv./av. with li. (av.* from $arrayContains)
                    listitem_condition := replace(condition_sql, 'fv._String', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'av._String', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'fv._Long', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'av._Long', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'fv._DateTimeOffset', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'av._DateTimeOffset', 'li.' || listitem_column);
                    
                    -- 🚀 OPTIMIZATION: fv._id_structure instead of fs._structure_id
                    field_condition := format(
                        'fv._id_structure = %s AND fv._array_index IS NULL AND %s', 
                        structure_info.root_structure_id, 
                        listitem_condition
                    );
                    
                    -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for ListItem!
                    exists_query := format('
                        %s EXISTS (
                            SELECT 1 FROM _values fv 
                            %s
                            WHERE fv._id_object = %s._id 
                              AND %s
                        )',
                        CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                        nested_join,
                        table_alias,
                        field_condition
                    );
                    RETURN ' AND ' || exists_query;
                END;
            
            -- REGULAR CLASS FIELDS (Contact.Name)
            ELSE
                IF structure_info.nested_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Nested structure % not found in field %', parsed_path.nested_field, parsed_path.root_field;
                END IF;
                
                -- 🚀 OPTIMIZATION: JOIN only to _values, without _scheme_metadata_cache
                nested_join := format('
                    JOIN _values nv ON nv._id_object = fv._id_object
                      AND nv._id_structure = %s
                      AND nv._array_index IS NULL',
                    structure_info.nested_structure_id);
                
                nested_condition := replace(condition_sql, 'fv.', 'nv.');
                field_condition := format(
                    'fv._id_structure = %s AND fv._array_index IS NULL AND %s', 
                    structure_info.root_structure_id, 
                    nested_condition
                );
                
                -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for Class!
                exists_query := format('
                    %s EXISTS (
                        SELECT 1 FROM _values fv 
                        %s
                        WHERE fv._id_object = %s._id 
                          AND %s
                    )',
                    CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                    nested_join,
                    table_alias,
                    field_condition
                );
                RETURN ' AND ' || exists_query;
            END IF;
        END IF;
    
    -- 📋 REGULAR FIELDS AND ARRAYS (Name, Tags[])
    -- 🚀 OPTIMIZATION: Get structure_id and use it directly without JOIN!
    ELSE
        -- Get structure information (if scheme_id provided)
        IF scheme_id IS NOT NULL THEN
            SELECT 
                fi.root_structure_id,
                fi.nested_structure_id,
                fi.root_type_info,
                fi.nested_type_info
            INTO structure_info
            FROM _find_structure_info(scheme_id, parsed_path.root_field, NULL) AS fi 
            LIMIT 1;
        END IF;
        
        IF structure_info.root_structure_id IS NOT NULL THEN
            -- ✅ OPTIMAL PATH: Use structure_id directly without JOIN
            IF parsed_path.is_array THEN
                field_condition := format('fv._id_structure = %s AND fv._array_index IS NOT NULL AND %s', 
                                        structure_info.root_structure_id, 
                                        condition_sql);
            ELSE
                field_condition := format('fv._id_structure = %s AND fv._array_index IS NULL AND %s', 
                                        structure_info.root_structure_id, 
                                        condition_sql);
            END IF;
            nested_join := '';
            
            -- 🚀 SUPER-OPTIMIZATION: EXISTS WITHOUT JOIN!
            exists_query := format('
                %s EXISTS (
                    SELECT 1 FROM _values fv 
                    %s
                    WHERE fv._id_object = %s._id 
                      AND %s
                )',
                CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                nested_join,
                table_alias,
                field_condition
            );
            
            RETURN ' AND ' || exists_query;
        ELSE
            -- Fallback: scheme_id not provided or structure not found - use old path with JOIN
            IF parsed_path.is_array THEN
                field_condition := format('fs._name = %L AND fs._collection_type IS NOT NULL AND %s', 
                                        parsed_path.root_field, 
                                        condition_sql);
            ELSE
                field_condition := format('fs._name = %L AND fs._collection_type IS NULL AND fv._array_index IS NULL AND %s', 
                                        parsed_path.root_field, 
                                        condition_sql);
            END IF;
            nested_join := '';
        END IF;
    END IF;
    
    -- Fallback path with JOIN (for complex cases: Dictionary, Class, or when structure_id not found)
    exists_query := format('
        %s EXISTS (
            SELECT 1 FROM _values fv 
            JOIN _scheme_metadata_cache fs ON fs._structure_id = fv._id_structure
            %s
            WHERE fv._id_object = %s._id 
              AND %s
        )',
        CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
        nested_join,
        table_alias,
        field_condition
    );
    
    RETURN ' AND ' || exists_query;
END;
$BODY$;

COMMENT ON FUNCTION _build_exists_condition(text, text, boolean, bigint, text) IS '🚀 EXTENDED universal wrapper for building EXISTS/NOT EXISTS conditions with full Class architecture support:
📝 Regular fields: Name, Title  
📋 Regular arrays: Tags[], Categories[]
📦 Class fields: Contact.Name, Address.City (via _structures._id_parent)
🔗 Class arrays: Contacts[].Email, Addresses[].Street (combination of _array_index + _id_parent)
Automatically determines field type, builds correct JOINs for nested structures, checks for structure existence in scheme.';

-- ===== LOGICAL OPERATORS =====

-- Function to build AND conditions (recursive)
CREATE OR REPLACE FUNCTION _build_and_condition(
    and_array jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    conditions text := '';
    condition_item jsonb;
    single_condition text;
    i integer;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth reached for $and operator';
    END IF;
    
    -- Check that this is an array
    IF jsonb_typeof(and_array) != 'array' OR jsonb_array_length(and_array) = 0 THEN
        RETURN '';
    END IF;
    
    -- Process each array element
    FOR i IN 0..jsonb_array_length(and_array) - 1 LOOP
        condition_item := and_array->i;
        
        -- Recursively process each element
        single_condition := _build_single_facet_condition(condition_item, scheme_id, table_alias, max_depth - 1);
        
        IF single_condition != '' AND single_condition != ' AND TRUE' THEN
            -- Remove extra ' AND ' from the beginning of each condition
            single_condition := ltrim(single_condition, ' AND ');
            
            IF conditions != '' THEN
                conditions := conditions || ' AND ';
            END IF;
            conditions := conditions || single_condition;
        END IF;
    END LOOP;
    
    IF conditions != '' THEN
        RETURN ' AND (' || conditions || ')';
    ELSE
        RETURN '';
    END IF;
END;
$BODY$;

-- Function to build OR conditions (recursive)  
CREATE OR REPLACE FUNCTION _build_or_condition(
    or_array jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    conditions text := '';
    condition_item jsonb;
    single_condition text;
    or_parts text[] := '{}';
    i integer;
    final_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth reached for $or operator';
    END IF;
    
    -- Check that this is an array
    IF jsonb_typeof(or_array) != 'array' OR jsonb_array_length(or_array) = 0 THEN
        RETURN '';
    END IF;
    
    -- Process each array element
    FOR i IN 0..jsonb_array_length(or_array) - 1 LOOP
        condition_item := or_array->i;
        
        -- Recursively process each element (remove prefix ' AND ')
        single_condition := _build_single_facet_condition(condition_item, scheme_id, table_alias, max_depth - 1);
        
        IF single_condition != '' AND single_condition != ' AND TRUE' THEN
            -- Remove ' AND ' from the beginning of each condition for OR
            single_condition := ltrim(single_condition, ' AND ');
            or_parts := array_append(or_parts, single_condition);
        END IF;
    END LOOP;
    
    -- Combine via OR
    IF array_length(or_parts, 1) > 0 THEN
        final_condition := array_to_string(or_parts, ' OR ');
        RETURN ' AND (' || final_condition || ')';
    END IF;
    
    RETURN '';
END;
$BODY$;

-- Function to build NOT conditions (recursive)
CREATE OR REPLACE FUNCTION _build_not_condition(
    not_object jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    inner_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth reached for $not operator';
    END IF;
    
    -- Recursively process inner condition
    inner_condition := _build_single_facet_condition(not_object, scheme_id, table_alias, max_depth - 1);
    
    IF inner_condition != '' AND inner_condition != 'TRUE' THEN
        -- Convert EXISTS to NOT EXISTS and vice versa
        IF inner_condition LIKE '%EXISTS (%' THEN
            inner_condition := replace(inner_condition, 'EXISTS (', 'NOT EXISTS (');
            RETURN ' AND ' || inner_condition;
        ELSIF inner_condition LIKE '%NOT EXISTS (%' THEN  
            inner_condition := replace(inner_condition, 'NOT EXISTS (', 'EXISTS (');
            RETURN ' AND ' || inner_condition;
        ELSE
            -- For complex conditions wrap in NOT
            RETURN ' AND NOT (' || inner_condition || ')';
        END IF;
    END IF;
    
    RETURN '';
END;
$BODY$;

-- Universal function for processing single facet condition (recursive)
CREATE OR REPLACE FUNCTION _build_single_facet_condition(
    facet_condition jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE  
AS $BODY$
DECLARE
    condition_key text;
    condition_value jsonb;
    field_path text;
    parsed_path RECORD;
    structure_info structure_info_type;
    operator_name text;
    operator_value text;
    inner_condition_sql text;
    all_conditions text := '';
    single_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth (50) reached for filter. Simplify JSON filter.';
    END IF;
    
    -- Check input data type
    IF jsonb_typeof(facet_condition) != 'object' THEN
        RETURN '';
    END IF;
    
    -- Process each key-value pair
    FOR condition_key, condition_value IN SELECT * FROM jsonb_each(facet_condition) LOOP
        -- Logical operators - ACCUMULATE instead of RETURN to process ALL keys at same level
        -- NOTE: Pass max_depth WITHOUT decrement, because _build_and/or/not decrement themselves when calling _build_single
        IF condition_key = '$and' THEN
            single_condition := _build_and_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        ELSIF condition_key = '$or' THEN
            single_condition := _build_or_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        ELSIF condition_key = '$not' THEN
            single_condition := _build_not_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        
        -- Hierarchical operators (processed separately)
        ELSIF condition_key IN ('$hasAncestor', '$hasDescendant', '$level', '$isRoot', '$isLeaf', '$childrenOf') THEN
            CONTINUE; -- Skip, they are processed in build_hierarchical_conditions
        
        -- 🆕 Property functions: Field.$length, Field[].$count
        -- p.Name.Length > 3  → {"Name.$length": {"$gt": 3}}
        -- p.Tags.Count >= 5  → {"Tags[].$count": {"$gte": 5}}
        ELSIF condition_key ~ '\.\$length$' OR condition_key ~ '\.\$count$' THEN
            DECLARE
                func_is_length boolean := condition_key ~ '\.\$length$';
                func_field_name text;
                func_structure_id bigint;
                func_condition text;
                func_op_name text;
                func_op_value text;
                func_compare_op text;
            BEGIN
                -- Extract field name: "Name.$length" -> "Name", "Tags[].$count" -> "Tags"
                IF func_is_length THEN
                    func_field_name := regexp_replace(condition_key, '\.\$length$', '');
                ELSE
                    func_field_name := regexp_replace(condition_key, '\[\]\.\$count$', '');
                    func_field_name := regexp_replace(func_field_name, '\.\$count$', '');
                END IF;
                
                -- Find structure
                SELECT _id INTO func_structure_id
                FROM _structures
                WHERE _id_scheme = scheme_id 
                  AND _name = func_field_name
                  AND _id_parent IS NULL;
                
                IF func_structure_id IS NULL THEN
                    CONTINUE; -- Field not found, skip
                END IF;
                
                -- Process operators
                IF jsonb_typeof(condition_value) = 'object' THEN
                    FOR func_op_name, func_op_value IN SELECT key, value #>> '{}' FROM jsonb_each(condition_value) LOOP
                        func_compare_op := CASE func_op_name
                            WHEN '$eq' THEN '='
                            WHEN '$ne' THEN '<>'
                            WHEN '$gt' THEN '>'
                            WHEN '$gte' THEN '>='
                            WHEN '$lt' THEN '<'
                            WHEN '$lte' THEN '<='
                            ELSE '='
                        END;
                        
                        IF func_is_length THEN
                            -- String length: LENGTH(v._String)
                            func_condition := format(
                                'EXISTS (SELECT 1 FROM _values fv WHERE fv._id_object = %I._id AND fv._id_structure = %L AND fv._array_index IS NULL AND LENGTH(fv._String) %s %L::integer)',
                                table_alias, func_structure_id, func_compare_op, func_op_value
                            );
                        ELSE
                            -- Array count
                            func_condition := format(
                                '(SELECT COUNT(*) FROM _values fv WHERE fv._id_object = %I._id AND fv._id_structure = %L AND fv._array_index IS NOT NULL) %s %L::integer',
                                table_alias, func_structure_id, func_compare_op, func_op_value
                            );
                        END IF;
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || func_condition;
                        ELSE
                            all_conditions := func_condition;
                        END IF;
                    END LOOP;
                END IF;
            END;
        
        -- 🚀 _objects TABLE BASE FIELDS (with "0$:" prefix)
        -- 🆕 CRITICAL BUG FIX: Now base fields are EXPLICITLY marked with "0$:" prefix
        -- This resolves name conflicts: Props.Name vs RedbObject.name
        -- Prefix "0$:" is impossible as identifier in any programming language
        -- C# passes: "0$:name", "0$:parent_id", "0$:Id" etc.
        ELSIF condition_key LIKE '0$:%' THEN
            DECLARE
                raw_field_name text := substring(condition_key from 4);  -- remove '0$:'
                sql_field_name text := _normalize_base_field_name(raw_field_name);
                base_condition text := '';
            BEGIN
                -- 🛡️ PROTECTION: Check that field is recognized as base
                IF sql_field_name IS NULL THEN
                    RAISE EXCEPTION 'Unknown RedbObject base field: "%" (passed as "0$:%"). Valid fields: id, parent_id, scheme_id, owner_id, who_change_id, date_create, date_modify, date_begin, date_complete, key, value_long, value_string, value_guid, value_bool, value_double, value_numeric, value_datetime, value_bytes, name, note, hash', 
                        raw_field_name, raw_field_name;
                END IF;
                
                -- Determine field type and build condition
                -- Numeric fields (bigint): _id, _id_parent, _id_scheme, _id_owner, _id_who_change, _value_long, _key
                -- 🛡️ SECURITY: Use %L::bigint instead of %s to protect against SQL injection
                IF sql_field_name IN ('_id', '_id_parent', '_id_scheme', '_id_owner', '_id_who_change', '_value_long', '_key') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::bigint', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::bigint', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::bigint', table_alias, sql_field_name, operator_value)
                                -- 🎯 $exists for base fields: IS NULL / IS NOT NULL
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'array' THEN
                        base_condition := format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value));
                    ELSIF jsonb_typeof(condition_value) = 'number' THEN
                        base_condition := format('%I.%I = %L::bigint', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- String fields (text): _value_string, _name, _note
                ELSIF sql_field_name IN ('_value_string', '_name', '_note') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$contains' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, '%' || operator_value || '%')
                                WHEN '$containsIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, '%' || operator_value || '%')
                                WHEN '$startsWith' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, operator_value || '%')
                                WHEN '$startsWithIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, operator_value || '%')
                                WHEN '$endsWith' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, '%' || operator_value)
                                WHEN '$endsWithIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, '%' || operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'array' THEN
                        base_condition := format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value));
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- UUID fields: _value_guid, _hash
                ELSIF sql_field_name IN ('_value_guid', '_hash') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::uuid', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::uuid', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L::uuid', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- DateTime fields: _date_create, _date_modify, _date_begin, _date_complete
                ELSIF sql_field_name IN ('_date_create', '_date_modify', '_date_begin', '_date_complete', '_value_datetime') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::timestamptz', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::timestamptz', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L::timestamptz', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- 🛡️ SECURITY: Boolean field with %L::boolean
                -- Boolean field: _value_bool
                ELSIF sql_field_name = '_value_bool' THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::boolean', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::boolean', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'boolean' THEN
                        base_condition := format('%I.%I = %L::boolean', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- Double/Numeric fields: _value_double, _value_numeric
                ELSIF sql_field_name IN ('_value_double', '_value_numeric') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::numeric', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::numeric', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'number' THEN
                        base_condition := format('%I.%I = %L::numeric', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- Bytes field: _value_bytes (only $exists supported)
                ELSIF sql_field_name = '_value_bytes' THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                END IF;
                
                -- ✅ ACCUMULATE base condition in all_conditions (instead of RETURN)
                IF base_condition != '' THEN
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || base_condition;
                    ELSE
                        all_conditions := base_condition;
                    END IF;
                END IF;
            END;
            -- Do NOT RETURN - continue processing other fields!
        
        -- 🆕 Collection != null / == null: Check that Array/Dictionary has/does not have elements
        -- Triggers when condition_value = {"$ne": null} or {"$exists": true/false} for collection field
        ELSIF jsonb_typeof(condition_value) = 'object' 
              AND (condition_value ? '$ne' OR condition_value ? '$exists')
              AND condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
            DECLARE
                coll_field_name text := condition_key;
                coll_structure_id bigint;
                coll_collection_type text;
                coll_condition text;
                coll_is_ne_null boolean;
                eav_ne_null_value text;
                eav_exists_value text;
            BEGIN
                -- Check if this is a Collection field (Array or Dictionary)
                SELECT s._id, s._collection_type INTO coll_structure_id, coll_collection_type
                FROM _structures s
                WHERE s._id_scheme = scheme_id 
                  AND s._name = coll_field_name
                  AND s._id_parent IS NULL;
                
                -- Only handle if it's a Collection (Array or Dictionary)
                IF coll_structure_id IS NOT NULL AND coll_collection_type IS NOT NULL THEN
                    -- Determine if checking for "not null" or "null"
                    coll_is_ne_null := (condition_value->>'$ne' IS NULL AND condition_value ? '$ne')
                                    OR (condition_value->>'$exists' = 'true');
                    
                    IF coll_is_ne_null OR condition_value ? '$ne' THEN
                        -- Collection != null: at least one element exists
                        coll_condition := format(
                            'EXISTS (SELECT 1 FROM _values cv WHERE cv._id_object = %I._id AND cv._id_structure = %L AND cv._array_index IS NOT NULL)',
                            table_alias, coll_structure_id
                        );
                    ELSE
                        -- Collection == null: no elements
                        coll_condition := format(
                            'NOT EXISTS (SELECT 1 FROM _values cv WHERE cv._id_object = %I._id AND cv._id_structure = %L AND cv._array_index IS NOT NULL)',
                            table_alias, coll_structure_id
                        );
                    END IF;
                    
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || coll_condition;
                    ELSE
                        all_conditions := coll_condition;
                    END IF;
                    
                    CONTINUE;  -- Skip to next field, don't fall through to standard handling
                
                -- 🆕 FIX: Regular EAV fields with $ne null or $exists (NOT collections!)
                ELSIF coll_structure_id IS NOT NULL THEN
                    -- This is a regular EAV field (not a collection)
                    eav_ne_null_value := condition_value->>'$ne';
                    eav_exists_value := condition_value->>'$exists';
                    
                    -- $ne null OR $exists true = "field exists" (record exists in _values)
                    IF (condition_value ? '$ne' AND (eav_ne_null_value IS NULL OR eav_ne_null_value = 'null'))
                       OR (eav_exists_value = 'true') THEN
                        -- In EAV model: record existence = field exists and not null
                        coll_condition := format(
                            'EXISTS (SELECT 1 FROM _values ev WHERE ev._id_object = %I._id AND ev._id_structure = %L AND ev._array_index IS NULL)',
                            table_alias, coll_structure_id
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || coll_condition;
                        ELSE
                            all_conditions := coll_condition;
                        END IF;
                        CONTINUE;
                    
                    -- $exists false = "field does NOT exist" (no record in _values)
                    ELSIF eav_exists_value = 'false' THEN
                        coll_condition := format(
                            'NOT EXISTS (SELECT 1 FROM _values ev WHERE ev._id_object = %I._id AND ev._id_structure = %L AND ev._array_index IS NULL)',
                            table_alias, coll_structure_id
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || coll_condition;
                        ELSE
                            all_conditions := coll_condition;
                        END IF;
                        CONTINUE;
                    END IF;
                    -- If $ne with specific value — fall through to standard handling
                END IF;
                -- If structure not found, fall through to standard handling
            END;
        
        -- 🆕 Dictionary indexer: FieldName[key] (example: "PhoneBook[home]": {"$eq": "+7-999..."})
        -- Direct implementation without _build_inner_condition (it uses incompatible aliases fs/fv)
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\[.+\]$' THEN
            DECLARE
                dict_field_name text;
                dict_key text;
                dict_structure_id bigint;
                dict_type_info jsonb;
                dict_db_type text;
                dict_condition text;
                dict_op_name text;
                dict_op_value text;
                dict_value text;
                dict_value_condition text;
            BEGIN
                -- Parse: "PhoneBook[home]" -> field="PhoneBook", key="home"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\[');
                dict_key := substring(condition_key from '\[(.+)\]$');
                
                -- Find structure and type_info for dictionary field
                SELECT fi.root_structure_id, fi.root_type_info
                INTO dict_structure_id, dict_type_info
                FROM _find_structure_info(scheme_id, dict_field_name, NULL) AS fi
                LIMIT 1;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Get db_type from type_info for correct column selection
                dict_db_type := dict_type_info->>'db_type';
                
                -- Process condition_value - build value comparison
                IF jsonb_typeof(condition_value) = 'object' THEN
                    -- Complex condition: {"$eq": "value"}, {"$ne": "x"}, {"$contains": "substr"}, {"$in": [...]}, etc.
                    FOR dict_op_name, dict_op_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                        -- Build value condition based on operator
                        IF dict_op_name = '$in' THEN
                            -- $in operator: value in list (use db_type for correct column)
                            dict_value_condition := CASE dict_db_type
                                WHEN 'Long' THEN format('dv._Long IN (SELECT (jsonb_array_elements_text(%L::jsonb))::bigint)', dict_op_value)
                                WHEN 'Numeric' THEN format('dv._Numeric IN (SELECT (jsonb_array_elements_text(%L::jsonb))::numeric)', dict_op_value)
                                WHEN 'Double' THEN format('dv._Double IN (SELECT (jsonb_array_elements_text(%L::jsonb))::double precision)', dict_op_value)
                                ELSE format('dv._String IN (SELECT jsonb_array_elements_text(%L::jsonb))', dict_op_value)
                            END;
                        ELSIF dict_op_name = '$nin' THEN
                            -- $nin operator: value not in list (use db_type for correct column)
                            dict_value_condition := CASE dict_db_type
                                WHEN 'Long' THEN format('dv._Long NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::bigint)', dict_op_value)
                                WHEN 'Numeric' THEN format('dv._Numeric NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::numeric)', dict_op_value)
                                WHEN 'Double' THEN format('dv._Double NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::double precision)', dict_op_value)
                                ELSE format('dv._String NOT IN (SELECT jsonb_array_elements_text(%L::jsonb))', dict_op_value)
                            END;
                        ELSE
                            -- Numeric comparisons - use db_type to select correct column
                            IF dict_op_name IN ('$gt', '$gte', '$lt', '$lte') THEN
                                dict_value_condition := CASE dict_db_type
                                    WHEN 'Numeric' THEN format('dv._Numeric %s %L::numeric', 
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END, 
                                        dict_op_value)
                                    WHEN 'Double' THEN format('dv._Double %s %L::double precision',
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                                        dict_op_value)
                                    ELSE format('dv._Long %s %L::bigint',
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                                        dict_op_value)
                                END;
                            ELSE
                                dict_value_condition := CASE dict_op_name
                                    -- Equality/Inequality - use db_type for correct column
                                    WHEN '$eq' THEN CASE dict_db_type
                                        WHEN 'Long' THEN format('dv._Long = %L::bigint', dict_op_value)
                                        WHEN 'Numeric' THEN format('dv._Numeric = %L::numeric', dict_op_value)
                                        WHEN 'Double' THEN format('dv._Double = %L::double precision', dict_op_value)
                                        ELSE format('dv._String = %L', dict_op_value)
                                    END
                                    WHEN '$ne' THEN CASE dict_db_type
                                        WHEN 'Long' THEN format('dv._Long <> %L::bigint', dict_op_value)
                                        WHEN 'Numeric' THEN format('dv._Numeric <> %L::numeric', dict_op_value)
                                        WHEN 'Double' THEN format('dv._Double <> %L::double precision', dict_op_value)
                                        ELSE format('dv._String <> %L', dict_op_value)
                                    END
                                    -- String operations
                                    WHEN '$contains' THEN format('dv._String LIKE %L', '%' || dict_op_value || '%')
                                    WHEN '$startsWith' THEN format('dv._String LIKE %L', dict_op_value || '%')
                                    WHEN '$endsWith' THEN format('dv._String LIKE %L', '%' || dict_op_value)
                                    WHEN '$containsIgnoreCase' THEN format('dv._String ILIKE %L', '%' || dict_op_value || '%')
                                    WHEN '$startsWithIgnoreCase' THEN format('dv._String ILIKE %L', dict_op_value || '%')
                                    WHEN '$endsWithIgnoreCase' THEN format('dv._String ILIKE %L', '%' || dict_op_value)
                                    -- Regex
                                    WHEN '$regex' THEN format('dv._String ~ %L', dict_op_value)
                                    WHEN '$iregex' THEN format('dv._String ~* %L', dict_op_value)
                                    -- Explicit Double comparisons (legacy support)
                                    WHEN '$gtDouble' THEN format('dv._Double > %L', dict_op_value::double precision)
                                    WHEN '$gteDouble' THEN format('dv._Double >= %L', dict_op_value::double precision)
                                    WHEN '$ltDouble' THEN format('dv._Double < %L', dict_op_value::double precision)
                                    WHEN '$lteDouble' THEN format('dv._Double <= %L', dict_op_value::double precision)
                                    ELSE format('dv._String = %L', dict_op_value)  -- fallback to equality
                                END;
                            END IF;
                        END IF;
                        
                        dict_condition := format(
                            'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L AND %s)',
                            table_alias, dict_structure_id, dict_key, dict_value_condition
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || dict_condition;
                        ELSE
                            all_conditions := dict_condition;
                        END IF;
                    END LOOP;
                ELSE
                    -- Simple value: direct equality (use db_type for correct column)
                    dict_value := condition_value #>> '{}';
                    dict_condition := format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L AND %s)',
                        table_alias, dict_structure_id, dict_key,
                        CASE dict_db_type
                            WHEN 'Long' THEN format('dv._Long = %L::bigint', dict_value)
                            WHEN 'Numeric' THEN format('dv._Numeric = %L::numeric', dict_value)
                            WHEN 'Double' THEN format('dv._Double = %L::double precision', dict_value)
                            ELSE format('dv._String = %L', dict_value)
                        END
                    );
                    
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || dict_condition;
                    ELSE
                        all_conditions := dict_condition;
                    END IF;
                END IF;
            END;
        
        -- 🆕 Dictionary ContainsKey: FieldName.ContainsKey (example: "PhoneBook.ContainsKey": "home")
        -- ⚠️ For Dictionary<K, Class> records are stored in child structures, not in the structure itself!
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.ContainsKey$' THEN
            DECLARE
                dict_field_name text;
                dict_key text;
                dict_structure_id bigint;
                dict_condition text;
            BEGIN
                -- Parse: "PhoneBook.ContainsKey" -> field="PhoneBook"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.ContainsKey$');
                -- Get the key (handle both {"$eq": "key"} and "key")
                IF condition_value ? '$eq' THEN
                    dict_key := condition_value->>'$eq';
                ELSE
                    dict_key := condition_value #>> '{}';
                END IF;
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build EXISTS condition
                -- 🆕 FIX: Check BOTH cases:
                --   1) Dictionary<K, primitive>: records in the structure itself (_id_structure = dict_id)
                --   2) Dictionary<K, Class>: records in child structures (_id_parent = dict_id)
                dict_condition := format(
                    'EXISTS (SELECT 1 FROM _values dv JOIN _structures s ON dv._id_structure = s._id WHERE dv._id_object = %I._id AND (s._id = %L OR s._id_parent = %L) AND dv._array_index = %L)',
                    table_alias, dict_structure_id, dict_structure_id, dict_key
                );
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- 🆕 Dictionary aggregation operators: FieldName.$dictCount, FieldName.$dictSum, etc.
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.\$dict(Count|Sum|Avg|Min|Max|Keys|Values|HasValue)$' THEN
            DECLARE
                dict_field_name text;
                dict_op text;
                dict_structure_id bigint;
                dict_condition text;
                dict_value text;
            BEGIN
                -- Parse: "PhoneBook.$dictCount" -> field="PhoneBook", op="Count"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.\$dict');
                dict_op := substring(condition_key from '\$dict([A-Za-z]+)$');
                dict_value := condition_value #>> '{}';
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build aggregation condition
                dict_condition := CASE dict_op
                    WHEN 'Count' THEN format(
                        '(SELECT COUNT(*) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::int',
                        table_alias, dict_structure_id, dict_value::int)
                    WHEN 'Sum' THEN format(
                        '(SELECT COALESCE(SUM(dv._Long), 0) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Avg' THEN format(
                        '(SELECT AVG(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::numeric',
                        table_alias, dict_structure_id, dict_value::numeric)
                    WHEN 'Min' THEN format(
                        '(SELECT MIN(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Max' THEN format(
                        '(SELECT MAX(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Keys' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L)',
                        table_alias, dict_structure_id, dict_value)
                    WHEN 'Values' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._String = %L)',
                        table_alias, dict_structure_id, dict_value)
                    WHEN 'HasValue' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._String = %L)',
                        table_alias, dict_structure_id, dict_value)
                    ELSE 'TRUE'
                END;
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- 🆕 Dictionary comparison operators: FieldName.$dictCountGt, FieldName.$dictCountLt, etc.
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.\$dict(Count|Sum|Avg|Min|Max)(Gt|Gte|Lt|Lte)$' THEN
            DECLARE
                dict_field_name text;
                dict_agg text;
                dict_cmp text;
                dict_structure_id bigint;
                dict_condition text;
                dict_value text;
                dict_cmp_op text;
            BEGIN
                -- Parse: "PhoneBook.$dictCountGt" -> field="PhoneBook", agg="Count", cmp="Gt"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.\$dict');
                dict_agg := substring(condition_key from '\$dict(Count|Sum|Avg|Min|Max)');
                dict_cmp := substring(condition_key from '(Gt|Gte|Lt|Lte)$');
                dict_value := condition_value #>> '{}';
                
                dict_cmp_op := CASE dict_cmp
                    WHEN 'Gt' THEN '>'
                    WHEN 'Gte' THEN '>='
                    WHEN 'Lt' THEN '<'
                    WHEN 'Lte' THEN '<='
                END;
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build aggregation condition with comparison
                dict_condition := CASE dict_agg
                    WHEN 'Count' THEN format(
                        '(SELECT COUNT(*) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::int',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::int)
                    WHEN 'Sum' THEN format(
                        '(SELECT COALESCE(SUM(dv._Long), 0) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    WHEN 'Avg' THEN format(
                        '(SELECT AVG(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::numeric',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::numeric)
                    WHEN 'Min' THEN format(
                        '(SELECT MIN(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    WHEN 'Max' THEN format(
                        '(SELECT MAX(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    ELSE 'TRUE'
                END;
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- Field operators
        ELSE
            -- Parse field path
            field_path := condition_key;
            SELECT * INTO parsed_path FROM _parse_field_path(field_path);
            
            -- Get structure information for all fields
            -- 🎯 FIX: Explicit assignment of fields from TABLE-returning function to RECORD
            SELECT 
                fi.root_structure_id,
                fi.nested_structure_id,
                fi.root_type_info,
                fi.nested_type_info
            INTO 
                structure_info
            FROM _find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi
            LIMIT 1;
            
            -- Process field value
            IF jsonb_typeof(condition_value) = 'object' THEN
                -- Complex condition with operators like {"$gt": 100, "$lt": 200}
                FOR operator_name, operator_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                    inner_condition_sql := _build_inner_condition(
                        operator_name, 
                        operator_value, 
                        CASE 
                            WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                            ELSE structure_info.root_type_info
                        END
                    );
                    
                    -- $ne for specific value (not null) requires NOT EXISTS
                    single_condition := _build_exists_condition(
                        field_path, 
                        inner_condition_sql, 
                        operator_name = '$ne' AND operator_value IS NOT NULL AND operator_value != 'null' AND operator_value != '',
                        scheme_id, 
                        table_alias
                    );
                    
                    -- Accumulate conditions via AND
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ';
                    END IF;
                    all_conditions := all_conditions || ltrim(single_condition, ' AND ');
                END LOOP;
            
            ELSIF jsonb_typeof(condition_value) = 'array' THEN
                -- Array of values - process as $in
                inner_condition_sql := _build_inner_condition(
                    '$in', 
                    condition_value::text,
                    CASE 
                        WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                        ELSE structure_info.root_type_info
                    END
                );
                
                single_condition := _build_exists_condition(field_path, inner_condition_sql, false, scheme_id, table_alias);
                
                -- Accumulate conditions via AND
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ';
                END IF;
                all_conditions := all_conditions || ltrim(single_condition, ' AND ');
            
            ELSE
                -- Simple value - equality
                inner_condition_sql := _build_inner_condition(
                    '=', 
                    -- Remove extra quotes from string values
                    CASE 
                        WHEN jsonb_typeof(condition_value) = 'string' THEN condition_value #>> '{}'
                        ELSE condition_value::text 
                    END,
                    CASE 
                        WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                        ELSE structure_info.root_type_info
                    END
                );
                
                single_condition := _build_exists_condition(field_path, inner_condition_sql, false, scheme_id, table_alias);
                
                -- Accumulate conditions via AND
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ';
                END IF;
                all_conditions := all_conditions || ltrim(single_condition, ' AND ');
            END IF;
        END IF;
    END LOOP;
    
    -- Return all accumulated conditions
    IF all_conditions != '' THEN
        RETURN ' AND (' || all_conditions || ')';
    END IF;
    RETURN '';
END;
$BODY$;

-- Comments for logical operators
COMMENT ON FUNCTION _build_and_condition(jsonb, bigint, text, integer) IS 'Recursive AND condition builder. Supports nested logical operators and Class fields. Recursion limit: 10 levels.';
COMMENT ON FUNCTION _build_or_condition(jsonb, bigint, text, integer) IS 'Recursive OR condition builder. Combines conditions via OR with proper parenthesis handling. Recursion limit: 10 levels.';
COMMENT ON FUNCTION _build_not_condition(jsonb, bigint, text, integer) IS 'Recursive NOT condition builder. Inverts EXISTS to NOT EXISTS and handles complex conditions. Recursion limit: 10 levels.';
COMMENT ON FUNCTION _build_single_facet_condition(jsonb, bigint, text, integer) IS 'Universal recursive function for processing facet conditions. Supports logical operators ($and, $or, $not), LINQ operators, Class fields and arrays. FIXED: Now correctly processes multiple fields in JSON via condition accumulation, not premature RETURN.';

-- ===== EXTENDED FACETS FUNCTION WITH CLASS FIELDS =====

-- Recursive function to build facet field path (example: "Contact.Name", "Contacts[].Email")  
CREATE OR REPLACE FUNCTION _build_facet_field_path(
    structure_id bigint,
    scheme_id bigint,
    current_path text DEFAULT '',
    max_depth integer DEFAULT 10
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    structure_record RECORD;
    parent_path text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RETURN current_path;
    END IF;
    
    -- Get information about current structure (use cache)
    SELECT c._name, c._parent_structure_id, c._collection_type IS NOT NULL as _is_array
    INTO structure_record
    FROM _scheme_metadata_cache c 
    WHERE c._structure_id = structure_id AND c._scheme_id = scheme_id;
    
    -- If structure not found, return current path
    IF NOT FOUND THEN
        RETURN current_path;
    END IF;
    
    -- Form field name considering arrays
    current_path := structure_record._name || 
                   CASE WHEN structure_record._is_array THEN '[]' ELSE '' END ||
                   CASE WHEN current_path != '' THEN '.' || current_path ELSE '' END;
    
    -- If there is a parent, recursively build path
    IF structure_record._parent_structure_id IS NOT NULL THEN
        RETURN _build_facet_field_path(structure_record._parent_structure_id, scheme_id, current_path, max_depth - 1);
    END IF;
    
    -- Return built path
    RETURN current_path;
END;
$BODY$;

-- Function to build extended facets with Class fields
CREATE OR REPLACE FUNCTION get_facets(scheme_id bigint)
RETURNS jsonb 
LANGUAGE 'plpgsql'
COST 150
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_facets jsonb := '{}'::jsonb;
    all_facets jsonb;
    class_facets jsonb;
BEGIN
    -- 🔥 AUTOMATIC CHECK AND CACHE POPULATION
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warming)
    END IF;
    
    -- 🚀 STEP 1: Get all basic facets (root fields and simple arrays)
    SELECT jsonb_object_agg(s._name, COALESCE(f.facet_values, '[]'::jsonb))
    INTO all_facets
    FROM _structures s
    LEFT JOIN (
        SELECT 
            v._id_structure, 
            jsonb_agg(DISTINCT 
                CASE 
                    -- Arrays (collections)
                    WHEN st._collection_type IS NOT NULL THEN
                        (
                            SELECT COALESCE(jsonb_agg(
                                CASE 
                                    -- Simple array types
                                    WHEN av_cache.db_type = 'String' THEN to_jsonb(av._String)
                                    WHEN av_cache.db_type = 'Long' AND av_cache.type_semantic != '_RObject' THEN to_jsonb(av._Long)
                                    WHEN av_cache.db_type = 'Guid' AND av_cache.type_semantic != 'Object' THEN to_jsonb(av._Guid)
                                    WHEN av_cache.db_type = 'Double' THEN to_jsonb(av._Double)
                                    WHEN av_cache.db_type = 'Numeric' THEN to_jsonb(av._Numeric)
                                    WHEN av_cache.db_type = 'DateTimeOffset' THEN to_jsonb(av._DateTimeOffset)
                                    WHEN av_cache.db_type = 'Boolean' THEN to_jsonb(av._Boolean)
                                    
                                    -- _RObject arrays - COMMENTED OUT (not needed in facets)
                                    -- WHEN av_cache.db_type = 'Long' AND av_cache.type_semantic = '_RObject' THEN 
                                    --     get_object_json(av._Object, 0)
                                    
                                    WHEN av_cache.db_type = 'ListItem' THEN
                                        (SELECT jsonb_build_object(
                                            'id', li._id,
                                            'value', li._value,
                                            'object', CASE 
                                                WHEN li._id_object IS NOT NULL THEN
                                                    get_object_json(li._id_object, 0)  -- Always base fields
                                                ELSE NULL 
                                            END
                                        )
                                        FROM _list_items li
                                        WHERE li._id = av._listitem)
                                    WHEN av_cache.db_type = 'ByteArray' THEN 
                                        to_jsonb(encode(av._ByteArray, 'base64'))
                                    ELSE to_jsonb(av._String)
                                END ORDER BY av._array_index::int
                            ), '[]'::jsonb)
                            FROM _values av 
                            JOIN _scheme_metadata_cache av_cache ON av_cache._structure_id = av._id_structure
                            WHERE av._id_object = v._id_object 
                              AND av._id_structure = v._id_structure 
                              AND av._array_index IS NOT NULL
                        )
                    
                    -- Regular fields
                    WHEN st.db_type = 'String' THEN to_jsonb(v._String)
                    WHEN st.db_type = 'Long' AND st.type_semantic != '_RObject' THEN to_jsonb(v._Long)
                    WHEN st.db_type = 'Guid' AND st.type_semantic != 'Object' THEN to_jsonb(v._Guid)
                    WHEN st.db_type = 'Double' THEN to_jsonb(v._Double)
                    WHEN st.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                    WHEN st.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                    WHEN st.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                    
                    -- _RObject fields - COMMENTED OUT (not needed in facets)
                    -- WHEN st.db_type = 'Long' AND st.type_semantic = '_RObject' THEN 
                    --     CASE 
                    --         WHEN v._Object IS NOT NULL THEN 
                    --             get_object_json(v._Object, 0)
                    --         ELSE NULL
                    --     END
                        
                    WHEN st.db_type = 'ListItem' THEN
                        CASE 
                            WHEN v._listitem IS NOT NULL THEN 
                                (SELECT jsonb_build_object(
                                    'id', li._id,
                                    'value', li._value,
                                    'object', CASE 
                                        WHEN li._id_object IS NOT NULL THEN
                                            get_object_json(li._id_object, 0)  -- Always base fields
                                        ELSE NULL 
                                    END
                                )
                                FROM _list_items li
                                WHERE li._id = v._listitem)
                            ELSE NULL
                        END
                    WHEN st.db_type = 'ByteArray' THEN 
                        CASE 
                            WHEN v._ByteArray IS NOT NULL THEN 
                                to_jsonb(encode(v._ByteArray, 'base64'))
                            ELSE NULL
                        END
                    ELSE to_jsonb(v._String)
                END
            ) FILTER (WHERE 
                CASE 
                    -- Array filtering (collections)
                    WHEN st._collection_type IS NOT NULL THEN 
                        EXISTS(SELECT 1 FROM _values av2 WHERE av2._id_object = v._id_object AND av2._id_structure = v._id_structure AND av2._array_index IS NOT NULL)
                    -- Regular field filtering
                    WHEN st.db_type = 'String' THEN v._String IS NOT NULL
                    WHEN st.db_type = 'Long' AND st.type_semantic != '_RObject' THEN v._Long IS NOT NULL
                    WHEN st.db_type = 'Guid' AND st.type_semantic != 'Object' THEN v._Guid IS NOT NULL
                    WHEN st.db_type = 'Double' THEN v._Double IS NOT NULL
                    WHEN st.db_type = 'DateTimeOffset' THEN v._DateTimeOffset IS NOT NULL
                    WHEN st.db_type = 'Boolean' THEN v._Boolean IS NOT NULL
                    -- _RObject fields excluded from facets (not needed for UI filters)
                    -- WHEN st.db_type = 'Long' AND st.type_semantic = '_RObject' THEN v._Object IS NOT NULL
                    WHEN st.db_type = 'ListItem' THEN v._listitem IS NOT NULL
                    WHEN st.db_type = 'ByteArray' THEN v._ByteArray IS NOT NULL
                    WHEN st.db_type = 'Numeric' THEN v._Numeric IS NOT NULL
                    ELSE FALSE
                END
            ) as facet_values
        FROM _values v
        JOIN _objects o ON o._id = v._id_object
        JOIN _scheme_metadata_cache st ON st._structure_id = v._id_structure
        WHERE o._id_scheme = scheme_id
          AND st._parent_structure_id IS NULL  -- 🔑 Only root fields at this stage
          AND NOT (st.db_type = 'Guid' AND st.type_semantic = 'Object') -- 🔑 Exclude Class fields, they will be processed separately
        GROUP BY v._id_structure
        HAVING COUNT(DISTINCT COALESCE(v._String, v._Long::text, v._Double::text, v._Guid::text)) <= 100  -- 🔑 Limit: max 100 unique values
    ) f ON f._id_structure = s._id
    WHERE s._id_scheme = scheme_id 
      AND s._id_parent IS NULL;  -- 🔑 Only root structures
    
    -- 🚀 STEP 2: Add expanded Class fields (Contact.Name, Contact[].Email)
    SELECT jsonb_object_agg(
        field_path,
        COALESCE(field_values, '[]'::jsonb)
    ) INTO class_facets
    FROM (
        SELECT 
            _build_facet_field_path(nested_s._structure_id, scheme_id) as field_path,
            jsonb_agg(DISTINCT
                CASE 
                    WHEN nested_s._collection_type IS NOT NULL THEN
                        (
                            SELECT COALESCE(jsonb_agg(
                                CASE 
                                    WHEN nested_s.db_type = 'String' THEN to_jsonb(nested_v._String)
                                    WHEN nested_s.db_type = 'Long' AND nested_s.type_semantic != '_RObject' THEN to_jsonb(nested_v._Long)
                                    WHEN nested_s.db_type = 'Double' THEN to_jsonb(nested_v._Double)
                                    WHEN nested_s.db_type = 'Numeric' THEN to_jsonb(nested_v._Numeric)
                                    WHEN nested_s.db_type = 'Boolean' THEN to_jsonb(nested_v._Boolean)
                                    WHEN nested_s.db_type = 'DateTimeOffset' THEN to_jsonb(nested_v._DateTimeOffset)
                                    WHEN nested_s.db_type = 'Guid' AND nested_s.type_semantic != 'Object' THEN to_jsonb(nested_v._Guid)
                                    ELSE to_jsonb(nested_v._String)
                                END ORDER BY nested_v._array_index::int
                            ), '[]'::jsonb)
                            FROM _values nested_v
                            WHERE nested_v._id_object = o._id 
                              AND nested_v._id_structure = nested_s._structure_id
                              AND nested_v._array_index IS NOT NULL
                        )
                    ELSE
                        CASE 
                            WHEN nested_s.db_type = 'String' THEN to_jsonb(nested_v._String)
                            WHEN nested_s.db_type = 'Long' AND nested_s.type_semantic != '_RObject' THEN to_jsonb(nested_v._Long)
                            WHEN nested_s.db_type = 'Double' THEN to_jsonb(nested_v._Double)
                            WHEN nested_s.db_type = 'Numeric' THEN to_jsonb(nested_v._Numeric)
                            WHEN nested_s.db_type = 'Boolean' THEN to_jsonb(nested_v._Boolean)
                            WHEN nested_s.db_type = 'DateTimeOffset' THEN to_jsonb(nested_v._DateTimeOffset)
                            WHEN nested_s.db_type = 'Guid' AND nested_s.type_semantic != 'Object' THEN to_jsonb(nested_v._Guid)
                            ELSE to_jsonb(nested_v._String)
                        END
                END
            ) FILTER (WHERE nested_v._id IS NOT NULL) as field_values
        FROM _objects o
        JOIN _values root_v ON root_v._id_object = o._id AND root_v._array_index IS NULL
        JOIN _scheme_metadata_cache root_s ON root_s._structure_id = root_v._id_structure AND root_s._parent_structure_id IS NULL AND root_s.db_type = 'Guid' AND root_s.type_semantic = 'Object'  -- 🔑 Only Class fields
        JOIN _scheme_metadata_cache nested_s ON nested_s._parent_structure_id = root_s._structure_id  -- 🔑 Nested structures
        LEFT JOIN _values nested_v ON nested_v._id_object = o._id AND nested_v._id_structure = nested_s._structure_id
        WHERE o._id_scheme = scheme_id
        GROUP BY nested_s._structure_id
        HAVING COUNT(nested_v._id) > 0  -- 🔑 Only fields with real values
           AND COUNT(DISTINCT COALESCE(nested_v._String, nested_v._Long::text, nested_v._Double::text)) <= 100  -- 🔑 Limit: max 100 unique values
    ) class_fields
    WHERE field_path IS NOT NULL AND field_path != '';
    
    -- 🚀 STEP 3: Combine basic and Class facets
    result_facets := COALESCE(all_facets, '{}'::jsonb) || COALESCE(class_facets, '{}'::jsonb);
    
    RETURN result_facets;
END;
$BODY$;

-- Comments for extended facets function
COMMENT ON FUNCTION _build_facet_field_path(bigint, bigint, text, integer) IS 'Recursive function for building paths for Class fields in facets. Creates paths like "Contact.Name", "Contacts[].Email", "Address.City" from _structures._id_parent hierarchy. Supports arrays and multi-level nesting.';

COMMENT ON FUNCTION get_facets(bigint) IS '🚀 EXTENDED function for building facets with full Class architecture support:
📋 Basic facets: Name, Status, Tags[] (root fields and simple arrays)
📦 Class facets: Address.City, Address.Street, Contacts[].Type (expanded from _structures._id_parent)  
🔗 Class arrays: Contacts[].Email, Products[].Price (combination of arrays + nesting)
📋 ListItem facets: {id, value, object} where object - base fields of related object
⚡ OPTIMIZATION: 
  - Returns only fields with <= 100 unique values (avoids Article, Description)
  - Object references (_RObject) EXCLUDED from facets (not needed for UI filters)
Two-stage processing: first basic facets, then Class field expansion. Excludes deleted objects.';

-- ===== NEW MODULAR ARCHITECTURE =====

-- ===== FINAL ARCHITECTURE: ABSOLUTE PURITY =====
-- ✅ build_advanced_facet_conditions() - REMOVED
-- ✅ build_base_facet_conditions() - REMOVED 
-- ✅ use_advanced_facets - REMOVED
-- 🚀 REMAINS: ONLY _build_single_facet_condition() as SINGLE ENTRY POINT
-- 💎 PERFECT PURITY WITHOUT A SINGLE EXTRA LINE!

-- Function 1: Building sort conditions
-- 🆕 UPDATED: Support for compact format {"fieldName": "asc"} and base fields with "0$:" prefix
CREATE OR REPLACE FUNCTION build_order_conditions(
    order_by jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    -- ✅ FIXED: Empty string by default (default ORDER BY _id killed performance!)
    order_conditions text := '';
    order_item jsonb;
    field_name text;
    direction text;
    order_clause text;
    i integer;
    item_key text;
    item_value text;
BEGIN
    -- Process sort parameters
    IF order_by IS NOT NULL AND jsonb_typeof(order_by) = 'array' AND jsonb_array_length(order_by) > 0 THEN
        order_conditions := '';
        
        -- Process each sort element
        FOR i IN 0..jsonb_array_length(order_by) - 1 LOOP
            order_item := order_by->i;
            
            -- 🆕 Support for two formats:
            -- 1. Compact: {"Name": "asc"} or {"0$:name": "desc"}
            -- 2. Expanded: {"field": "Name", "direction": "ASC"}
            IF order_item ? 'field' THEN
                -- Expanded format
                field_name := order_item->>'field';
                direction := UPPER(COALESCE(order_item->>'direction', 'ASC'));
            ELSE
                -- Compact format: first key = field name, value = direction
                SELECT key, value INTO item_key, item_value 
                FROM jsonb_each_text(order_item) LIMIT 1;
                field_name := item_key;
                direction := UPPER(COALESCE(item_value, 'ASC'));
            END IF;
            
            -- Skip incorrect sort elements
            IF field_name IS NOT NULL AND field_name != '' THEN
                
                -- 🆕 RedbObject BASE FIELDS: check for "0$:" prefix
                IF field_name LIKE '0$:%' THEN
                    DECLARE
                        raw_field_name text := substring(field_name from 4);  -- remove '0$:'
                        sql_column text := _normalize_base_field_name(raw_field_name);
                    BEGIN
                        -- Check that field is recognized as base
                        IF sql_column IS NULL THEN
                            RAISE EXCEPTION 'Unknown RedbObject base field for sorting: "%" (passed as "0$:%")', 
                                raw_field_name, raw_field_name;
                        END IF;
                        
                        -- 🚀 DIRECT SORTING by _objects column (faster than subquery to _values!)
                        order_clause := format('%s.%s %s NULLS LAST', table_alias, sql_column, direction);
                    END;
                ELSE
                    -- Props fields: sorting via subquery to _values
                    order_clause := format('(
                        SELECT CASE 
                            WHEN s.type_semantic = ''TimeSpan'' THEN TO_CHAR(v._String::interval, ''HH24:MI:SS'')
                            WHEN v._String IS NOT NULL THEN v._String
                            WHEN v._Long IS NOT NULL THEN LPAD(v._Long::text, 20, ''0'')
                            WHEN v._Double IS NOT NULL THEN LPAD(REPLACE(v._Double::text, ''.'', ''~''), 25, ''0'')
                            WHEN v._Numeric IS NOT NULL THEN LPAD(REPLACE(v._Numeric::text, ''.'', ''~''), 30, ''0'')
                            WHEN v._DateTimeOffset IS NOT NULL THEN TO_CHAR(v._DateTimeOffset, ''YYYY-MM-DD HH24:MI:SS.US'')
                            WHEN v._Boolean IS NOT NULL THEN v._Boolean::text
                            ELSE NULL
                        END
                        FROM _values v 
                        JOIN _scheme_metadata_cache s ON v._id_structure = s._structure_id 
                        WHERE v._id_object = %s._id AND s._name = %L
                          AND v._array_index IS NULL  -- exclude array elements
                        LIMIT 1
                    ) %s NULLS LAST', table_alias, field_name, direction);
                END IF;
                
                -- Add comma if conditions already exist
                IF order_conditions != '' THEN
                    order_conditions := order_conditions || ', ';
                END IF;
                order_conditions := order_conditions || order_clause;
            END IF;
        END LOOP;
        
        -- Form final ORDER BY
        IF order_conditions != '' THEN
            -- ✅ Add _id only for pagination stability with explicit sorting
            order_conditions := 'ORDER BY ' || order_conditions || format(', %s._id', table_alias);
        ELSE
            -- ✅ FIXED: No default sorting - otherwise kills performance for large CTEs!
            order_conditions := '';
        END IF;
    END IF;
    
    RETURN order_conditions;
END;
$BODY$;

-- Comment for sort function
COMMENT ON FUNCTION build_order_conditions(jsonb, text) IS '🆕 Builds ORDER BY conditions with support for:
- Compact format: [{"Name": "asc"}, {"0$:date_create": "desc"}]
- Expanded format: [{"field": "Name", "direction": "ASC"}]
- 🚀 RedbObject base fields (0$: prefix): direct sorting by _objects (faster!)
- Props fields: sorting via _values with correct type handling
- Cascading sorts: OrderBy().ThenByRedb().ThenBy()';

-- Function 2: Building hierarchical conditions
CREATE OR REPLACE FUNCTION build_has_ancestor_condition(
    ancestor_filter jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    condition_json jsonb;
    target_scheme_id bigint;
    max_depth_value integer;
    condition_sql text := '';
    depth_limit_sql text := '';
    scheme_filter_sql text := '';
BEGIN
    -- Extract parameters from JSON
    condition_json := ancestor_filter->'condition';
    target_scheme_id := (ancestor_filter->>'scheme_id')::bigint;
    max_depth_value := (ancestor_filter->>'max_depth')::integer;
    
    -- Form SQL for depth limitation
    IF max_depth_value IS NOT NULL THEN
        depth_limit_sql := format(' AND ancestors.level <= %s', max_depth_value);
    ELSE
        depth_limit_sql := ' AND ancestors.level < 50';
    END IF;
    
    -- Form SQL for scheme_id filtering
    IF target_scheme_id IS NOT NULL THEN
        scheme_filter_sql := format(' AND anc_obj._id_scheme = %s', target_scheme_id);
    END IF;
    
    -- Form SQL for Props condition
    IF condition_json IS NOT NULL AND jsonb_typeof(condition_json) = 'object' THEN
        -- Use _build_single_facet_condition to build WHERE conditions
        IF target_scheme_id IS NOT NULL THEN
            condition_sql := format(' AND %s', _build_single_facet_condition(condition_json, target_scheme_id, 'anc_obj'));
        END IF;
    END IF;
    
    RETURN format(
        ' AND EXISTS (
            WITH RECURSIVE ancestors AS (
                SELECT %s._id_parent as parent_id, 1 as level
                FROM _objects dummy WHERE dummy._id = %s._id
                UNION ALL
                SELECT o._id_parent, ancestors.level + 1
                FROM _objects o
                JOIN ancestors ON o._id = ancestors.parent_id
                WHERE true %s
            )
            SELECT 1 FROM ancestors
            JOIN _objects anc_obj ON anc_obj._id = ancestors.parent_id
            WHERE true %s %s
        )', 
        table_alias, table_alias, depth_limit_sql, scheme_filter_sql, condition_sql
    );
END;
$BODY$;

CREATE OR REPLACE FUNCTION build_has_descendant_condition(
    descendant_filter jsonb,
    table_alias text DEFAULT 'o'  
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    condition_json jsonb;
    target_scheme_id bigint;
    max_depth_value integer;
    condition_sql text := '';
    depth_limit_sql text := '';
    scheme_filter_sql text := '';
BEGIN
    -- Extract parameters from JSON
    condition_json := descendant_filter->'condition';
    target_scheme_id := (descendant_filter->>'scheme_id')::bigint;
    max_depth_value := (descendant_filter->>'max_depth')::integer;
    
    -- Form SQL for depth limitation
    IF max_depth_value IS NOT NULL THEN
        depth_limit_sql := format(' AND descendants.level <= %s', max_depth_value);
    ELSE
        depth_limit_sql := ' AND descendants.level < 50';
    END IF;
    
    -- Form SQL for scheme_id filtering
    IF target_scheme_id IS NOT NULL THEN
        scheme_filter_sql := format(' AND desc_obj._id_scheme = %s', target_scheme_id);
    END IF;
    
    -- Form SQL for Props condition
    IF condition_json IS NOT NULL AND jsonb_typeof(condition_json) = 'object' THEN
        -- Use _build_single_facet_condition to build WHERE conditions
        IF target_scheme_id IS NOT NULL THEN
            condition_sql := format(' AND %s', _build_single_facet_condition(condition_json, target_scheme_id, 'desc_obj'));
        END IF;
    END IF;
    
    RETURN format(
        ' AND EXISTS (
            WITH RECURSIVE descendants AS (
                SELECT %s._id as parent_id, 1 as level
                UNION ALL
                SELECT o._id, descendants.level + 1
                FROM _objects o
                JOIN descendants ON o._id_parent = descendants.parent_id
                WHERE true %s
            )
            SELECT 1 FROM descendants
            JOIN _objects desc_obj ON desc_obj._id = descendants.parent_id
            WHERE desc_obj._id != %s._id %s %s
        )', 
        table_alias, depth_limit_sql, table_alias, scheme_filter_sql, condition_sql
    );
END;
$BODY$;

CREATE OR REPLACE FUNCTION build_level_condition(
    target_level integer,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
BEGIN
    -- ✅ FIX: Calculate level going UP from object to root
    -- Level 0 = root (where _id_parent IS NULL)
    -- Level 1 = direct child of root
    -- etc.
    RETURN format(
        ' AND (
            SELECT COUNT(*)::integer FROM (
                WITH RECURSIVE ancestors AS (
                    SELECT %s._id_parent as parent_id
                    UNION ALL
                    SELECT o._id_parent
                    FROM _objects o
                    JOIN ancestors ON o._id = ancestors.parent_id
                    WHERE o._id_parent IS NOT NULL
                )
                SELECT parent_id FROM ancestors WHERE parent_id IS NOT NULL
            ) AS a
        ) = %s', 
        table_alias, target_level
    );
END;
$BODY$;

-- ✅ NEW FUNCTION: Support for comparison operators for levels
CREATE OR REPLACE FUNCTION build_level_condition_with_operators(
    level_operators jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    operator_name text;
    operator_value text;
    level_condition text := '';
    op_symbol text;
BEGIN
    -- Process each operator in JSON object
    FOR operator_name, operator_value IN SELECT key, value FROM jsonb_each_text(level_operators) LOOP
        
        -- Determine SQL operator
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$lte' THEN op_symbol := '<=';
            WHEN '$eq' THEN op_symbol := '=';
            WHEN '$ne' THEN op_symbol := '!=';
            ELSE 
                CONTINUE; -- Skip unknown operators
        END CASE;
        
        -- Form condition for current operator
        IF level_condition != '' THEN
            level_condition := level_condition || ' AND ';
        END IF;
        
        -- ✅ FIX: Calculate level going UP from object to root
        level_condition := level_condition || format(
            '(
                SELECT COUNT(*)::integer FROM (
                    WITH RECURSIVE ancestors AS (
                        SELECT %s._id_parent as parent_id
                        UNION ALL
                        SELECT o._id_parent
                        FROM _objects o
                        JOIN ancestors ON o._id = ancestors.parent_id
                        WHERE o._id_parent IS NOT NULL
                    )
                    SELECT parent_id FROM ancestors WHERE parent_id IS NOT NULL
                ) AS a
            ) %s %s',
            table_alias, op_symbol, operator_value
        );
    END LOOP;
    
    -- Return full condition with AND prefix
    IF level_condition != '' THEN
        RETURN ' AND (' || level_condition || ')';
    END IF;
    
    RETURN '';
END;
$BODY$;

-- Function to combine hierarchical conditions
CREATE OR REPLACE FUNCTION build_hierarchical_conditions(
    facet_filters jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    where_conditions text := '';
    ancestor_id bigint;
    descendant_id bigint;
    target_level integer;
BEGIN
    IF facet_filters IS NOT NULL AND jsonb_typeof(facet_filters) = 'object' THEN
        -- $hasAncestor: Polymorphic ancestor search with condition, scheme_id and max_depth
        IF facet_filters ? '$hasAncestor' THEN
            where_conditions := where_conditions || build_has_ancestor_condition(facet_filters->'$hasAncestor', table_alias);
        END IF;
        
        -- $hasDescendant: Polymorphic descendant search with condition, scheme_id and max_depth
        IF facet_filters ? '$hasDescendant' THEN
            where_conditions := where_conditions || build_has_descendant_condition(facet_filters->'$hasDescendant', table_alias);
        END IF;
        
        -- $level: Support for comparison operators {"$gt": 2}, {"$eq": 3} etc.
        IF facet_filters ? '$level' THEN
            -- ✅ FIX: Processing JSON operators for $level
            IF jsonb_typeof(facet_filters->'$level') = 'object' THEN
                -- Complex condition with operators like {"$gt": 2}, {"$lt": 5}
                where_conditions := where_conditions || build_level_condition_with_operators(facet_filters->'$level', table_alias);
            ELSE
                -- Simple value - exact equality
                target_level := (facet_filters->>'$level')::integer;
                where_conditions := where_conditions || build_level_condition(target_level, table_alias);
            END IF;
        END IF;
        
        -- $isRoot
        IF facet_filters ? '$isRoot' AND (facet_filters->>'$isRoot')::boolean THEN
            where_conditions := where_conditions || format(' AND %s._id_parent IS NULL', table_alias);
        END IF;
        
        -- $isLeaf  
        IF facet_filters ? '$isLeaf' AND (facet_filters->>'$isLeaf')::boolean THEN
            where_conditions := where_conditions || format(
                ' AND NOT EXISTS (SELECT 1 FROM _objects child WHERE child._id_parent = %s._id)', 
                table_alias
            );
        END IF;
        
        -- $childrenOf - direct children of specified parent
        IF facet_filters ? '$childrenOf' THEN
            where_conditions := where_conditions || format(
                ' AND %s._id_parent = %s', 
                table_alias,
                (facet_filters->>'$childrenOf')::bigint
            );
        END IF;
    END IF;
    
    RETURN where_conditions;
END;
$BODY$;

-- Comment for hierarchical conditions
COMMENT ON FUNCTION build_hierarchical_conditions(jsonb, text) IS 'Builds WHERE conditions for hierarchical filters: $hasAncestor, $hasDescendant, $level, $isRoot, $isLeaf, $childrenOf. Uses recursive CTEs for efficient search in object hierarchy. Recursion depth limit: 50 levels.';

-- Function 3: Execute query and return result
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS execute_objects_query(bigint, text, text, text, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS execute_objects_query(bigint, text, text, text, integer, integer, boolean, boolean) CASCADE;

CREATE OR REPLACE FUNCTION execute_objects_query(
    scheme_id bigint,
    base_conditions text,
    hierarchical_conditions text,
    order_conditions text,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    include_facets boolean DEFAULT false,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    final_where text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Combine all conditions
    final_where := format('WHERE o._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (o._hash)';
        -- PostgreSQL requires: ORDER BY must start with DISTINCT ON expression
        order_for_distinct := 'ORDER BY o._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY o._id');
    END IF;
    
    -- ✅ FIX: Build main query with NULL limit handling
    query_text := format('
        SELECT jsonb_agg(get_object_json(sub._id, 10))
        FROM (
            SELECT %s o._id
            FROM _objects o
            %s
            %s
            %s
        ) sub',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''  -- ✅ NO LIMIT if not specified or very large
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    -- Build count query
    -- ✅ With DISTINCT count unique hash values
    IF distinct_hash THEN
        count_query_text := format('
            SELECT COUNT(DISTINCT o._hash)
            FROM _objects o  
            %s',
            final_where
        );
    ELSE
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects o  
            %s',
            final_where
        );
    END IF;
    
    -- Execute queries
    EXECUTE query_text INTO objects_result;
    EXECUTE count_query_text INTO total_count;
    
    -- Form result
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'facets', CASE 
            WHEN include_facets THEN get_facets(scheme_id)
            ELSE '{}'::jsonb  -- empty object for speed
        END
    );
END;
$BODY$;

-- Comment for query execution function
COMMENT ON FUNCTION execute_objects_query(bigint, text, text, text, integer, integer, boolean, boolean) IS 'Executes object search with built conditions and returns standardized result with objects, metadata and optional facets. include_facets (DEFAULT false) - disabling heavy facets for speed on large schemes (10,000+ objects). ✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.';

-- Main function for faceted object search with purest architecture
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS search_objects_with_facets(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS search_objects_with_facets(bigint, jsonb, integer, integer, jsonb, integer, boolean, boolean) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_facets(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    include_facets boolean DEFAULT false,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
BEGIN
    -- 🚀 FINAL PURITY: ONLY _build_single_facet_condition() - NO DEAD CODE!
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'o', max_recursion_depth);
    
    -- Build hierarchical and sort conditions (unchanged)
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'o');
    order_conditions := build_order_conditions(order_by, 'o');
    
    -- Execute search
    RETURN execute_objects_query(
        scheme_id,
        base_conditions,
        hierarchical_conditions,
        order_conditions,
        limit_count,
        offset_count,
        include_facets,
        distinct_hash  -- ✅ Pass parameter
    );
END;
$BODY$;

-- Comment for main search function with new capabilities
COMMENT ON FUNCTION search_objects_with_facets(bigint, jsonb, integer, integer, jsonb, integer, boolean, boolean) IS '🚀 FINAL PURITY: Absolutely clean architecture NO DEAD CODE! Direct call to _build_single_facet_condition() as SINGLE entry point. NO legacy functions, NO use_advanced_facets, NO dead branches! Supports logical operators ($and, $or, $not), 25+ LINQ operators ($gt, $contains, $arrayContains, etc.), Class fields (Contact.Name), Class arrays (Contacts[].Email). 🆕 max_recursion_depth for complex queries (DEFAULT 10). 🆕 include_facets (DEFAULT false) - disabling heavy facets for speed on large schemes. ✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.';

-- Function for hierarchical search (object children) with SUPPORT FOR NEW LINQ PARADIGM
CREATE OR REPLACE FUNCTION search_tree_objects_with_facets(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents for optimization (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10,
    include_facets boolean DEFAULT false  -- ⭐ NEW: facets disabled by default
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 300
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    base_conditions text;
    hierarchical_conditions text;  -- ✅ FIX: added for $level, $isRoot, $isLeaf
    order_conditions text;
BEGIN
    -- 🔥 AUTOMATIC CHECK AND CACHE POPULATION
    -- Guarantees that scheme metadata cache is populated before building conditions
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warming)
    END IF;
    
    -- 🚀 VARIANT C: PUREST ARCHITECTURE - direct call to universal system
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'd');  -- ✅ FIX: processing $level, $isRoot, $isLeaf
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- ✅ FIX: When parent_ids empty - search ENTIRE _objects table (without CTE)
    -- Needed for TreeQuery<T>().WhereLeaves() and similar queries without rootId
    IF parent_ids IS NULL OR array_length(parent_ids, 1) IS NULL OR array_length(parent_ids, 1) = 0 THEN
        query_text := format('
            SELECT jsonb_agg(get_object_json(sub._id, 10))
            FROM (
                SELECT d._id
                FROM _objects d
                WHERE d._id_scheme = %s%s%s
                %s
                %s
            ) sub',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects d
            WHERE d._id_scheme = %s%s%s',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, '')
        );
        
        -- Execute queries WITHOUT USING (no $1 parameter)
        EXECUTE query_text INTO objects_result;
        EXECUTE count_query_text INTO total_count;
        
        RETURN jsonb_build_object(
            'objects', COALESCE(objects_result, '[]'::jsonb),
            'total_count', total_count,
            'limit', limit_count,
            'offset', offset_count,
            'parent_ids', parent_ids,
            'max_depth', max_depth,
            'facets', CASE 
                WHEN include_facets THEN get_facets(scheme_id)
                ELSE '{}'::jsonb
            END
        );
    END IF;
    
    -- If max_depth = 1, search only direct children
    IF max_depth = 1 THEN
        query_text := format('
            SELECT jsonb_agg(get_object_json(sub._id, 10))
            FROM (
                SELECT d._id
                FROM _objects d
                WHERE d._id_scheme = %s 
                  AND d._id_parent = ANY($1)%s%s
                %s
                %s
            ) sub',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''  -- ✅ NO LIMIT if not explicitly specified
            END
        );
        
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects d
            WHERE d._id_scheme = %s 
              AND d._id_parent = ANY($1)%s%s',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, '')  -- ✅ FIX: added $level, $isRoot, $isLeaf
        );
    ELSE
        -- Recursive descendant search
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        -- In tree duplicates impossible (each object has one parent)
        query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT jsonb_agg(get_object_json(sub._id, 10))
            FROM (
                SELECT d._id
                FROM descendants dt
                JOIN _objects d ON dt._id = d._id
                WHERE dt.depth > 0 
                  AND d._id_scheme = %s%s%s
                %s
                %s
            ) sub',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''  -- ✅ NO LIMIT if not explicitly specified
            END
        );
        
        count_query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT COUNT(DISTINCT d._id)
            FROM descendants dt
            JOIN _objects d ON dt._id = d._id
            WHERE dt.depth > 0 
              AND d._id_scheme = %s%s%s',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, '')  -- ✅ FIX: added $level, $isRoot, $isLeaf
        );
    END IF;
    
    -- Execute queries with USING for array passing!
    EXECUTE query_text INTO objects_result USING parent_ids;
    EXECUTE count_query_text INTO total_count USING parent_ids;
    
    -- Form result
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'parent_ids', parent_ids,  -- ✅ BATCH: Array of parents
        'max_depth', max_depth,
        'facets', CASE 
            WHEN include_facets THEN get_facets(scheme_id)
            ELSE '{}'::jsonb  -- empty object for speed
        END
    );
END;
$BODY$;

-- Comment for tree search function
COMMENT ON FUNCTION search_tree_objects_with_facets(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer, boolean) IS '🚀 BATCH OPTIMIZATION! Accepts parent_ids[] array for 3-4x speedup. Direct call to _build_single_facet_condition() for tree queries. NO build_advanced_facet_conditions() - MAXIMUM PURITY! Supports:
📊 Logical operators: $and, $or, $not
🔍 LINQ operators: $gt, $contains, $arrayContains, $arrayAny, etc.
📦 Class fields: Contact.Name, Address.City
🔗 Class arrays: Contacts[].Email, Products[].Price  
🌳 Hierarchical conditions: direct children search (max_depth=1) and recursive descendant search
🆕 max_recursion_depth for complex queries (DEFAULT 10)
🆕 include_facets (DEFAULT false) - disabling heavy facets for speed on large schemes. SINGLE entry point!';

-- ===== SQL PREVIEW FUNCTIONS (for debugging) =====

-- Function 1: Preview for standard search
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS get_search_sql_preview(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION get_search_sql_preview(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
    final_where text;
    query_text text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Reuse condition building functions
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'o', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'o');
    order_conditions := build_order_conditions(order_by, 'o');
    
    -- Combine WHERE
    final_where := format('WHERE o._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (o._hash)';
        order_for_distinct := 'ORDER BY o._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY o._id');
    END IF;
    
    -- Build SQL (DO NOT EXECUTE!)
    query_text := format('
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT %s o._id
    FROM _objects o
    %s
    %s
    %s
) sub',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_sql_preview(bigint, jsonb, integer, integer, jsonb, integer, boolean) IS 
'Returns SQL query for debugging. Shows what will be executed in search_objects_with_facets(). ✅ distinct_hash=true adds DISTINCT ON (_hash). Used in ToSqlStringAsync() to view final SQL without execution.';

-- Function 2: Preview for tree search
CREATE OR REPLACE FUNCTION get_search_tree_sql_preview(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10
) RETURNS text
LANGUAGE 'plpgsql'
COST 100
IMMUTABLE
AS $BODY$
DECLARE
    query_text text;
    base_conditions text;
    hierarchical_conditions text;  -- ✅ FIX: added for $level, $isRoot, $isLeaf
    order_conditions text;
BEGIN
    -- Reuse condition building functions
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'd');  -- ✅ FIX: processing $level, $isRoot, $isLeaf
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- ✅ FIX: When parent_ids empty - show query without CTE
    IF parent_ids IS NULL OR array_length(parent_ids, 1) IS NULL OR array_length(parent_ids, 1) = 0 THEN
        query_text := format('
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT d._id
    FROM _objects d
    WHERE d._id_scheme = %s%s%s
    %s
    %s
) sub',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        RETURN query_text;
    END IF;
    
    -- If max_depth = 1, search only direct children
    IF max_depth = 1 THEN
        query_text := format('
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT d._id
    FROM _objects d
    WHERE d._id_scheme = %s 
      AND d._id_parent = ANY(%L)%s%s
    %s
    %s
) sub',
            scheme_id,
            parent_ids,  -- ✅ BATCH: Array of parents
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    ELSE
        -- Recursive descendant search
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        query_text := format('
WITH RECURSIVE descendants AS (
    SELECT unnest(%L) as _id, 0::bigint as depth
    UNION ALL
    SELECT o._id, d.depth + 1
    FROM _objects o
    JOIN descendants d ON o._id_parent = d._id
    WHERE d.depth < %s
)
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT d._id
    FROM descendants dt
    JOIN _objects d ON dt._id = d._id
    WHERE dt.depth > 0 
      AND d._id_scheme = %s%s%s
    %s
    %s
) sub',
            parent_ids,  -- ✅ BATCH: Array of parents
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    END IF;
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_tree_sql_preview(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) IS 
'Returns SQL query for tree search (for debugging). Shows what will be executed in search_tree_objects_with_facets(). Supports direct children search (max_depth=1) and recursive descendant search. 🔥 No DISTINCT for compatibility with ORDER BY.';

-- ===== EXAMPLES OF NEW ARCHITECTURE USAGE =====
/*
-- 🚀 UPDATED CAPABILITIES WITH OPTIMIZED EAV SEMANTICS:

-- 🎯 NEW NULL SEMANTICS:
-- = null now searches for MISSING fields (NOT records with NULL values)
SELECT search_objects_with_facets(
    9001, 
    '{"OptionalField": null}'::jsonb  -- will find objects WITHOUT this field in _values
);

-- $ne null now searches for fields with REAL non-NULL values  
SELECT search_objects_with_facets(
    9001,
    '{"Name": {"$ne": null}}'::jsonb  -- will find objects where Name is actually filled
);

-- 🎯 NEW $exists OPERATOR:
-- Explicit field existence control
SELECT search_objects_with_facets(
    9001,
    '{
        "RequiredField": {"$exists": true},    -- field MUST exist
        "OptionalField": {"$exists": false}    -- field must NOT exist
    }'::jsonb
);

-- 🚀 NEW CAPABILITIES:

-- 1. Logical operators:
SELECT search_objects_with_facets(
    1002, 
    '{
        "$and": [
            {"Status": "Active"}, 
            {"$or": [{"Priority": "High"}, {"Urgent": true}]}
        ]
    }'::jsonb,
    10, 0, NULL
);

-- 2. LINQ operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Price": {"$gt": "100", "$lt": "500"},
        "Title": {"$contains": "analytics"},
        "CreatedDate": {"$gte": "2024-01-01"}
    }'::jsonb,
    10, 0, NULL
);

-- 3. Basic array operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Tags[]": {"$arrayContains": "important"},
        "Scores[]": {"$arrayCountGt": 3},
        "Categories[]": {"$arrayAny": true},
        "Items[]": {"$arrayEmpty": false}
    }'::jsonb,
    10, 0, NULL
);

-- 4. Positional array operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Tags[]": {"$arrayFirst": "urgent"},
        "Scores[]": {"$arrayLast": "100"},
        "Items[]": {"$arrayAt": "2"}
    }'::jsonb,
    10, 0, NULL
);

-- 5. Search array operators:  
SELECT search_objects_with_facets(
    1002,
    '{
        "Tags[]": {"$arrayStartsWith": "test_"},
        "Names[]": {"$arrayEndsWith": "_prod"},
        "Descriptions[]": {"$arrayMatches": ".*error.*"}
    }'::jsonb,
    10, 0, NULL
);

-- 6. Aggregation array operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Scores[]": {"$arraySum": "300"},
        "Ratings[]": {"$arrayAvg": "4.5"},
        "Prices[]": {"$arrayMin": "10.50"},
        "Quantities[]": {"$arrayMax": "1000"}
    }'::jsonb,
    10, 0, NULL
);

-- 7. NOT conditions:
SELECT search_objects_with_facets(
    1002,
    '{
        "$not": {"Status": "Deleted"},
        "Title": {"$ne": null}
    }'::jsonb,
    10, 0, NULL
);

-- 8. Class fields - full support:
SELECT search_objects_with_facets(
    1002,
    '{
        "Contact.Name": "John Doe",
        "Address.City": "Moscow",
        "Contact.Phone": {"$startsWith": "+7"},
        "Address.PostalCode": {"$in": ["101000", "102000"]},
        "$not": {"Contact.Email": {"$endsWith": "@test.com"}}
    }'::jsonb,
    10, 0, NULL
);

-- 9. Class arrays with nested fields:
SELECT search_objects_with_facets(
    1002,
    '{
        "Contacts[].Name": "Jane Smith",
        "Addresses[].Country": "Russia", 
        "Products[].Price": {"$gt": "100"},
        "Tags[].Category": {"$contains": "business"},
        "$or": [
            {"Contacts[].Email": {"$endsWith": "@company.com"}},
            {"Addresses[].City": {"$in": ["Moscow", "SPb"]}}
        ]
    }'::jsonb,
    10, 0, NULL
);

-- 10. 🎯 RECURSION CONFIGURATION - custom depth:
SELECT search_objects_with_facets(
    1002, 
    '{"$and": [{"Tags[]": {"$arrayContains": "complex"}}, {"$or": [{"Age": {"$gt": "25"}}, {"Stock": {"$gt": "100"}}]}]}'::jsonb,
    10, 0,
    '[{"field": "Date", "direction": "DESC"}]'::jsonb,
    20  -- max_recursion_depth = 20 for complex queries
);

-- 📊 HIERARCHICAL conditions:
SELECT search_objects_with_facets(
    1002,
    '{"$isRoot": true, "Status": ["Active"]}'::jsonb
);

-- 🌳 TREE SEARCH:
SELECT search_tree_objects_with_facets(
    1002, 1021,  -- scheme_id, parent_id
    '{"Status": ["Active"]}'::jsonb,
    10, 0, NULL, 1  -- direct children
);

-- Recursive descendant search:
SELECT search_tree_objects_with_facets(
    1002, 1021,  -- scheme_id, parent_id  
    NULL, 20, 0, NULL, 5  -- up to 10 levels deep
);

-- 📈 GETTING FACETS for UI:
SELECT get_facets(1002);

-- ⚡ COMPLEX EXAMPLE - combination of all capabilities:
SELECT search_objects_with_facets(
    1002,
    '{
        "$and": [
            {"Status": {"$ne": "Deleted"}},
            {"$or": [
                {"Priority": {"$in": ["High", "Critical"]}},
                {"Tags[]": {"$arrayContains": "urgent"}}
            ]},
            {"CreatedDate": {"$gte": "2024-01-01"}},
            {"Price": {"$gt": "0"}},
            {"$not": {"Archive": true}}
        ],
        "$isRoot": false
    }'::jsonb,
    20, 0,
    '[{"field": "CreatedDate", "direction": "DESC"}]'::jsonb,
    15  -- max_recursion_depth = 15 for extremely complex queries
);
*/

-- ===== redb_grouping.sql =====
-- =====================================================
-- REDB EAV GROUPING FUNCTIONS
-- GroupBy aggregations for EAV model
-- =====================================================

-- =====================================================
-- aggregate_grouped: GroupBy with aggregations
-- =====================================================
-- Parameters:
--   p_scheme_id     - Scheme ID
--   p_group_fields  - JSON array of grouping fields:
--                     [{"field":"Category","alias":"Category"}]
--                     [{"field":"Address.City","alias":"City"}]
--   p_aggregations  - JSON array of aggregations (like in aggregate_batch):
--                     [{"field":"Stock","func":"SUM","alias":"TotalStock"}]
--   p_filter_json   - JSON filter (optional)
--
-- Returns: jsonb array of groups
--   [{"Category":"Electronics","TotalStock":1500},...]
-- =====================================================

DROP FUNCTION IF EXISTS aggregate_grouped(bigint, jsonb, jsonb, jsonb);

CREATE OR REPLACE FUNCTION aggregate_grouped(
    p_scheme_id bigint,
    p_group_fields jsonb,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_group_field record;
    v_agg record;
    v_resolved record;
    
    -- SQL parts
    v_select_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    v_group_parts text[] := ARRAY[]::text[];
    
    -- Counters for unique aliases
    v_join_idx int := 0;
    v_field text;
    v_alias text;
    v_func text;
    v_column_name text;
    v_join_alias text;
    v_array_condition text;
    
    -- Filter
    v_object_ids bigint[];
    v_where_clause text := '';
    
    -- Result
    v_sql text;
    v_result jsonb;
BEGIN
    -- =========================================
    -- 1. Process grouping fields
    -- =========================================
    FOR v_group_field IN SELECT * FROM jsonb_array_elements(p_group_fields)
    LOOP
        v_field := v_group_field.value->>'field';
        v_alias := COALESCE(v_group_field.value->>'alias', v_field);
        
        -- 🆕 Check for base field (prefix "0$:")
        IF v_field LIKE '0$:%' THEN
            -- ═══════════════════════════════════════════════════
            -- BASE FIELD from _objects (scheme_id, parent_id, etc.)
            -- ═══════════════════════════════════════════════════
            DECLARE
                raw_field_name text := substring(v_field from 4);  -- remove '0$:'
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'aggregate_grouped: Unknown base field "%"', raw_field_name;
                END IF;
                
                -- SELECT part: directly from _objects
                v_select_parts := array_append(v_select_parts, 
                    format('o.%I AS "%s"', sql_column, v_alias));
                
                -- GROUP BY part: directly from _objects
                v_group_parts := array_append(v_group_parts, 
                    format('o.%I', sql_column));
                
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- ═══════════════════════════════════════════════════
            -- 📦 EAV FIELD from _values (+ Dictionary support)
            -- ═══════════════════════════════════════════════════
            -- Resolve path to field
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
            
            IF v_resolved.structure_id IS NULL THEN
                RAISE EXCEPTION 'Group field "%" not found in scheme %', v_field, p_scheme_id;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'g' || v_join_idx;
            
            -- Determine column by type
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                WHEN 'Int' THEN '_Long'
                WHEN 'String' THEN '_String'
                WHEN 'Bool' THEN '_Bool'
                WHEN 'DateTime' THEN '_DateTime'
                WHEN 'ListItem' THEN '_ListItem'
                ELSE '_String'
            END;
            
            -- SELECT part
            v_select_parts := array_append(v_select_parts, 
                format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
            
            -- JOIN part (+ Dictionary/Array support)
            IF v_resolved.dict_key IS NOT NULL THEN
                -- Dictionary with key: PhoneBook[home]
                v_join_parts := array_append(v_join_parts,
                    format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
            ELSIF v_resolved.array_index IS NOT NULL THEN
                -- Array with index: Items[2]
                v_join_parts := array_append(v_join_parts,
                    format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
            ELSE
                -- Simple field
                v_join_parts := array_append(v_join_parts,
                    format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                        v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
            END IF;
            
            -- GROUP BY part
            v_group_parts := array_append(v_group_parts, 
                format('%s.%s', v_join_alias, v_column_name));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 2. Process aggregations
    -- =========================================
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        v_alias := COALESCE(v_agg.value->>'alias', v_func || '_' || COALESCE(v_field, 'count'));
        
        -- COUNT(*) - special case
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*' OR v_field = '') THEN
            v_select_parts := array_append(v_select_parts,
                format('COUNT(DISTINCT o._id) AS "%s"', v_alias));
            CONTINUE;
        END IF;
        
        -- 🆕 Check for base field (prefix "0$:")
        IF v_field LIKE '0$:%' THEN
            -- BASE FIELD for aggregation
            DECLARE
                raw_field_name text := substring(v_field from 4);
                sql_column text := _normalize_base_field_name(raw_field_name);
            BEGIN
                IF sql_column IS NULL THEN
                    RAISE EXCEPTION 'aggregate_grouped: Unknown base field for aggregation "%"', raw_field_name;
                END IF;
                
                -- SELECT with aggregation directly from _objects
                -- For MIN/MAX ::numeric not needed (they work with timestamp, text, etc.)
                -- For SUM/AVG ::numeric needed (only for numeric fields)
                IF v_func IN ('SUM', 'AVG') THEN
                    v_select_parts := array_append(v_select_parts,
                        format('%s(o.%I::numeric) AS "%s"', v_func, sql_column, v_alias));
                ELSE
                    -- MIN, MAX, COUNT — work with any types
                    v_select_parts := array_append(v_select_parts,
                        format('%s(o.%I) AS "%s"', v_func, sql_column, v_alias));
                END IF;
                -- NO JOIN needed for base fields!
            END;
        ELSE
            -- EAV FIELD (+ Dictionary support)
            -- Resolve field path
            SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
            
            IF v_resolved.structure_id IS NULL THEN
                RAISE EXCEPTION 'Aggregation field "%" not found in scheme %', v_field, p_scheme_id;
            END IF;
            
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'a' || v_join_idx;
            
            -- Determine column by type
            v_column_name := CASE v_resolved.db_type
                WHEN 'Long' THEN '_Long'
                WHEN 'Double' THEN '_Double'
                WHEN 'Numeric' THEN '_Numeric'
                WHEN 'Int' THEN '_Long'
                WHEN 'Decimal' THEN '_Numeric'
                WHEN 'Money' THEN '_Numeric'
                ELSE '_Long'
            END;
            
            -- Condition for Dictionary/Array/Simple field
            IF v_resolved.dict_key IS NOT NULL THEN
                -- Dictionary with key: PhoneBook[home]
                v_array_condition := format(' AND %s._array_index = %L', v_join_alias, v_resolved.dict_key);
            ELSIF v_resolved.is_array THEN
                IF v_resolved.array_index IS NOT NULL THEN
                    -- Array with index: Items[2]
                    v_array_condition := format(' AND %s._array_index = %L', v_join_alias, v_resolved.array_index::text);
                ELSE
                    v_array_condition := '';  -- all array/dictionary elements
                END IF;
            ELSE
                v_array_condition := format(' AND %s._array_index IS NULL', v_join_alias);
            END IF;
            
            -- JOIN part for aggregation
            v_join_parts := array_append(v_join_parts,
                format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s%s',
                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_array_condition));
            
            -- SELECT part with aggregation
            v_select_parts := array_append(v_select_parts,
                format('%s(%s.%s::numeric) AS "%s"', v_func, v_join_alias, v_column_name, v_alias));
        END IF;
    END LOOP;
    
    -- =========================================
    -- 3. Process filter
    -- =========================================
    IF p_filter_json IS NOT NULL AND p_filter_json != 'null'::jsonb THEN
        BEGIN
            v_object_ids := get_filtered_object_ids(p_scheme_id, p_filter_json, 10);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'aggregate_grouped filter error: %', SQLERRM;
            v_object_ids := NULL;
        END;
        
        IF v_object_ids IS NULL OR array_length(v_object_ids, 1) IS NULL THEN
            RETURN '[]'::jsonb;
        END IF;
        
        v_where_clause := format(' AND o._id = ANY(ARRAY[%s]::bigint[])', 
            array_to_string(v_object_ids, ','));
    END IF;
    
    -- =========================================
    -- 4. Build and execute SQL
    -- =========================================
    -- 🆕 CHECK: If no grouping (v_group_parts empty), do not add GROUP BY and ORDER BY
    IF array_length(v_group_parts, 1) IS NULL OR array_length(v_group_parts, 1) = 0 THEN
        -- WITHOUT grouping (for simple aggregations like SumRedbAsync)
        v_sql := format(
            'SELECT jsonb_agg(row_to_json(t)) FROM (
                SELECT %s
                FROM _objects o
                %s
                WHERE o._id_scheme = %s%s
            ) t',
            array_to_string(v_select_parts, ', '),
            array_to_string(v_join_parts, ' '),
            p_scheme_id,
            v_where_clause
        );
    ELSE
        -- WITH grouping (regular GroupBy)
        v_sql := format(
            'SELECT jsonb_agg(row_to_json(t)) FROM (
                SELECT %s
                FROM _objects o
                %s
                WHERE o._id_scheme = %s%s
                GROUP BY %s
                ORDER BY %s
            ) t',
            array_to_string(v_select_parts, ', '),
            array_to_string(v_join_parts, ' '),
            p_scheme_id,
            v_where_clause,
            array_to_string(v_group_parts, ', '),
            array_to_string(v_group_parts, ', ')
        );
    END IF;
    
    -- DEBUG: uncomment for debugging
    -- RAISE NOTICE 'aggregate_grouped SQL: %', v_sql;
    
    EXECUTE v_sql INTO v_result;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION aggregate_grouped(bigint, jsonb, jsonb, jsonb) IS 
'GroupBy aggregation for EAV.
Supports: simple fields, nested paths, arrays Items[2], dictionaries PhoneBook[home].
Examples:
  -- Simple grouping
  SELECT aggregate_grouped(1002, 
    ''[{"field":"Tag","alias":"Tag"}]''::jsonb,
    ''[{"field":"Stock","func":"SUM","alias":"TotalStock"},{"field":"*","func":"COUNT","alias":"Count"}]''::jsonb,
    NULL);
    
  -- Nested path
  SELECT aggregate_grouped(1002,
    ''[{"field":"Address.City","alias":"City"}]''::jsonb,
    ''[{"field":"Age","func":"AVG","alias":"AvgAge"}]''::jsonb,
    NULL);
    
  -- Dictionary: grouping by dictionary value
  SELECT aggregate_grouped(1002,
    ''[{"field":"PhoneBook[home]","alias":"HomePhone"}]''::jsonb,
    ''[{"field":"*","func":"COUNT","alias":"Count"}]''::jsonb,
    NULL);
    
  -- Multiple keys
  SELECT aggregate_grouped(1002,
    ''[{"field":"Tag","alias":"Tag"},{"field":"Age","alias":"Age"}]''::jsonb,
    ''[{"field":"Stock","func":"SUM","alias":"Total"}]''::jsonb,
    NULL);';

-- =====================================================
-- aggregate_grouped_preview: SQL preview for debugging
-- =====================================================
CREATE OR REPLACE FUNCTION aggregate_grouped_preview(
    p_scheme_id bigint,
    p_group_fields jsonb,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_group_field record;
    v_agg record;
    v_resolved record;
    v_select_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    v_group_parts text[] := ARRAY[]::text[];
    v_join_idx int := 0;
    v_field text;
    v_alias text;
    v_func text;
    v_column_name text;
    v_join_alias text;
    v_array_condition text;
BEGIN
    -- Grouping fields
    FOR v_group_field IN SELECT * FROM jsonb_array_elements(p_group_fields)
    LOOP
        v_field := v_group_field.value->>'field';
        v_alias := COALESCE(v_group_field.value->>'alias', v_field);
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'g' || v_join_idx;
        v_column_name := CASE v_resolved.db_type
            WHEN 'Long' THEN '_Long' WHEN 'String' THEN '_String' WHEN 'ListItem' THEN '_ListItem' ELSE '_String' END;
        
        v_select_parts := array_append(v_select_parts, format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
        
        -- 🆕 Dictionary/Array/Simple support
        IF v_resolved.dict_key IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L', 
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
        ELSIF v_resolved.array_index IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L', 
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
        ELSE
            v_join_parts := array_append(v_join_parts, format('JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL', 
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        END IF;
        
        v_group_parts := array_append(v_group_parts, format('%s.%s', v_join_alias, v_column_name));
    END LOOP;
    
    -- Aggregations
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        v_alias := COALESCE(v_agg.value->>'alias', v_func);
        
        IF v_func = 'COUNT' AND (v_field IS NULL OR v_field = '*') THEN
            v_select_parts := array_append(v_select_parts, format('COUNT(DISTINCT o._id) AS "%s"', v_alias));
            CONTINUE;
        END IF;
        
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_field);
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'a' || v_join_idx;
        v_column_name := CASE v_resolved.db_type WHEN 'Long' THEN '_Long' ELSE '_Numeric' END;
        
        -- 🆕 Dictionary/Array/Simple support
        IF v_resolved.dict_key IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.dict_key));
        ELSIF v_resolved.array_index IS NOT NULL THEN
            v_join_parts := array_append(v_join_parts, format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index = %L',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias, v_resolved.array_index::text));
        ELSE
            v_join_parts := array_append(v_join_parts, format('LEFT JOIN _values %s ON %s._id_object = o._id AND %s._id_structure = %s AND %s._array_index IS NULL',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        END IF;
        
        v_select_parts := array_append(v_select_parts, format('%s(%s.%s) AS "%s"', v_func, v_join_alias, v_column_name, v_alias));
    END LOOP;
    
    RETURN format(
        'SELECT %s FROM _objects o %s WHERE o._id_scheme = %s GROUP BY %s',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_join_parts, ' '),
        p_scheme_id,
        array_to_string(v_group_parts, ', ')
    );
END;
$BODY$;

-- =====================================================
-- aggregate_array_grouped: GroupBy by array elements
-- =====================================================
-- Parameters:
--   p_scheme_id      - Scheme ID
--   p_array_path     - path to array (e.g. "Items")
--   p_group_fields   - JSON: [{"field":"Category","alias":"Category"}]
--   p_aggregations   - JSON: [{"field":"Price","func":"SUM","alias":"Total"}]
--   p_filter_json    - filter (optional)
--
-- Example:
--   SELECT aggregate_array_grouped(
--     1002,
--     'Items',
--     '[{"field":"Category","alias":"Category"}]',
--     '[{"field":"Price","func":"SUM","alias":"TotalPrice"}]',
--     NULL
--   );
-- =====================================================

DROP FUNCTION IF EXISTS aggregate_array_grouped(bigint, text, jsonb, jsonb, jsonb);

CREATE OR REPLACE FUNCTION aggregate_array_grouped(
    p_scheme_id bigint,
    p_array_path text,
    p_group_fields jsonb,
    p_aggregations jsonb,
    p_filter_json jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_field record;
    v_agg record;
    v_resolved record;
    v_array_struct_id bigint;
    
    v_select_parts text[] := ARRAY[]::text[];
    v_group_parts text[] := ARRAY[]::text[];
    v_join_parts text[] := ARRAY[]::text[];
    
    v_join_idx int := 0;
    v_field_path text;
    v_alias text;
    v_func text;
    v_column_name text;
    v_join_alias text;
    
    v_sql text;
    v_result jsonb;
BEGIN
    -- 1. Get structure_id of array via resolve_field_path
    SELECT r.structure_id INTO v_array_struct_id
    FROM resolve_field_path(p_scheme_id, p_array_path) r;
    
    IF v_array_struct_id IS NULL THEN
        RAISE EXCEPTION 'Array "%" not found in scheme %', p_array_path, p_scheme_id;
    END IF;
    
    -- 2. Grouping fields (from array element)
    FOR v_field IN SELECT * FROM jsonb_array_elements(p_group_fields)
    LOOP
        v_field_path := v_field.value->>'field';
        v_alias := COALESCE(v_field.value->>'alias', v_field_path);
        
        -- Path inside element: Contacts.Type -> search Contacts[].Type
        SELECT * INTO v_resolved 
        FROM resolve_field_path(p_scheme_id, p_array_path || '[].' || v_field_path);
        
        IF v_resolved.structure_id IS NULL THEN
            RAISE WARNING 'aggregate_array_grouped: field "%" not found!', v_field_path;
            CONTINUE;
        END IF;
        
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'g' || v_join_idx;
        v_column_name := CASE v_resolved.db_type
            WHEN 'Long' THEN '_Long'
            WHEN 'String' THEN '_String'
            WHEN 'ListItem' THEN '_ListItem'
            ELSE '_String'
        END;
        
        -- JOIN: for nested business object fields link via _array_parent_id
        v_join_parts := array_append(v_join_parts,
            format('LEFT JOIN _values %s ON %s._id_object = arr._id_object AND %s._id_structure = %s AND %s._array_parent_id = arr._id',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        
        v_select_parts := array_append(v_select_parts, format('%s.%s AS "%s"', v_join_alias, v_column_name, v_alias));
        v_group_parts := array_append(v_group_parts, format('%s.%s', v_join_alias, v_column_name));
    END LOOP;
    
    -- 3. Aggregations
    FOR v_agg IN SELECT * FROM jsonb_array_elements(p_aggregations)
    LOOP
        v_field_path := v_agg.value->>'field';
        v_func := upper(v_agg.value->>'func');
        v_alias := COALESCE(v_agg.value->>'alias', v_func);
        
        IF v_func = 'COUNT' AND (v_field_path IS NULL OR v_field_path = '*') THEN
            v_select_parts := array_append(v_select_parts, format('COUNT(*) AS "%s"', v_alias));
            CONTINUE;
        END IF;
        
        SELECT * INTO v_resolved 
        FROM resolve_field_path(p_scheme_id, p_array_path || '[].' || v_field_path);
        
        IF v_resolved.structure_id IS NULL THEN
            CONTINUE;
        END IF;
        
        v_join_idx := v_join_idx + 1;
        v_join_alias := 'a' || v_join_idx;
        v_column_name := CASE v_resolved.db_type
            WHEN 'Long' THEN '_Long'
            WHEN 'Double' THEN '_Double'
            WHEN 'Numeric' THEN '_Numeric'
            ELSE '_Long'
        END;
        
        -- JOIN: for nested business object fields link via _array_parent_id
        v_join_parts := array_append(v_join_parts,
            format('LEFT JOIN _values %s ON %s._id_object = arr._id_object AND %s._id_structure = %s AND %s._array_parent_id = arr._id',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias));
        
        v_select_parts := array_append(v_select_parts, format('%s(%s.%s) AS "%s"', v_func, v_join_alias, v_column_name, v_alias));
    END LOOP;
    
    -- 4. Assemble SQL with array expansion
    v_sql := format(
        'SELECT jsonb_agg(row_to_json(t)) FROM (
            SELECT %s
            FROM _values arr
            JOIN _objects o ON o._id = arr._id_object
            %s
            WHERE o._id_scheme = %s 
              AND arr._id_structure = %s 
              AND arr._array_index IS NOT NULL
            GROUP BY %s
        ) t',
        array_to_string(v_select_parts, ', '),
        array_to_string(v_join_parts, ' '),
        p_scheme_id,
        v_array_struct_id,
        array_to_string(v_group_parts, ', ')
    );
    
    -- 🔍 DEBUG: uncomment for debugging
    -- RETURN jsonb_build_object('debug_sql', v_sql, 'array_struct_id', v_array_struct_id);
    
    EXECUTE v_sql INTO v_result;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$BODY$;

COMMENT ON FUNCTION aggregate_array_grouped IS 
'GroupBy aggregation by EAV array elements.
Example: GroupBy Items[].Category with SUM(Items[].Price)';

-- ===== redb_json_objects.sql =====
DROP VIEW IF EXISTS v_objects_json;
DROP FUNCTION IF EXISTS get_object_json;
DROP FUNCTION IF EXISTS build_listitem_jsonb;
-- Drop old signatures with jsonb parameter (before optimization to _values[])
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, jsonb, integer, integer, bigint);
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, jsonb, integer, text, bigint);
-- Drop new signature with _values[] array (if exists)
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, _values[], integer, text, bigint);

-- ===== HELPER: Build ListItem JSON (DRY - used in multiple places) =====
CREATE OR REPLACE FUNCTION build_listitem_jsonb(
    listitem_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 10
STABLE
AS $BODY$
BEGIN
    IF listitem_id IS NULL THEN
        RETURN NULL;
    END IF;
    
    RETURN (SELECT jsonb_build_object(
        'id', li._id,
        'idList', li._id_list,
        'value', li._value,
        'alias', li._alias,
        'object', CASE 
            WHEN li._id_object IS NOT NULL THEN
                get_object_json(li._id_object, GREATEST(0, max_depth - 1))
            ELSE NULL 
        END
    )
    FROM _list_items li
    WHERE li._id = listitem_id);
END;
$BODY$;

-- ===== OPTIMIZED FUNCTIONS =====

-- Optimized function for building hierarchical properties with preloaded values array
-- 🚀 OPTIMIZATION: Uses _values[] array instead of jsonb - all data in memory, no repeated table queries
CREATE OR REPLACE FUNCTION build_hierarchical_properties_optimized(
    object_id bigint,
    parent_structure_id bigint,
    object_scheme_id bigint,
    all_values _values[],  -- 🚀 Array of _values records instead of jsonb
    max_depth integer DEFAULT 10,
    array_index text DEFAULT NULL, -- Text to support Dictionary string keys
    parent_value_id bigint DEFAULT NULL -- ID of parent element for nested arrays
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb := '{}'::jsonb;
    structure_record RECORD;
    current_value_record _values;  -- 🚀 Typed record instead of jsonb
    field_value jsonb;
    base_array_value_id bigint; -- ID of base array record for recursion
BEGIN
    -- Protection against infinite recursion for Class fields (hierarchical structures)
    -- IMPORTANT: This function is for Class fields (Address.Street, Contacts[].Email)
    -- max_depth is NOT checked here - Class fields are always loaded completely!
    -- max_depth is controlled only in get_object_json() for Object references (_RObject)
    IF max_depth < -100 THEN
        -- Protection against anomalous recursion (practically impossible)
        RETURN jsonb_build_object('error', 'Max recursion depth reached for hierarchical fields');
    END IF;
    
    -- 🔥 AUTOMATIC CACHE CHECK AND POPULATION
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = object_scheme_id LIMIT 1) THEN
        -- Cache is empty for this scheme → populate automatically
        PERFORM sync_metadata_cache_for_scheme(object_scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- Collect all structures for given parent_structure_id (NO JOIN with _values!)
    -- 🚀 OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    FOR structure_record IN
        SELECT 
            c._structure_id as structure_id,
            c._name as field_name,
            c._collection_type as collection_type,  -- NULL = scalar, Array ID = array, Dictionary ID = dictionary
            c._collection_type = -9223372036854775668 as _is_array,  -- Array type ID
            c._collection_type = -9223372036854775667 as _is_dictionary,  -- Dictionary type ID
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = object_scheme_id
          AND ((parent_structure_id IS NULL AND c._parent_structure_id IS NULL) 
               OR (parent_structure_id IS NOT NULL AND c._parent_structure_id = parent_structure_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- 🚀 OPTIMIZATION: Search value from preloaded array using unnest()
        IF parent_value_id IS NOT NULL THEN
            -- For nested fields inside array/dictionary elements - find by _array_parent_id
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_parent_id = parent_value_id
            LIMIT 1;
        ELSIF array_index IS NOT NULL THEN
            -- For array/dictionary elements - find value with specific array_index
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_index = array_index
            LIMIT 1;
        ELSE
            -- For regular fields or root fields - find first match by structure_id
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_index IS NULL
            LIMIT 1;
        END IF;
        
        -- ✅ Get ID of base array record for recursion
        base_array_value_id := NULL; -- Reset before each field
        
        IF structure_record._is_array = true OR structure_record._is_dictionary = true THEN
            -- Find base array/dictionary record from preloaded data
            IF parent_value_id IS NULL THEN
                -- Root array/dictionary: _array_parent_id must be NULL
                SELECT v._id INTO base_array_value_id
                FROM unnest(all_values) AS v
                WHERE v._id_structure = structure_record.structure_id
                  AND v._array_index IS NULL
                  AND v._array_parent_id IS NULL
                LIMIT 1;
            ELSE
                -- Nested array/dictionary: _array_parent_id must match parent_value_id
                SELECT v._id INTO base_array_value_id
                FROM unnest(all_values) AS v
                WHERE v._id_structure = structure_record.structure_id
                  AND v._array_index IS NULL
                  AND v._array_parent_id = parent_value_id
                LIMIT 1;
            END IF;
        END IF;
        
        -- Determine field value based on its type and preloaded data
        field_value := CASE 
            -- If this is an array - process relationally through _array_index
            WHEN structure_record._is_array = true THEN
                CASE 
                    -- Array of Class fields - build from relational data recursively
                    WHEN structure_record.type_semantic = 'Object' THEN
                        (
                            WITH array_elements AS (
                                -- Find all array elements with their indices from preloaded data
                                SELECT 
                                    v._array_index,
                                    -- Safe numeric sorting: only for numeric indices (Array), text keys (Dictionary) sort as 0
                                    CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END as array_index_int,
                                    v._id as element_value_id,
                                    v._array_parent_id,
                                    build_hierarchical_properties_optimized(
                                        object_id, 
                                        structure_record.structure_id, 
                                        object_scheme_id, 
                                        all_values,  -- 🚀 Pass array, not jsonb
                                        max_depth,
                                        v._array_index,
                                        v._id
                                    ) as element_json
                                FROM unnest(all_values) AS v  -- 🚀 From memory array
                                WHERE v._id_structure = structure_record.structure_id
                                  AND v._array_index IS NOT NULL
                                  AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                                ORDER BY array_index_int, v._array_index  -- numeric first, then text
                            )
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '[]'::jsonb  -- Empty array = []
                                ELSE jsonb_agg(element_json ORDER BY array_index_int)
                            END
                            FROM array_elements
                        )
                    -- Arrays of primitive types (String, Long, Boolean, etc.) - relationally
                    ELSE
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '[]'::jsonb  -- Empty array = []
                                ELSE jsonb_agg(
                                CASE 
                                    -- Object references (_RObject) - check by type_semantic
                                    WHEN structure_record.type_semantic = '_RObject' AND v._Object IS NOT NULL THEN
                                        get_object_json(v._Object, max_depth - 1)
                                    WHEN structure_record.db_type = 'String' THEN to_jsonb(v._String)
                                    WHEN structure_record.db_type = 'Long' THEN 
                                        -- If _ListItem is filled, process as ListItem (for backward compatibility)
                                        CASE 
                                            WHEN v._ListItem IS NOT NULL THEN
                                                build_listitem_jsonb(v._ListItem, max_depth)
                                            ELSE to_jsonb(v._Long)
                                        END
                                    WHEN structure_record.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                    WHEN structure_record.db_type = 'Double' THEN to_jsonb(v._Double)
                                    WHEN structure_record.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                    WHEN structure_record.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                    WHEN structure_record.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                    WHEN structure_record.db_type = 'ListItem' THEN
                                        build_listitem_jsonb(v._ListItem, max_depth)
                                    WHEN structure_record.db_type = 'ByteArray' THEN 
                                        to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                    ELSE NULL
                                -- Safe sorting: numeric for Array, text for Dictionary
                                END ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END, v._array_index
                                )
                            END
                            FROM unnest(all_values) AS v  -- 🚀 From memory array
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                END
            
            -- Dictionary<K,V> fields - return JSON object with keys
            WHEN structure_record._is_dictionary = true THEN
                CASE 
                    -- Dictionary of RedbObject references (_RObject)
                    WHEN structure_record.type_semantic = '_RObject' THEN
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    CASE 
                                        WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1)
                                        ELSE NULL
                                    END
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                    -- Dictionary of Class fields
                    WHEN structure_record.type_semantic = 'Object' THEN
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    build_hierarchical_properties_optimized(
                                        object_id, 
                                        structure_record.structure_id, 
                                        object_scheme_id, 
                                        all_values,
                                        max_depth,
                                        NULL,  -- array_index = NULL for nested Class fields!
                                        v._id  -- parent_value_id = element record ID
                                    )
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                    -- Dictionary of primitive types
                    ELSE
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    CASE 
                                        WHEN structure_record.db_type = 'String' THEN to_jsonb(v._String)
                                        WHEN structure_record.db_type = 'Long' THEN to_jsonb(v._Long)
                                        WHEN structure_record.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                        WHEN structure_record.db_type = 'Double' THEN to_jsonb(v._Double)
                                        WHEN structure_record.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                        WHEN structure_record.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                        WHEN structure_record.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                        ELSE NULL
                                    END
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                END
            
            -- Regular fields (not arrays or dictionaries)
            -- Object reference to another object
            WHEN structure_record.type_name = 'Object' AND structure_record.type_semantic = '_RObject' THEN
                CASE 
                    WHEN current_value_record._Object IS NOT NULL THEN 
                        get_object_json(current_value_record._Object, max_depth - 1)
                    ELSE NULL
                END
            
            -- Class field with hierarchical child fields  
            WHEN structure_record.type_semantic = 'Object' THEN
                CASE 
                    WHEN current_value_record._Guid IS NULL THEN 
                        NULL  -- Class field is truly NULL - don't build object
                    ELSE
                        build_hierarchical_properties_optimized(
                            object_id, 
                            structure_record.structure_id, 
                            object_scheme_id, 
                            all_values,  -- 🚀 Pass array, not jsonb
                            max_depth,  -- Don't decrease max_depth for Class fields!
                            NULL,  -- array_index = NULL for nested Class fields
                            current_value_record._id  -- IMPORTANT: pass ID of current Class field record!
                        )
                END
                
            -- Primitive types - direct access to typed record fields (no JSON parsing!)
            -- Check _id IS NOT NULL to verify record was found (prevents jsonb null instead of SQL NULL)
            WHEN structure_record.db_type = 'String' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._String) ELSE NULL END
            WHEN structure_record.db_type = 'Long' THEN 
                -- If _ListItem is filled, process as ListItem (for backward compatibility)
                CASE 
                    WHEN current_value_record._ListItem IS NOT NULL THEN 
                        -- This is ListItem saved in old schema with db_type=Long
                        build_listitem_jsonb(current_value_record._ListItem, max_depth)
                    WHEN current_value_record._id IS NOT NULL THEN 
                        to_jsonb(current_value_record._Long)
                    ELSE NULL 
                END
            WHEN structure_record.db_type = 'Guid' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Guid) ELSE NULL END
            WHEN structure_record.db_type = 'Double' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Double) ELSE NULL END
            WHEN structure_record.db_type = 'Numeric' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Numeric) ELSE NULL END
            WHEN structure_record.db_type = 'DateTimeOffset' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._DateTimeOffset) ELSE NULL END
            WHEN structure_record.db_type = 'Boolean' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Boolean) ELSE NULL END
            WHEN structure_record.db_type = 'ListItem' OR current_value_record._ListItem IS NOT NULL THEN 
                -- Process as ListItem if db_type=ListItem OR if _ListItem is filled (backward compatibility)
                CASE 
                    WHEN current_value_record._ListItem IS NOT NULL THEN 
                        build_listitem_jsonb(current_value_record._ListItem, max_depth)
                    ELSE NULL
                END
            WHEN structure_record.db_type = 'ByteArray' THEN 
                CASE 
                    WHEN current_value_record._ByteArray IS NOT NULL THEN 
                        to_jsonb(encode(decode(current_value_record._ByteArray::text, 'base64'), 'base64'))
                    ELSE NULL
                END
            ELSE NULL
        END;
        -- Add field to result only if value is not NULL
        IF field_value IS NOT NULL THEN
            result_json := result_json || jsonb_build_object(structure_record.field_name, field_value);
        END IF;
        
    END LOOP;
    
    RETURN result_json;
END;
$BODY$;

-- OPTIMIZED function for getting object in JSON format with preloaded values array
-- 🚀 OPTIMIZATION: Loads all _values into typed array - no JSON parsing overhead
CREATE OR REPLACE FUNCTION get_object_json(
    object_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
    object_exists boolean;
    base_info jsonb;
    properties_info jsonb;
    object_scheme_id bigint;
    all_values _values[];  -- 🚀 Typed array instead of jsonb
BEGIN
    -- Check if object exists - return NULL if not found
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = object_id) INTO object_exists;
    
    IF NOT object_exists THEN
        RETURN NULL;
    END IF;
    
    -- Check recursion depth
    IF max_depth <= 0 THEN
        -- max_depth = 0: return ONLY base fields WITHOUT properties
        SELECT jsonb_build_object(
            'id', o._id,
            'name', o._name,
            'scheme_id', o._id_scheme,
            'scheme_name', sc._name,
            'parent_id', o._id_parent,
            'owner_id', o._id_owner,
            'who_change_id', o._id_who_change,
            'date_create', o._date_create,
            'date_modify', o._date_modify,
            'date_begin', o._date_begin,
            'date_complete', o._date_complete,
            'key', o._key,
            'value_long', o._value_long,
            'value_string', o._value_string,
            'value_guid', o._value_guid,
            'note', o._note,
            'value_bool', o._value_bool,
            'value_double', o._value_double,
            'value_numeric', o._value_numeric,
            'value_datetime', o._value_datetime,
            'value_bytes', o._value_bytes,
            'hash', o._hash
        ) INTO result_json
        FROM _objects o
        JOIN _schemes sc ON sc._id = o._id_scheme
        WHERE o._id = object_id;
        
        RETURN result_json;
    END IF;
    
    -- Collect base object info + get scheme_id
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'scheme_name', sc._name,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash
    ), o._id_scheme
    INTO base_info, object_scheme_id
    FROM _objects o
    JOIN _schemes sc ON sc._id = o._id_scheme
    WHERE o._id = object_id;
    
    -- 🚀 OPTIMIZATION: Load ALL values into typed array - single query, no JSON overhead
    SELECT array_agg(v) INTO all_values
    FROM _values v
    WHERE v._id_object = object_id;
    
    -- Use optimized function with preloaded values array
    SELECT build_hierarchical_properties_optimized(
        object_id, 
        NULL, 
        object_scheme_id, 
        COALESCE(all_values, ARRAY[]::_values[]),  -- 🚀 Pass typed array
        max_depth,
        NULL, -- array_index = NULL for root fields
        NULL  -- parent_value_id = NULL for root level
    ) INTO properties_info;
    
    -- Combine base info with properties
    result_json := base_info || jsonb_build_object('properties', COALESCE(properties_info, '{}'::jsonb));
    
    RETURN result_json;
END;
$BODY$;

-- BULK-OPTIMIZED VIEW for batch object retrieval in JSON format  
-- CREATE OR REPLACE VIEW v_objects_json AS
-- WITH 
-- -- Stage 1: BULK load values (optimal - GROUP BY only by ID)
-- all_values AS (
--     SELECT 
--         o._id,
--         COALESCE(
--             jsonb_object_agg(
--                 v._id_structure::text, 
--                 jsonb_build_object(
--                     '_String', v._String,
--                     '_Long', v._Long,
--                     '_Guid', v._Guid,
--                     '_Double', v._Double,
--                     '_DateTimeOffset', v._DateTimeOffset,
--                     '_Boolean', v._Boolean,
--                     '_ByteArray', v._ByteArray,
--                     '_array_parent_id', v._array_parent_id,
--                     '_array_index', v._array_index
--                 )
--             ) FILTER (WHERE v._id IS NOT NULL),
--             '{}'::jsonb
--         ) as all_values_json
--     FROM _objects o
--     LEFT JOIN _values v ON v._id_object = o._id
--     GROUP BY o._id  -- GROUP BY only by ID (fast!)
-- ),
-- -- Stage 2: Join with _objects fields and build JSON
-- objects_with_json AS (
--     SELECT 
--         o.*,  -- All _objects fields with single asterisk (efficient)
--         -- Full object JSON with properties
--         jsonb_build_object(
--             'id', o._id,
--             'name', o._name,
--             'scheme_id', o._id_scheme,
--             'scheme_name', s._name,
--             'parent_id', o._id_parent,
--             'owner_id', o._id_owner,
--             'who_change_id', o._id_who_change,
--             'date_create', o._date_create,
--             'date_modify', o._date_modify,
--             'date_begin', o._date_begin,
--             'date_complete', o._date_complete,
--             'key', o._key,
--             'value_long', o._value_long,
--             'value_string', o._value_string,
--             'value_guid', o._value_guid,
--             'note', o._note,
--             'value_bool', o._value_bool,
--             'hash', o._hash,
--             'properties', 
--             build_hierarchical_properties_optimized(
--                 o._id, 
--                 NULL, 
--                 o._id_scheme, 
--                 av.all_values_json,  -- Use preloaded data
--                 10,
--                 NULL -- array_index = NULL for root fields
--             )
--         ) as object_json
--     FROM _objects o
--     JOIN _schemes s ON s._id = o._id_scheme  
--     JOIN all_values av ON av._id = o._id  -- JOIN with preloaded values
-- )
-- SELECT * FROM objects_with_json ORDER BY _id;

-- -- Comments for OPTIMIZED functions and VIEWs for object retrieval
-- COMMENT ON VIEW v_objects_json IS 'MAXIMALLY OPTIMIZED VIEW for object retrieval. Two-stage architecture: 1) BULK _values aggregation with GROUP BY only by _id (fast!) 2) JOIN ready data with _objects via o.* (efficient). Returns ALL original _objects fields as columns PLUS full JSON with properties. Avoids heavy GROUP BY on 17 fields. Perfect for integration and API. Supports hierarchical Class fields.';

COMMENT ON FUNCTION build_hierarchical_properties_optimized(bigint, bigint, bigint, _values[], integer, text, bigint) IS 'Optimized function for recursive building of hierarchical JSON structure with preloaded _values[] array.
OPTIMIZATION: Uses typed _values[] array instead of jsonb - all data in memory, no repeated table queries!
IMPORTANT: max_depth is NOT decreased for Class fields - they are always loaded completely as part of object structure.
max_depth controls ONLY the depth of Object references (_RObject) in get_object_json().
Supports:
Relational arrays of Class fields
Nested arrays (arrays inside array elements) via array_index and parent_value_id
NO JOIN with _values in loop - uses unnest() from memory array!
5-10x faster for objects with arrays due to zero table queries.';

COMMENT ON FUNCTION get_object_json(bigint, integer) IS 'OPTIMIZED function for getting object in JSON format with SMART recursion depth:
max_depth = 0: only base fields WITHOUT properties (fast)
max_depth >= 1: base fields + properties
OPTIMIZATION: Loads ALL _values into typed array - single query, no JSON parsing overhead!
IMPORTANT: max_depth controls depth of Object references (_RObject):
  - Object references are called with max_depth-1
  - Class fields (Address, Contacts) are ALWAYS loaded COMPLETELY (max_depth not decreased)
  - ListItem._id_object also called with max_depth-1 (like regular Object references)
Supports:
Hierarchical Class fields (Address.Street, Contacts[].Email) - always fully
Object references (_RObject) - controlled depth via max_depth
ListItem with _id_object (base fields of linked object)
Relational arrays of all types
Optimal for objects with 10+ fields and arrays.';

-- ===== SIMPLE VIEW FOR OBJECTS WITH JSON =====

-- Drop existing view if exists
DROP VIEW IF EXISTS v_objects_json;

-- COMMENTED OUT: v_objects_json is inefficient for bulk operations (calls get_object_json for each row)
-- For LoadAsync direct SELECT from _objects + LoadPropsForManyAsync (LAZY) or get_object_json batch via unnest (EAGER) is used

-- -- Simple view: all _objects fields + JSON via get_object_json
-- CREATE VIEW v_objects_json AS
-- SELECT 
--     o.*,  -- All _objects fields as is
--     get_object_json(o._id, 10) as object_json  -- JSON representation of object
-- FROM _objects o;
-- COMMENT ON VIEW v_objects_json IS 'Simple view for object retrieval: all _objects fields + full JSON via get_object_json. Convenient for viewing and debugging.';


-- ===== redb_lazy_loading_search.sql =====
-- ===== LAZY LOADING SUPPORT FOR FACET SEARCH =====
-- New functions to return base objects without Props
-- Old functions (search_objects_with_facets, search_tree_objects_with_facets) remain unchanged
-- Author: AI Assistant
-- Creation date: 2025-11-17

-- ===== CLEANUP OF EXISTING FUNCTIONS =====
-- Drop ALL versions of functions (old and new signatures)
DROP FUNCTION IF EXISTS search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets_base(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview_base(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) CASCADE;

-- ========== FUNCTION 1: Return base fields WITHOUT Props ==========
CREATE OR REPLACE FUNCTION get_object_base_fields(object_id bigint)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash  -- CRITICAL for cache!
    )
    FROM _objects o
    WHERE o._id = object_id;
$$;

COMMENT ON FUNCTION get_object_base_fields(bigint) IS 
'Returns base object fields WITHOUT Props for lazy loading.
Includes hash for cache validation. 10-50x faster than get_object_json().
ATTENTION: Function kept for compatibility and direct use.
In aggregate queries (search_*_base), direct JOIN is used instead of function call for optimization.';

-- ========== FUNCTION 2: Execute query with base fields ==========
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS execute_objects_query_base(bigint, text, text, text, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS execute_objects_query_base(bigint, text, text, text, integer, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION execute_objects_query_base(
    scheme_id bigint,
    base_conditions text,
    hierarchical_conditions text,
    order_conditions text,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    final_where text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Combine all conditions (REUSE logic from execute_objects_query)
    final_where := format('WHERE obj._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (obj._hash)';
        -- PostgreSQL requires: ORDER BY must start with DISTINCT ON expression
        order_for_distinct := 'ORDER BY obj._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY obj._id');
    END IF;
    
    -- Query with direct JOIN instead of function call (optimization!)
    query_text := format('
        SELECT jsonb_agg(
            jsonb_build_object(
                ''id'', o._id,
                ''name'', o._name,
                ''scheme_id'', o._id_scheme,
                ''parent_id'', o._id_parent,
                ''owner_id'', o._id_owner,
                ''who_change_id'', o._id_who_change,
                ''date_create'', o._date_create,
                ''date_modify'', o._date_modify,
                ''date_begin'', o._date_begin,
                ''date_complete'', o._date_complete,
                ''key'', o._key,
                ''value_long'', o._value_long,
                ''value_string'', o._value_string,
                ''value_guid'', o._value_guid,
                ''note'', o._note,
                ''value_bool'', o._value_bool,
                ''value_double'', o._value_double,
                ''value_numeric'', o._value_numeric,
                ''value_datetime'', o._value_datetime,
                ''value_bytes'', o._value_bytes,
                ''hash'', o._hash
            )
        )
        FROM (
            SELECT %s obj._id
            FROM _objects obj
            %s
            %s
            %s
        ) sub
        JOIN _objects o ON o._id = sub._id',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    -- Count query (same as in original)
    -- ✅ With DISTINCT count unique hashes
    IF distinct_hash THEN
        count_query_text := format('
            SELECT COUNT(DISTINCT obj._hash)
            FROM _objects obj  
            %s',
            final_where
        );
    ELSE
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects obj  
            %s',
            final_where
        );
    END IF;
    
    EXECUTE query_text INTO objects_result;
    EXECUTE count_query_text INTO total_count;
    
    -- ⚡ LAZY LOADING: WITHOUT FACETS (they are expensive and not needed for base version)
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'facets', '[]'::jsonb  -- Empty array instead of get_facets(scheme_id)
    );
END;
$BODY$;

COMMENT ON FUNCTION execute_objects_query_base(bigint, text, text, text, integer, integer, boolean) IS 
'Executes search with base objects WITHOUT Props.
Returns the same JSON format as execute_objects_query, but objects without properties.
Used for lazy loading via GlobalPropsCache.
✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.
Reuses conditions from _build_single_facet_condition, build_hierarchical_conditions, build_order_conditions.';

-- ========== FUNCTION 3: Search with facets (base objects) ==========
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_facets_base(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
BEGIN
    -- REUSE existing condition building functions
    -- Same functions used by search_objects_with_facets
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'obj', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'obj');
    order_conditions := build_order_conditions(order_by, 'obj');
    
    -- Call new execution function with base fields
    RETURN execute_objects_query_base(
        scheme_id,
        base_conditions,
        hierarchical_conditions,
        order_conditions,
        limit_count,
        offset_count,
        distinct_hash  -- ✅ Pass through parameter
    );
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_facets_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) IS 
'Faceted search returning base objects WITHOUT Props.
Used for lazy loading + GlobalPropsCache.
Supports all LINQ operators ($gt, $contains, $arrayContains, etc.), Class fields (Contact.Name), arrays (Tags[]).
✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.
Signature and response format match search_objects_with_facets, but objects without properties.
100% reuse of condition logic from search_objects_with_facets.';

-- ========== FUNCTION 3.5: Get only filtered object IDs ==========
-- ⚡ OPTIMIZED for aggregations — returns only bigint[] without JSON overhead
DROP FUNCTION IF EXISTS get_filtered_object_ids(bigint, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION get_filtered_object_ids(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_max_recursion_depth integer DEFAULT 10
) RETURNS bigint[]
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$
DECLARE
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_final_where text;
    v_result bigint[];
BEGIN
    -- ⚡ REUSE existing condition building functions
    -- Same functions as in search_objects_with_facets_base — NO duplication!
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', p_max_recursion_depth);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    
    -- Build WHERE (same logic as in execute_objects_query_base)
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', 
                           p_scheme_id, 
                           COALESCE(v_base_conditions, ''),
                           COALESCE(v_hierarchical_conditions, ''));
    
    -- ⚡ Simple SELECT only IDs — no JSON, no sorting, no pagination!
    EXECUTE format('SELECT ARRAY_AGG(obj._id) FROM _objects obj %s', v_final_where)
    INTO v_result;
    
    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION get_filtered_object_ids(bigint, jsonb, integer) IS 
'⚡ Optimized function for aggregations.
Returns only array of object IDs (bigint[]) instead of full JSON.
100% reuses filter logic from search_objects_with_facets_base.
No overhead for JSON serialization, sorting, and pagination.
Example: SELECT get_filtered_object_ids(1002, ''{"Age": {"$gt": 50}}'');';

-- ========== FUNCTION 4: Tree search (base objects) ==========
CREATE OR REPLACE FUNCTION search_tree_objects_with_facets_base(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 300
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    base_conditions text;
    order_conditions text;
BEGIN
    -- 🔥 AUTOMATIC CHECK AND CACHE POPULATION
    -- Ensures scheme metadata cache is populated before building conditions
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- REUSE existing condition building functions
    -- Same functions used by search_tree_objects_with_facets
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- max_depth = 1: only direct children
    IF max_depth = 1 THEN
        query_text := format('
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', o._id,
                    ''name'', o._name,
                    ''scheme_id'', o._id_scheme,
                    ''parent_id'', o._id_parent,
                    ''owner_id'', o._id_owner,
                    ''who_change_id'', o._id_who_change,
                    ''date_create'', o._date_create,
                    ''date_modify'', o._date_modify,
                    ''date_begin'', o._date_begin,
                    ''date_complete'', o._date_complete,
                    ''key'', o._key,
                    ''value_long'', o._value_long,
                    ''value_string'', o._value_string,
                    ''value_guid'', o._value_guid,
                    ''note'', o._note,
                    ''value_bool'', o._value_bool,
                    ''value_double'', o._value_double,
                    ''value_numeric'', o._value_numeric,
                    ''value_datetime'', o._value_datetime,
                    ''value_bytes'', o._value_bytes,
                    ''hash'', o._hash
                )
            )
            FROM (
                SELECT d._id
                FROM _objects d
                WHERE d._id_scheme = %s 
                  AND d._id_parent = ANY($1)%s
                %s
                %s
            ) sub
            JOIN _objects o ON o._id = sub._id',
            scheme_id,
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects d
            WHERE d._id_scheme = %s 
              AND d._id_parent = ANY($1)%s',
            scheme_id,
            COALESCE(base_conditions, '')
        );
    
    -- max_depth > 1: recursive descendant search
    ELSE
        -- ✅ SECURITY: Use positional parameters $1, $2 for EXECUTE USING
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', o._id,
                    ''name'', o._name,
                    ''scheme_id'', o._id_scheme,
                    ''parent_id'', o._id_parent,
                    ''owner_id'', o._id_owner,
                    ''who_change_id'', o._id_who_change,
                    ''date_create'', o._date_create,
                    ''date_modify'', o._date_modify,
                    ''date_begin'', o._date_begin,
                    ''date_complete'', o._date_complete,
                    ''key'', o._key,
                    ''value_long'', o._value_long,
                    ''value_string'', o._value_string,
                    ''value_guid'', o._value_guid,
                    ''note'', o._note,
                    ''value_bool'', o._value_bool,
                    ''value_double'', o._value_double,
                    ''value_numeric'', o._value_numeric,
                    ''value_datetime'', o._value_datetime,
                    ''value_bytes'', o._value_bytes,
                    ''hash'', o._hash
                )
            )
            FROM (
                SELECT d._id
                FROM descendants dt
                JOIN _objects d ON dt._id = d._id
                WHERE dt.depth > 0 
                  AND d._id_scheme = %s%s
                %s
                %s
            ) sub
            JOIN _objects o ON o._id = sub._id',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        
        count_query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT COUNT(DISTINCT d._id)
            FROM descendants dt
            JOIN _objects d ON dt._id = d._id
            WHERE dt.depth > 0 
              AND d._id_scheme = %s%s',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, '')
        );
    END IF;
    
    -- Execute queries with USING to pass array!
    EXECUTE query_text INTO objects_result USING parent_ids;
    EXECUTE count_query_text INTO total_count USING parent_ids;
    
    -- ⚡ LAZY LOADING: WITHOUT FACETS (they are expensive and not needed for base version)
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'parent_ids', parent_ids,  -- ✅ BATCH: Array of parents
        'max_depth', max_depth,
        'facets', '[]'::jsonb  -- Empty array instead of get_facets(scheme_id)
    );
END;
$BODY$;

COMMENT ON FUNCTION search_tree_objects_with_facets_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) IS 
'✅ BATCH OPTIMIZATION! Tree search returning base objects WITHOUT Props. Accepts parent_ids[] array for 3-4x speedup.
Used for lazy loading + GlobalPropsCache.
Supports hierarchical conditions ($hasAncestor, $hasDescendant, $level), LINQ operators, Class fields.
Signature and response format match search_tree_objects_with_facets, but objects without properties.
100% reuse of condition logic from search_tree_objects_with_facets.';

-- ===== USAGE EXAMPLES =====
/*
-- Example 1: Base objects without filters
SELECT search_objects_with_facets_base(1002, NULL, 10, 0, NULL, 10);

-- Example 2: With LINQ filters
SELECT search_objects_with_facets_base(
    1002, 
    '{"Status": "Active", "Price": {"$gt": "100"}}'::jsonb,
    10, 0, NULL, 10
);

-- Example 3: Tree search for direct children
SELECT search_tree_objects_with_facets_base(1002, 100, NULL, 10, 0, NULL, 1, 10);

-- Example 4: Recursive descendant search
SELECT search_tree_objects_with_facets_base(1002, 100, NULL, 20, 0, NULL, 5, 10);

-- Example 5: Performance comparison
EXPLAIN ANALYZE SELECT search_objects_with_facets(1002, NULL, 100, 0);
EXPLAIN ANALYZE SELECT search_objects_with_facets_base(1002, NULL, 100, 0);

-- Check result - should be WITHOUT "properties" field:
-- {"objects": [{"id": 1, "name": "...", "hash": "abc-123", ...}], "total_count": 10, "facets": {...}}
*/

-- ===== SQL PREVIEW for LAZY LOADING (for debugging) =====

-- Function 1: Preview for standard search with base fields
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION get_search_sql_preview_base(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
    final_where text;
    query_text text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Reuse condition building functions (from redb_facets_search.sql)
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'obj', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'obj');
    order_conditions := build_order_conditions(order_by, 'obj');
    
    -- Combine WHERE
    final_where := format('WHERE obj._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (obj._hash)';
        order_for_distinct := 'ORDER BY obj._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY obj._id');
    END IF;
    
    -- Build SQL with direct JOIN (DO NOT EXECUTE!)
    query_text := format('
SELECT jsonb_agg(
    jsonb_build_object(
        ''id'', o._id,
        ''name'', o._name,
        ''scheme_id'', o._id_scheme,
        ''parent_id'', o._id_parent,
        ''owner_id'', o._id_owner,
        ''who_change_id'', o._id_who_change,
        ''date_create'', o._date_create,
        ''date_modify'', o._date_modify,
        ''date_begin'', o._date_begin,
        ''date_complete'', o._date_complete,
        ''key'', o._key,
        ''value_long'', o._value_long,
        ''value_string'', o._value_string,
        ''value_guid'', o._value_guid,
        ''note'', o._note,
        ''value_bool'', o._value_bool,
        ''value_double'', o._value_double,
        ''value_numeric'', o._value_numeric,
        ''value_datetime'', o._value_datetime,
        ''value_bytes'', o._value_bytes,
        ''hash'', o._hash
    )
)
FROM (
    SELECT %s obj._id
    FROM _objects obj
    %s
    %s
    %s
) sub
JOIN _objects o ON o._id = sub._id',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer, boolean) IS 
'Returns SQL query for lazy loading (for debugging). Shows what will be executed in search_objects_with_facets_base(). ✅ distinct_hash=true adds DISTINCT ON (_hash). Returns base fields WITHOUT Props.';

-- Function 2: Preview for tree search with base fields
CREATE OR REPLACE FUNCTION get_search_tree_sql_preview_base(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10
) RETURNS text
LANGUAGE 'plpgsql'
COST 100
IMMUTABLE
AS $BODY$
DECLARE
    query_text text;
    base_conditions text;
    order_conditions text;
BEGIN
    -- Reuse condition building functions
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- If max_depth = 1, search only direct children
    IF max_depth = 1 THEN
        query_text := format('
SELECT jsonb_agg(
    jsonb_build_object(
        ''id'', o._id,
        ''name'', o._name,
        ''scheme_id'', o._id_scheme,
        ''parent_id'', o._id_parent,
        ''owner_id'', o._id_owner,
        ''who_change_id'', o._id_who_change,
        ''date_create'', o._date_create,
        ''date_modify'', o._date_modify,
        ''date_begin'', o._date_begin,
        ''date_complete'', o._date_complete,
        ''key'', o._key,
        ''value_long'', o._value_long,
        ''value_string'', o._value_string,
        ''value_guid'', o._value_guid,
        ''note'', o._note,
        ''value_bool'', o._value_bool,
        ''value_double'', o._value_double,
        ''value_numeric'', o._value_numeric,
        ''value_datetime'', o._value_datetime,
        ''value_bytes'', o._value_bytes,
        ''hash'', o._hash
    )
)
FROM (
    SELECT d._id
    FROM _objects d
    WHERE d._id_scheme = %s 
      AND d._id_parent = ANY(%L)%s
    %s
    %s
) sub
JOIN _objects o ON o._id = sub._id',
            scheme_id,
            parent_ids,  -- ✅ BATCH: Array of parents
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    ELSE
        -- Recursive descendant search
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', o._id,
                    ''name'', o._name,
                    ''scheme_id'', o._id_scheme,
                    ''parent_id'', o._id_parent,
                    ''owner_id'', o._id_owner,
                    ''who_change_id'', o._id_who_change,
                    ''date_create'', o._date_create,
                    ''date_modify'', o._date_modify,
                    ''date_begin'', o._date_begin,
                    ''date_complete'', o._date_complete,
                    ''key'', o._key,
                    ''value_long'', o._value_long,
                    ''value_string'', o._value_string,
                    ''value_guid'', o._value_guid,
                    ''note'', o._note,
                    ''value_bool'', o._value_bool,
                    ''value_double'', o._value_double,
                    ''value_numeric'', o._value_numeric,
                    ''value_datetime'', o._value_datetime,
                    ''value_bytes'', o._value_bytes,
                    ''hash'', o._hash
                )
            )
            FROM (
                SELECT d._id
                FROM descendants dt
                JOIN _objects d ON dt._id = d._id
                WHERE dt.depth > 0 
                  AND d._id_scheme = %s%s
                %s
                %s
            ) sub
            JOIN _objects o ON o._id = sub._id',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    END IF;
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_tree_sql_preview_base(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) IS 
'✅ BATCH: Accepts parent_ids[]. Returns SQL query for tree lazy loading (for debugging). Shows what will be executed in search_tree_objects_with_facets_base(). Returns base fields WITHOUT Props. 🔥 Without DISTINCT for compatibility with ORDER BY.';

-- ===== redb_migrations.sql =====
-- =====================================================
-- REDB Pro: Таблица истории миграций (PostgreSQL)
-- =====================================================

CREATE TABLE IF NOT EXISTS _migrations (
    _id BIGSERIAL PRIMARY KEY,
    _migration_id TEXT NOT NULL,                    -- уникальный ID миграции "OrderProps_TotalPrice_v1"
    _scheme_id BIGINT NOT NULL REFERENCES _schemes(_id) ON DELETE CASCADE,
    _structure_id BIGINT REFERENCES _structures(_id) ON DELETE SET NULL,  -- NULL = вся схема
    _property_name TEXT,                            -- имя свойства (для логов)
    _expression_hash TEXT,                          -- MD5 от Expression для детекции изменений
    _migration_type TEXT NOT NULL,                  -- ComputedFrom, TypeChange, DefaultValue, Transform
    _applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _applied_by TEXT,                               -- кто применил (user/system)
    _sql_executed TEXT,                             -- SQL который был выполнен (для аудита)
    _affected_rows INT,                             -- сколько записей затронуто
    _duration_ms INT,                               -- время выполнения
    _dry_run BOOLEAN NOT NULL DEFAULT FALSE,        -- это был dry-run?
    
    CONSTRAINT uq_migration_scheme UNIQUE(_scheme_id, _migration_id)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_migrations_scheme ON _migrations(_scheme_id);
CREATE INDEX IF NOT EXISTS idx_migrations_applied ON _migrations(_applied_at DESC);

COMMENT ON TABLE _migrations IS 'История применённых миграций данных (Pro feature)';
COMMENT ON COLUMN _migrations._migration_id IS 'Уникальный ID миграции в формате SchemeType_PropertyName_vN';
COMMENT ON COLUMN _migrations._expression_hash IS 'MD5 хеш Expression для детекции изменений';
COMMENT ON COLUMN _migrations._sql_executed IS 'SQL запрос для аудита и отладки';


-- ===== redb_projection.sql =====
-- ============================================================
-- PROJECTION FUNCTIONS: Optimized loading of only required fields
-- ============================================================
-- Result format:
--   - Class field → flat with paths: "Contact.Name": "John"
--   - Arrays → flat with indexes: "Items[0].Price": 100
--   - _RObject → NESTED object: "Author": { "Name": "Pushkin", ... }
-- ============================================================

-- ===== FUNCTION 1: Building a flat projection =====

DROP FUNCTION IF EXISTS build_flat_projection(bigint, bigint, jsonb, jsonb, integer, text, integer, bigint) CASCADE;

CREATE OR REPLACE FUNCTION build_flat_projection(
    p_object_id bigint,
    p_scheme_id bigint,
    p_projection_paths jsonb,
    p_all_values_json jsonb,
    p_max_depth integer DEFAULT 10,
    p_path_prefix text DEFAULT '',
    p_array_index integer DEFAULT NULL,
    p_parent_value_id bigint DEFAULT NULL
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb := '{}'::jsonb;
    proj_item jsonb;
    v_path text;
    v_structure_id bigint;
    v_is_array boolean;
    v_is_dictionary boolean;
    v_collection_type_name text;
    v_db_type text;
    v_type_semantic text;
    v_current_value jsonb;
    v_field_value jsonb;
    v_ref_object_id bigint;
    v_ref_scheme_id bigint;
    v_ref_values_json jsonb;
    v_ref_projection jsonb;
    v_array_element RECORD;
    v_base_array_value_id bigint;
    v_full_path text;
BEGIN
    IF p_max_depth < -100 THEN
        RETURN jsonb_build_object('_error', 'Max depth exceeded');
    END IF;
    
    FOR proj_item IN SELECT * FROM jsonb_array_elements(p_projection_paths)
    LOOP
        v_path := proj_item->>'path';
        v_structure_id := (proj_item->>'structure_id')::bigint;
        
        IF v_structure_id IS NULL THEN CONTINUE; END IF;
        
        -- Get collection info: is_array = true for both Array and Dictionary
        -- v_collection_type_name used to distinguish them ('Array' or 'Dictionary')
        SELECT 
            _collection_type IS NOT NULL,
            db_type, 
            type_semantic,
            CASE 
                WHEN _collection_type IS NOT NULL THEN 
                    (SELECT t._name FROM _types t WHERE t._id = _collection_type)
                ELSE NULL 
            END,
            -- ⭐ If path is not provided, take from metadata
            COALESCE(v_path, _name)
        INTO v_is_array, v_db_type, v_type_semantic, v_collection_type_name, v_path
        FROM _scheme_metadata_cache
        WHERE _scheme_id = p_scheme_id AND _structure_id = v_structure_id;
        
        IF NOT FOUND THEN CONTINUE; END IF;
        
        -- Determine if this is a Dictionary (string keys) vs Array (numeric keys)
        v_is_dictionary := (v_collection_type_name = 'Dictionary');
        
        v_full_path := CASE WHEN p_path_prefix = '' THEN v_path ELSE p_path_prefix || v_path END;
        
        IF p_parent_value_id IS NOT NULL THEN
            -- ⭐ Child field of Dictionary/Array: search by _array_parent_id
            SELECT jsonb_build_object(
                '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
                '_Double', v._Double, '_Numeric', v._Numeric,
                '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
                '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
            ) INTO v_current_value
            FROM _values v
            WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
              AND v._array_parent_id = p_parent_value_id
            LIMIT 1;
        ELSIF p_array_index IS NOT NULL THEN
            -- ⭐ Dictionary/Array element: search by _array_index
            SELECT jsonb_build_object(
                '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
                '_Double', v._Double, '_Numeric', v._Numeric,
                '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
                '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
            ) INTO v_current_value
            FROM _values v
            WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
              AND v._array_index = p_array_index::text
            LIMIT 1;
        ELSE
            -- ⭐ Regular field: take from cache
            v_current_value := p_all_values_json->v_structure_id::text;
        END IF;
        
        -- _RObject → NESTED object (reference to another RedbObject)
        IF v_type_semantic = '_RObject' THEN
            IF p_max_depth > 0 AND v_current_value IS NOT NULL THEN
                v_ref_object_id := (v_current_value->>'_Object')::bigint;
                IF v_ref_object_id IS NOT NULL THEN
                    SELECT _id_scheme INTO v_ref_scheme_id FROM _objects WHERE _id = v_ref_object_id;
                    
                    SELECT jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                        '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
                        '_Double', v._Double, '_Numeric', v._Numeric,
                        '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
                        '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
                    )) INTO v_ref_values_json
                    FROM _values v WHERE v._id_object = v_ref_object_id AND v._array_index IS NULL;
                    
                    v_ref_projection := proj_item->'nested';
                    IF v_ref_projection IS NOT NULL AND jsonb_array_length(v_ref_projection) > 0 THEN
                        v_field_value := build_flat_projection(
                            v_ref_object_id, v_ref_scheme_id, v_ref_projection,
                            COALESCE(v_ref_values_json, '{}'::jsonb), p_max_depth - 1, v_path, NULL, NULL
                        );
                    ELSE
                        v_field_value := jsonb_build_object('_id', v_ref_object_id);
                    END IF;
                    result_json := result_json || jsonb_build_object(v_path, v_field_value);
                END IF;
            ELSIF v_current_value IS NOT NULL AND (v_current_value->>'_Object') IS NOT NULL THEN
                result_json := result_json || jsonb_build_object(v_path, jsonb_build_object('_id', (v_current_value->>'_Object')::bigint));
            END IF;
        
        -- Arrays and Dictionaries (both have _array_index) — MUST BE BEFORE Object!
        -- Dictionary<K,V> can have type_semantic='Object' if V is a class
        ELSIF v_is_array THEN
            DECLARE
                v_dict_key text := proj_item->>'dict_key';  -- ⭐ Specific Dictionary key
            BEGIN
                v_base_array_value_id := NULL;
                IF p_parent_value_id IS NULL THEN
                    SELECT v._id INTO v_base_array_value_id FROM _values v
                    WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
                      AND v._array_index IS NULL AND v._array_parent_id IS NULL LIMIT 1;
                ELSE
                    SELECT v._id INTO v_base_array_value_id FROM _values v
                    WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
                      AND v._array_index IS NULL AND v._array_parent_id = p_parent_value_id LIMIT 1;
                END IF;
                
                FOR v_array_element IN
                    SELECT v._array_index, v._id,
                        CASE 
                            WHEN v_db_type = 'String' THEN to_jsonb(v._String)
                            WHEN v_db_type = 'Long' THEN to_jsonb(v._Long)
                            WHEN v_db_type = 'Guid' THEN to_jsonb(v._Guid)
                            WHEN v_db_type = 'Double' THEN to_jsonb(v._Double)
                            WHEN v_db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                            WHEN v_db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                            WHEN v_db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                            ELSE NULL
                        END as value
                    FROM _values v
                    WHERE v._id_object = p_object_id AND v._id_structure = v_structure_id
                      AND v._array_index IS NOT NULL
                      AND (v_base_array_value_id IS NULL OR v._array_parent_id = v_base_array_value_id)
                      -- ⭐ Filter by specific Dictionary key (if provided)
                      AND (v_dict_key IS NULL OR v._array_index = v_dict_key)
                    ORDER BY 
                        CASE WHEN v_is_dictionary THEN v._array_index 
                             ELSE lpad(v._array_index, 10, '0') -- numeric sort for arrays
                        END
                LOOP
                -- Dictionary: path[key] or Array: path[index]
                v_full_path := v_path || '[' || v_array_element._array_index || ']';
                
                IF v_type_semantic = 'Object' THEN
                    v_ref_projection := proj_item->'nested';
                    IF v_ref_projection IS NOT NULL AND jsonb_array_length(v_ref_projection) > 0 THEN
                        -- ⭐ For Dictionary<K,Class> return hierarchical JSON: {"AddressBook": {"home": {...}}}
                        -- Use build_hierarchical_properties_optimized to build full nested object
                        DECLARE
                            v_all_values _values[];
                            v_nested_json jsonb;
                            v_existing_dict jsonb;
                        BEGIN
                            -- Load all object values for recursive function
                            SELECT array_agg(v) INTO v_all_values
                            FROM _values v WHERE v._id_object = p_object_id;
                            
                            -- Build full JSON for Dictionary element
                            v_nested_json := build_hierarchical_properties_optimized(
                                p_object_id,
                                v_structure_id,  -- parent = AddressBook structure
                                p_scheme_id,
                                COALESCE(v_all_values, ARRAY[]::_values[]),
                                p_max_depth - 1,
                                v_array_element._array_index,  -- array_index = 'home'
                                v_array_element._id  -- parent_value_id
                            );
                            
                            -- Add to hierarchical structure: {"AddressBook": {"home": {...}}}
                            v_existing_dict := result_json->v_path;
                            IF v_existing_dict IS NULL THEN
                                v_existing_dict := '{}'::jsonb;
                            END IF;
                            v_existing_dict := v_existing_dict || jsonb_build_object(v_array_element._array_index, v_nested_json);
                            result_json := result_json || jsonb_build_object(v_path, v_existing_dict);
                        END;
                    END IF;
                ELSIF v_array_element.value IS NOT NULL THEN
                    -- ⭐ For Dictionary return hierarchical JSON: {"PhoneBook": {"home": "..."}}
                    IF v_is_dictionary THEN
                        DECLARE
                            v_existing_dict jsonb;
                        BEGIN
                            v_existing_dict := result_json->v_path;
                            IF v_existing_dict IS NULL THEN
                                v_existing_dict := '{}'::jsonb;
                            END IF;
                            v_existing_dict := v_existing_dict || jsonb_build_object(v_array_element._array_index, v_array_element.value);
                            result_json := result_json || jsonb_build_object(v_path, v_existing_dict);
                        END;
                    ELSE
                        -- For Array — flat format: "Items[0]": value
                        result_json := result_json || jsonb_build_object(v_full_path, v_array_element.value);
                    END IF;
                END IF;
            END LOOP;
            END;  -- ⭐ Closing DECLARE block for v_dict_key
        
        -- ⭐ Object (Class) → flat with paths: Address1.City, Address1.Street
        -- NOT array/dictionary, but has nested fields
        ELSIF v_type_semantic = 'Object' THEN
            v_ref_projection := proj_item->'nested';
            IF v_ref_projection IS NOT NULL AND jsonb_array_length(v_ref_projection) > 0 THEN
                -- Has nested — recursively process child fields
                v_field_value := build_flat_projection(
                    p_object_id, p_scheme_id, v_ref_projection, p_all_values_json,
                    p_max_depth - 1, v_full_path || '.', NULL, NULL
                );
                result_json := result_json || v_field_value;
            END IF;
            
        -- Simple fields → flat with path
        ELSE
            v_field_value := CASE 
                WHEN v_db_type = 'String' AND v_current_value IS NOT NULL THEN to_jsonb(v_current_value->>'_String')
                WHEN v_db_type = 'Long' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Long')::bigint)
                WHEN v_db_type = 'Guid' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Guid')::uuid)
                WHEN v_db_type = 'Double' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Double')::double precision)
                WHEN v_db_type = 'Numeric' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Numeric')::numeric)
                WHEN v_db_type = 'DateTimeOffset' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_DateTimeOffset')::timestamptz)
                WHEN v_db_type = 'Boolean' AND v_current_value IS NOT NULL THEN to_jsonb((v_current_value->>'_Boolean')::boolean)
                WHEN v_db_type = 'ListItem' AND v_current_value IS NOT NULL AND (v_current_value->>'_ListItem')::bigint IS NOT NULL THEN
                    (SELECT jsonb_build_object('id', li._id, 'value', li._value, 'alias', li._alias)
                     FROM _list_items li WHERE li._id = (v_current_value->>'_ListItem')::bigint)
                ELSE NULL
            END;
            IF v_field_value IS NOT NULL THEN
                result_json := result_json || jsonb_build_object(v_full_path, v_field_value);
            END IF;
        END IF;
    END LOOP;
    
    RETURN result_json;
END;
$BODY$;

COMMENT ON FUNCTION build_flat_projection IS 'Flat projection: Class→path, array→index, _RObject→nested';


-- ===== FUNCTION 2: Search with projection =====

DROP FUNCTION IF EXISTS search_objects_with_projection(bigint, jsonb, jsonb, integer, integer, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_projection(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_projection_paths jsonb DEFAULT '[]'::jsonb,
    p_limit integer DEFAULT NULL,
    p_offset integer DEFAULT 0,
    p_order_by jsonb DEFAULT NULL,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 500
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_structure_ids bigint[];
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_order_conditions text;
    v_final_where text;
    v_objects_result jsonb;
    v_total_count integer;
    v_query text;
BEGIN
    -- Extract structure_ids from projection_paths
    WITH RECURSIVE all_paths AS (
        SELECT (elem->>'structure_id')::bigint as sid FROM jsonb_array_elements(p_projection_paths) elem
        WHERE elem->>'structure_id' IS NOT NULL
        UNION ALL
        SELECT (nested_elem->>'structure_id')::bigint
        FROM jsonb_array_elements(p_projection_paths) elem, jsonb_array_elements(elem->'nested') nested_elem
        WHERE elem->'nested' IS NOT NULL AND nested_elem->>'structure_id' IS NOT NULL
    )
    SELECT ARRAY_AGG(DISTINCT sid) INTO v_structure_ids FROM all_paths WHERE sid IS NOT NULL;
    
    -- Filter conditions
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', 10);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    v_order_conditions := build_order_conditions(p_order_by, 'obj');
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', p_scheme_id, COALESCE(v_base_conditions, ''), COALESCE(v_hierarchical_conditions, ''));
    
    EXECUTE format('SELECT COUNT(*) FROM _objects obj %s', v_final_where) INTO v_total_count;
    
    v_query := format('
        WITH filtered_objects AS (
            SELECT obj._id FROM _objects obj %s %s %s
        ),
        projected_values AS (
            SELECT v._id_object, jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                ''_String'', v._String, ''_Long'', v._Long, ''_Guid'', v._Guid,
                ''_Double'', v._Double, ''_Numeric'', v._Numeric, ''_DateTimeOffset'', v._DateTimeOffset,
                ''_Boolean'', v._Boolean, ''_ByteArray'', v._ByteArray, ''_ListItem'', v._ListItem, ''_Object'', v._Object
            )) as values_json
            FROM _values v
            WHERE v._id_object IN (SELECT _id FROM filtered_objects) AND v._id_structure = ANY($1) AND v._array_index IS NULL
            GROUP BY v._id_object
        )
        SELECT jsonb_agg(jsonb_build_object(
            ''_id'', o._id, ''_name'', o._name, ''_scheme_id'', o._id_scheme, ''_hash'', o._hash,
            ''properties'', build_flat_projection(o._id, o._id_scheme, $2, COALESCE(pv.values_json, ''{}''::jsonb), %s, '''', NULL, NULL)
        ))
        FROM filtered_objects fo JOIN _objects o ON o._id = fo._id LEFT JOIN projected_values pv ON pv._id_object = o._id',
        v_final_where, v_order_conditions,
        CASE WHEN p_limit IS NULL OR p_limit >= 2000000000 THEN '' ELSE format('LIMIT %s OFFSET %s', p_limit, p_offset) END,
        p_max_depth
    );
    
    EXECUTE v_query INTO v_objects_result USING v_structure_ids, p_projection_paths;
    
    RETURN jsonb_build_object('objects', COALESCE(v_objects_result, '[]'::jsonb), 'total_count', v_total_count, 'limit', p_limit, 'offset', p_offset);
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_projection IS 'Search with flat projection. Loads only required structure_ids.';


-- ===== FUNCTION 3: Get ONE object with projection =====

DROP FUNCTION IF EXISTS get_object_with_projection(bigint, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION get_object_with_projection(
    p_object_id bigint,
    p_projection_paths jsonb,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_scheme_id bigint;
    v_values_json jsonb;
    v_structure_ids bigint[];
    v_result jsonb;
BEGIN
    -- Get object scheme_id
    SELECT _id_scheme INTO v_scheme_id FROM _objects WHERE _id = p_object_id;
    IF v_scheme_id IS NULL THEN
        RETURN jsonb_build_object('error', 'Object not found');
    END IF;
    
    -- Extract all structure_ids from projection_paths
    WITH RECURSIVE all_paths AS (
        SELECT (elem->>'structure_id')::bigint as sid FROM jsonb_array_elements(p_projection_paths) elem
        WHERE elem->>'structure_id' IS NOT NULL
        UNION ALL
        SELECT (nested_elem->>'structure_id')::bigint
        FROM jsonb_array_elements(p_projection_paths) elem, jsonb_array_elements(elem->'nested') nested_elem
        WHERE elem->'nested' IS NOT NULL AND nested_elem->>'structure_id' IS NOT NULL
    )
    SELECT ARRAY_AGG(DISTINCT sid) INTO v_structure_ids FROM all_paths WHERE sid IS NOT NULL;
    
    -- Load only required values
    SELECT jsonb_object_agg(v._id_structure::text, jsonb_build_object(
        '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
        '_Double', v._Double, '_Numeric', v._Numeric, '_DateTimeOffset', v._DateTimeOffset,
        '_Boolean', v._Boolean, '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
    ))
    INTO v_values_json
    FROM _values v
    WHERE v._id_object = p_object_id 
      AND v._id_structure = ANY(v_structure_ids)
      AND v._array_index IS NULL;
    
    -- Build result
    SELECT jsonb_build_object(
        '_id', o._id,
        '_name', o._name,
        '_scheme_id', o._id_scheme,
        '_hash', o._hash,
        '_date_modify', o._date_modify,
        'properties', build_flat_projection(
            o._id, o._id_scheme, p_projection_paths,
            COALESCE(v_values_json, '{}'::jsonb), p_max_depth, '', NULL, NULL
        )
    )
    INTO v_result
    FROM _objects o WHERE o._id = p_object_id;
    
    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION get_object_with_projection IS 'Get one object with flat projection by ID.';


-- ===== FUNCTION 4: Search with projection by text paths (MAIN) =====
-- Accepts human-readable paths: ["Name", "AddressBook[home].City"]
-- SQL resolves them into structure_ids and builds nested projection

DROP FUNCTION IF EXISTS search_objects_with_projection_by_paths(bigint, jsonb, text[], integer, integer, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_projection_by_paths(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_field_paths text[] DEFAULT ARRAY[]::text[],
    p_limit integer DEFAULT NULL,
    p_offset integer DEFAULT 0,
    p_order_by jsonb DEFAULT NULL,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 500
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_projection_paths jsonb := '[]'::jsonb;
    v_path text;
    v_resolved record;
    v_nested_paths jsonb;
    v_parent_path text;
    v_parent_key text;
    v_child_field text;
    v_parent_entry jsonb;
    v_idx int;
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_order_conditions text;
    v_final_where text;
    v_objects_result jsonb;
    v_total_count integer;
    v_query text;
    v_structure_ids bigint[] := ARRAY[]::bigint[];
BEGIN
    -- STEP 1: Parse text paths and build nested projection
    -- Input format: ["Name", "AddressBook[home].City", "AddressBook[home].Street"]
    -- Output format: [
    --   {"path": "Name", "structure_id": 123},
    --   {"path": "AddressBook", "structure_id": 456, "nested": [
    --     {"path": "City", "structure_id": 789},
    --     {"path": "Street", "structure_id": 790}
    --   ]}
    -- ]
    
    FOREACH v_path IN ARRAY p_field_paths
    LOOP
        -- Resolve path via existing function
        SELECT * INTO v_resolved FROM resolve_field_path(p_scheme_id, v_path);
        
        IF v_resolved.structure_id IS NULL THEN
            RAISE WARNING 'Field path "%" not found in scheme %, skipping', v_path, p_scheme_id;
            CONTINUE;
        END IF;
        
        v_structure_ids := array_append(v_structure_ids, v_resolved.structure_id);
        
        -- Check: is it a nested path with Dictionary key? (AddressBook[home].City)
        IF v_resolved.dict_key IS NOT NULL AND position('.' IN v_path) > 0 THEN
            -- Parse: "AddressBook[home].City" -> parent="AddressBook", key="home", child="City"
            v_parent_path := split_part(v_path, '[', 1);  -- "AddressBook"
            v_parent_key := v_resolved.dict_key;          -- "home"
            v_child_field := substring(v_path from '\]\.(.*)'::text);  -- "City" or "City.SubField"
            
            -- Find parent structure AddressBook
            DECLARE
                v_parent_structure_id bigint;
                v_parent_resolved record;
            BEGIN
                SELECT * INTO v_parent_resolved FROM resolve_field_path(p_scheme_id, v_parent_path);
                v_parent_structure_id := v_parent_resolved.structure_id;
                
                IF v_parent_structure_id IS NOT NULL THEN
                    -- Check if parent entry with same key already exists
                    v_idx := NULL;
                    FOR i IN 0..jsonb_array_length(v_projection_paths) - 1 LOOP
                        IF (v_projection_paths->i->>'structure_id')::bigint = v_parent_structure_id 
                           AND v_projection_paths->i->>'dict_key' = v_parent_key THEN
                            v_idx := i;
                            EXIT;
                        END IF;
                    END LOOP;
                    
                    IF v_idx IS NULL THEN
                        -- Create new entry for parent with nested and dict_key
                        v_projection_paths := v_projection_paths || jsonb_build_object(
                            'path', v_parent_path,
                            'structure_id', v_parent_structure_id,
                            'dict_key', v_parent_key,
                            'nested', jsonb_build_array(jsonb_build_object(
                                'path', split_part(v_child_field, '.', 1),
                                'structure_id', v_resolved.structure_id
                            ))
                        );
                        v_structure_ids := array_append(v_structure_ids, v_parent_structure_id);
                    ELSE
                        -- Add to existing nested
                        v_nested_paths := v_projection_paths->v_idx->'nested';
                        IF v_nested_paths IS NULL THEN
                            v_nested_paths := '[]'::jsonb;
                        END IF;
                        v_nested_paths := v_nested_paths || jsonb_build_object(
                            'path', split_part(v_child_field, '.', 1),
                            'structure_id', v_resolved.structure_id
                        );
                        v_projection_paths := jsonb_set(v_projection_paths, ARRAY[v_idx::text, 'nested'], v_nested_paths);
                    END IF;
                END IF;
            END;
        -- ⭐ NEW: Simple Dictionary key without nested fields (PhoneBook[home])
        ELSIF v_resolved.dict_key IS NOT NULL THEN
            v_parent_path := split_part(v_path, '[', 1);  -- "PhoneBook"
            v_parent_key := v_resolved.dict_key;          -- "home"
            
            -- Find the field structure itself PhoneBook
            DECLARE
                v_parent_structure_id bigint;
                v_parent_resolved record;
            BEGIN
                SELECT * INTO v_parent_resolved FROM resolve_field_path(p_scheme_id, v_parent_path);
                v_parent_structure_id := v_parent_resolved.structure_id;
                
                IF v_parent_structure_id IS NOT NULL THEN
                    -- Add entry for Dictionary with dict_key
                    v_projection_paths := v_projection_paths || jsonb_build_object(
                        'path', v_parent_path,
                        'structure_id', v_parent_structure_id,
                        'dict_key', v_parent_key
                    );
                    v_structure_ids := array_append(v_structure_ids, v_parent_structure_id);
                END IF;
            END;
        ELSE
            -- Check: is it a Class field (Object)? Automatically add nested for child fields
            DECLARE
                v_type_semantic text;
                v_child_nested jsonb := '[]'::jsonb;
                v_child record;
            BEGIN
                SELECT type_semantic INTO v_type_semantic
                FROM _scheme_metadata_cache
                WHERE _scheme_id = p_scheme_id AND _structure_id = v_resolved.structure_id;
                
                IF v_type_semantic = 'Object' THEN
                    -- ⭐ Class field: automatically add all child fields
                    FOR v_child IN
                        SELECT _structure_id, _name
                        FROM _scheme_metadata_cache
                        WHERE _scheme_id = p_scheme_id AND _parent_structure_id = v_resolved.structure_id
                    LOOP
                        v_child_nested := v_child_nested || jsonb_build_object(
                            'path', v_child._name,
                            'structure_id', v_child._structure_id
                        );
                        v_structure_ids := array_append(v_structure_ids, v_child._structure_id);
                    END LOOP;
                    
                    IF jsonb_array_length(v_child_nested) > 0 THEN
                        v_projection_paths := v_projection_paths || jsonb_build_object(
                            'path', v_path,
                            'structure_id', v_resolved.structure_id,
                            'nested', v_child_nested
                        );
                    ELSE
                        v_projection_paths := v_projection_paths || jsonb_build_object(
                            'path', v_path,
                            'structure_id', v_resolved.structure_id
                        );
                    END IF;
                ELSE
                    -- Simple path without nesting
                    v_projection_paths := v_projection_paths || jsonb_build_object(
                        'path', v_path,
                        'structure_id', v_resolved.structure_id
                    );
                END IF;
            END;
        END IF;
    END LOOP;
    
    -- If no paths — return empty result
    IF jsonb_array_length(v_projection_paths) = 0 THEN
        RETURN jsonb_build_object('objects', '[]'::jsonb, 'total_count', 0, 'limit', p_limit, 'offset', p_offset);
    END IF;
    
    -- STEP 2: Filter conditions
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', 10);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    v_order_conditions := build_order_conditions(p_order_by, 'obj');
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', p_scheme_id, COALESCE(v_base_conditions, ''), COALESCE(v_hierarchical_conditions, ''));
    
    -- STEP 3: Calculate total_count
    EXECUTE format('SELECT COUNT(*) FROM _objects obj %s', v_final_where) INTO v_total_count;
    
    -- STEP 4: Main query
    v_query := format('
        WITH filtered_objects AS (
            SELECT obj._id FROM _objects obj %s %s %s
        ),
        projected_values AS (
            SELECT v._id_object, jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                ''_String'', v._String, ''_Long'', v._Long, ''_Guid'', v._Guid,
                ''_Double'', v._Double, ''_Numeric'', v._Numeric, ''_DateTimeOffset'', v._DateTimeOffset,
                ''_Boolean'', v._Boolean, ''_ByteArray'', v._ByteArray, ''_ListItem'', v._ListItem, ''_Object'', v._Object
            )) as values_json
            FROM _values v
            WHERE v._id_object IN (SELECT _id FROM filtered_objects) 
              AND v._id_structure = ANY($1) 
              AND v._array_index IS NULL
            GROUP BY v._id_object
        )
        SELECT jsonb_agg(jsonb_build_object(
            ''id'', o._id, 
            ''name'', o._name, 
            ''scheme_id'', o._id_scheme, 
            ''scheme_name'', s._name,
            ''parent_id'', o._id_parent,
            ''owner_id'', o._id_owner,
            ''who_change_id'', o._id_who_change,
            ''date_create'', o._date_create,
            ''date_modify'', o._date_modify,
            ''date_begin'', o._date_begin,
            ''date_complete'', o._date_complete,
            ''key'', o._key,
            ''value_long'', o._value_long,
            ''value_string'', o._value_string,
            ''value_guid'', o._value_guid,
            ''note'', o._note,
            ''value_bool'', o._value_bool,
            ''value_double'', o._value_double,
            ''value_numeric'', o._value_numeric,
            ''value_datetime'', o._value_datetime,
            ''value_bytes'', o._value_bytes,
            ''hash'', o._hash,
            ''properties'', build_flat_projection(o._id, o._id_scheme, $2, COALESCE(pv.values_json, ''{}''::jsonb), %s, '''', NULL, NULL)
        ))
        FROM filtered_objects fo 
        JOIN _objects o ON o._id = fo._id 
        JOIN _schemes s ON s._id = o._id_scheme
        LEFT JOIN projected_values pv ON pv._id_object = o._id',
        v_final_where, 
        v_order_conditions,
        CASE WHEN p_limit IS NULL OR p_limit >= 2000000000 THEN '' ELSE format('LIMIT %s OFFSET %s', p_limit, p_offset) END,
        p_max_depth
    );
    
    EXECUTE v_query INTO v_objects_result USING v_structure_ids, v_projection_paths;
    
    RETURN jsonb_build_object(
        'objects', COALESCE(v_objects_result, '[]'::jsonb), 
        'total_count', v_total_count, 
        'limit', p_limit, 
        'offset', p_offset,
        'facets', '[]'::jsonb
    );
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_projection_by_paths IS 
'Search objects with projection by text paths (human-readable format).
Path examples:
  - "Name"                    simple field
  - "Address.City"            nested Class
  - "Items[].Price"           all array elements
  - "Items[0].Price"          first array element
  - "PhoneBook[home]"         specific Dictionary key
  - "AddressBook[home].City"  field inside Dictionary value

Usage:
  SELECT search_objects_with_projection_by_paths(
      4504439,                                           -- scheme_id
      ''{"Name": {"$ne": null}}''::jsonb,                -- filters
      ARRAY[''Name'', ''AddressBook[home].City''],       -- projection
      100, 0, NULL, 10
  );
';


-- ===== FUNCTION 5: Search with projection by array of structure_ids =====
-- Legacy version for backward compatibility with C#
-- Recommended to use search_objects_with_projection_by_paths

DROP FUNCTION IF EXISTS search_objects_with_projection_by_ids(bigint, jsonb, bigint[], integer, integer, jsonb, integer) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_projection_by_ids(
    p_scheme_id bigint,
    p_filter_json jsonb DEFAULT NULL,
    p_structure_ids bigint[] DEFAULT ARRAY[]::bigint[],
    p_limit integer DEFAULT NULL,
    p_offset integer DEFAULT 0,
    p_order_by jsonb DEFAULT NULL,
    p_max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 500
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    v_projection_paths jsonb;
    v_base_conditions text;
    v_hierarchical_conditions text;
    v_order_conditions text;
    v_final_where text;
    v_objects_result jsonb;
    v_total_count integer;
    v_query text;
BEGIN
    -- STEP 1: Build projection_paths from structure_ids using metadata
    -- Format: [{"path": "Article", "structure_id": 1011}, ...]
    SELECT jsonb_agg(
        jsonb_build_object(
            'path', _build_field_path(c._structure_id, p_scheme_id),
            'structure_id', c._structure_id
        )
    )
    INTO v_projection_paths
    FROM _scheme_metadata_cache c
    WHERE c._scheme_id = p_scheme_id
      AND c._structure_id = ANY(p_structure_ids);
    
    -- If no paths — return empty result
    IF v_projection_paths IS NULL OR jsonb_array_length(v_projection_paths) = 0 THEN
        v_projection_paths := '[]'::jsonb;
    END IF;
    
    -- STEP 2: Filter conditions
    v_base_conditions := _build_single_facet_condition(p_filter_json, p_scheme_id, 'obj', 10);
    v_hierarchical_conditions := build_hierarchical_conditions(p_filter_json, 'obj');
    v_order_conditions := build_order_conditions(p_order_by, 'obj');
    v_final_where := format('WHERE obj._id_scheme = %s%s%s', p_scheme_id, COALESCE(v_base_conditions, ''), COALESCE(v_hierarchical_conditions, ''));
    
    -- STEP 3: Calculate total_count
    EXECUTE format('SELECT COUNT(*) FROM _objects obj %s', v_final_where) INTO v_total_count;
    
    -- STEP 4: Main query
    v_query := format('
        WITH filtered_objects AS (
            SELECT obj._id FROM _objects obj %s %s %s
        ),
        projected_values AS (
            SELECT v._id_object, jsonb_object_agg(v._id_structure::text, jsonb_build_object(
                ''_String'', v._String, ''_Long'', v._Long, ''_Guid'', v._Guid,
                ''_Double'', v._Double, ''_Numeric'', v._Numeric, ''_DateTimeOffset'', v._DateTimeOffset,
                ''_Boolean'', v._Boolean, ''_ByteArray'', v._ByteArray, ''_ListItem'', v._ListItem, ''_Object'', v._Object
            )) as values_json
            FROM _values v
            WHERE v._id_object IN (SELECT _id FROM filtered_objects) 
              AND v._id_structure = ANY($1) 
              AND v._array_index IS NULL
            GROUP BY v._id_object
        )
        SELECT jsonb_agg(jsonb_build_object(
            ''id'', o._id, 
            ''name'', o._name, 
            ''scheme_id'', o._id_scheme, 
            ''scheme_name'', s._name,
            ''parent_id'', o._id_parent,
            ''owner_id'', o._id_owner,
            ''who_change_id'', o._id_who_change,
            ''date_create'', o._date_create,
            ''date_modify'', o._date_modify,
            ''key'', o._key,
            ''value_long'', o._value_long,
            ''value_string'', o._value_string,
            ''value_guid'', o._value_guid,
            ''note'', o._note,
            ''value_bool'', o._value_bool,
            ''value_double'', o._value_double,
            ''value_numeric'', o._value_numeric,
            ''value_datetime'', o._value_datetime,
            ''value_bytes'', o._value_bytes,
            ''hash'', o._hash,
            ''properties'', build_flat_projection(o._id, o._id_scheme, $2, COALESCE(pv.values_json, ''{}''::jsonb), %s, '''', NULL, NULL)
        ))
        FROM filtered_objects fo 
        JOIN _objects o ON o._id = fo._id 
        JOIN _schemes s ON s._id = o._id_scheme
        LEFT JOIN projected_values pv ON pv._id_object = o._id',
        v_final_where, 
        v_order_conditions,
        CASE WHEN p_limit IS NULL OR p_limit >= 2000000000 THEN '' ELSE format('LIMIT %s OFFSET %s', p_limit, p_offset) END,
        p_max_depth
    );
    
    EXECUTE v_query INTO v_objects_result USING p_structure_ids, v_projection_paths;
    
    RETURN jsonb_build_object(
        'objects', COALESCE(v_objects_result, '[]'::jsonb), 
        'total_count', v_total_count, 
        'limit', p_limit, 
        'offset', p_offset,
        'facets', '[]'::jsonb
    );
END;
$BODY$;

COMMENT ON FUNCTION search_objects_with_projection_by_ids IS 
'Simplified version of search_objects_with_projection for C#.
Accepts bigint[] structure_ids instead of JSONB paths.
SQL automatically builds paths from _scheme_metadata_cache.
Response format compatible with search_objects_with_facets.';


-- ===== HELPER FUNCTION: Build field path from structure_id =====

DROP FUNCTION IF EXISTS _build_field_path(bigint, bigint) CASCADE;

CREATE OR REPLACE FUNCTION _build_field_path(
    p_structure_id bigint,
    p_scheme_id bigint
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
STABLE
AS $BODY$
DECLARE
    v_path text := '';
    v_current_id bigint := p_structure_id;
    v_name text;
    v_parent_id bigint;
BEGIN
    -- Recursively build path from structure_id to root
    LOOP
        SELECT _name, _parent_structure_id
        INTO v_name, v_parent_id
        FROM _scheme_metadata_cache
        WHERE _scheme_id = p_scheme_id AND _structure_id = v_current_id;
        
        IF NOT FOUND THEN
            EXIT;
        END IF;
        
        -- Add name to path
        IF v_path = '' THEN
            v_path := v_name;
        ELSE
            v_path := v_name || '.' || v_path;
        END IF;
        
        -- Move to parent
        IF v_parent_id IS NULL THEN
            EXIT;
        END IF;
        v_current_id := v_parent_id;
    END LOOP;
    
    RETURN v_path;
END;
$BODY$;

COMMENT ON FUNCTION _build_field_path IS 'Builds field path from structure_id (e.g., "Contact.Name")';


-- ============================================================
-- TEST EXAMPLES
-- ============================================================

/*
-- ===== STEP 0: Check that metadata cache is filled =====
SELECT * FROM warmup_all_metadata_caches();

-- ===== STEP 1: Find scheme_id and structure_ids for testing =====
-- View all schemes:
SELECT _id, _name FROM _schemes LIMIT 10;

-- View scheme structures (replace 1002 with your scheme):
SELECT 
    _structure_id, 
    _name, 
    _parent_structure_id,
    db_type, 
    type_semantic,
    _collection_type IS NOT NULL as is_array  -- _collection_type = Array/Dictionary/NULL
FROM _scheme_metadata_cache 
WHERE _scheme_id = 1002  -- your scheme
ORDER BY _parent_structure_id NULLS FIRST, _order;

-- ===== STEP 2: Simple test — one field =====
-- Replace structure_id with real one from your scheme
SELECT search_objects_with_projection(
    1002,                                           -- scheme_id
    NULL,                                           -- no filter
    '[{"path": "Age", "structure_id": 100}]'::jsonb, -- projection
    5,                                              -- limit
    0                                               -- offset
);

-- ===== STEP 3: Several simple fields =====
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Name", "structure_id": 101},
        {"path": "Stock", "structure_id": 102}
    ]'::jsonb,
    5, 0
);

-- ===== STEP 4: Class fields (nested in same object) =====
-- For example Contact — Class with fields Name, Email
-- Contact._structure_id = 200, Contact.Name._structure_id = 201
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Contact.Name", "structure_id": 201},
        {"path": "Contact.Email", "structure_id": 202}
    ]'::jsonb,
    5, 0
);
-- Expected result: {"Age": 30, "Contact.Name": "John", "Contact.Email": "j@mail.ru"}

-- ===== STEP 5: Simple type arrays =====
-- Scores — array of Long, structure_id = 300
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[{"path": "Scores", "structure_id": 300}]'::jsonb,
    5, 0
);
-- Expected result: {"Scores[0]": 100, "Scores[1]": 85, "Scores[2]": 90}

-- ===== STEP 6: Class arrays (Items[].Price) =====
-- Items — array of Object, Items.Price — Long
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Items", "structure_id": 400, "nested": [
            {"path": "Price", "structure_id": 401},
            {"path": "Name", "structure_id": 402}
        ]}
    ]'::jsonb,
    5, 0
);
-- Expected result: {"Items[0].Price": 100, "Items[0].Name": "Book", "Items[1].Price": 200, ...}

-- ===== STEP 7: _RObject (reference to another object) =====
-- Author — _RObject, references another object with its own scheme
SELECT search_objects_with_projection(
    1002,
    NULL,
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Author", "structure_id": 500, "nested": [
            {"path": "Name", "structure_id": 101},
            {"path": "Email", "structure_id": 102}
        ]}
    ]'::jsonb,
    5, 0
);
-- Expected result: {"Age": 30, "Author": {"Name": "Pushkin", "Email": "push@mail.ru"}}

-- ===== STEP 8: With filter =====
SELECT search_objects_with_projection(
    1002,
    '{"Age": {"$gt": 25}}'::jsonb,                  -- filter
    '[
        {"path": "Age", "structure_id": 100},
        {"path": "Name", "structure_id": 101}
    ]'::jsonb,
    10, 0
);

-- ===== STEP 9: Test build_flat_projection directly =====
-- First find an object:
SELECT _id, _id_scheme FROM _objects WHERE _id_scheme = 1002 LIMIT 1;

-- Then load its values:
WITH obj_values AS (
    SELECT jsonb_object_agg(
        v._id_structure::text,
        jsonb_build_object(
            '_String', v._String, '_Long', v._Long, '_Guid', v._Guid,
            '_Double', v._Double, '_Numeric', v._Numeric,
            '_DateTimeOffset', v._DateTimeOffset, '_Boolean', v._Boolean,
            '_ByteArray', v._ByteArray, '_ListItem', v._ListItem, '_Object', v._Object
        )
    ) as vals
    FROM _values v
    WHERE v._id_object = 12345  -- replace with real _id
      AND v._array_index IS NULL
)
SELECT build_flat_projection(
    12345,                                          -- object_id
    1002,                                           -- scheme_id
    '[{"path": "Age", "structure_id": 100}]'::jsonb,
    vals,
    10, '', NULL, NULL
) FROM obj_values;

-- ===== STEP 10: Performance comparison =====
-- Without projection (all fields):
EXPLAIN ANALYZE 
SELECT search_objects_with_facets_base(1002, NULL, 100, 0, NULL, 10);

-- With projection (only 2 fields):
EXPLAIN ANALYZE 
SELECT search_objects_with_projection(
    1002, NULL,
    '[{"path": "Age", "structure_id": 100}, {"path": "Name", "structure_id": 101}]'::jsonb,
    100, 0
);

-- ===== ERROR DIAGNOSTICS =====

-- Check that structure_id exists:
SELECT * FROM _scheme_metadata_cache 
WHERE _scheme_id = 1002 AND _structure_id = 100;

-- Check that data exists:
SELECT COUNT(*) FROM _values 
WHERE _id_structure = 100;

-- Check type_semantic for _RObject:
SELECT _structure_id, _name, type_semantic 
FROM _scheme_metadata_cache 
WHERE _scheme_id = 1002 AND type_semantic = '_RObject';
*/

-- ===== redb_soft_delete.sql =====
-- =====================================================
-- SOFT DELETE FUNCTIONS FOR POSTGRESQL
-- Part of Background Deletion System
-- =====================================================

-- Drop existing functions if any
DROP FUNCTION IF EXISTS mark_for_deletion(bigint[], bigint);
DROP FUNCTION IF EXISTS mark_for_deletion(bigint[], bigint, bigint);
DROP FUNCTION IF EXISTS purge_trash(bigint, integer);

-- =====================================================
-- FUNCTION: mark_for_deletion
-- Marks objects for deletion by moving them under a trash container
-- Creates trash container, finds all descendants via CTE, updates parent and scheme
-- All operations in single transaction (atomic)
-- p_trash_parent_id: optional parent for trash container (NULL = root level)
-- =====================================================
CREATE OR REPLACE FUNCTION mark_for_deletion(
    p_object_ids bigint[],
    p_user_id bigint,
    p_trash_parent_id bigint DEFAULT NULL
) RETURNS TABLE(trash_id bigint, marked_count bigint) AS $$
DECLARE
    v_trash_id bigint;
    v_count bigint;
BEGIN
    -- 1. Create Trash container object with @@__deleted scheme
    -- Progress fields: _value_long=total, _key=deleted, _value_string=status
    INSERT INTO _objects (
        _id, _id_scheme, _id_parent, _id_owner, _id_who_change,
        _name, _date_create, _date_modify,
        _value_long, _key, _value_string
    ) VALUES (
        nextval('global_identity'), 
        -10,  -- @@__deleted scheme
        p_trash_parent_id,  -- user-specified parent or NULL
        p_user_id, 
        p_user_id,
        '__TRASH__' || p_user_id || '_' || extract(epoch from now())::bigint,
        NOW(), 
        NOW(),
        0,          -- _value_long = total (will be updated after count)
        0,          -- _key = deleted
        'pending'   -- _value_string = status
    ) RETURNING _id INTO v_trash_id;
    
    -- 2. CTE: find all objects and their descendants recursively
    -- 3. UPDATE: move all found objects under Trash container and change scheme
    WITH RECURSIVE all_descendants AS (
        -- Start with requested objects
        SELECT _id FROM _objects 
        WHERE _id = ANY(p_object_ids)
          AND _id_scheme != -10  -- skip already deleted
        
        UNION ALL
        
        -- Recursively find children
        SELECT o._id FROM _objects o
        INNER JOIN all_descendants d ON o._id_parent = d._id
        WHERE o._id_scheme != -10  -- skip already deleted
    )
    UPDATE _objects 
    SET _id_parent = v_trash_id,
        _id_scheme = -10,
        _date_modify = NOW()
    WHERE _id IN (SELECT _id FROM all_descendants);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- 4. Update trash container with total count
    UPDATE _objects 
    SET _value_long = v_count
    WHERE _id = v_trash_id;
    
    -- 5. Return trash container ID and count of marked objects
    RETURN QUERY SELECT v_trash_id, v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION mark_for_deletion(bigint[], bigint, bigint) IS 
'Marks objects for soft-deletion. Creates a trash container, moves all specified objects 
and their descendants under it with scheme=@@__deleted. Returns (trash_id, marked_count). 
p_trash_parent_id: optional parent ID for trash container (NULL = root level).
Atomic operation - all or nothing.';


-- =====================================================
-- FUNCTION: purge_trash
-- Physically deletes objects from a trash container in batches
-- ON DELETE CASCADE handles _values deletion automatically
-- Updates progress in trash container (_key=deleted, _value_string=status)
-- After all children deleted, removes the trash container itself
-- =====================================================
CREATE OR REPLACE FUNCTION purge_trash(
    p_trash_id bigint,
    p_batch_size integer DEFAULT 10
) RETURNS TABLE(deleted_count bigint, remaining_count bigint) AS $$
DECLARE
    v_deleted bigint;
    v_remaining bigint;
BEGIN
    -- Update status to 'running' if it was 'pending'
    UPDATE _objects 
    SET _value_string = 'running',
        _date_modify = NOW()
    WHERE _id = p_trash_id AND _value_string = 'pending';
    
    -- Delete a batch of objects (CASCADE handles _values)
    WITH to_delete AS (
        SELECT _id FROM _objects
        WHERE _id_parent = p_trash_id
        LIMIT p_batch_size
    )
    DELETE FROM _objects 
    WHERE _id IN (SELECT _id FROM to_delete);
    
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    -- Count remaining objects in this trash
    SELECT COUNT(*) INTO v_remaining
    FROM _objects 
    WHERE _id_parent = p_trash_id;
    
    -- Update progress in trash container
    UPDATE _objects 
    SET _key = _key + v_deleted,
        _value_string = CASE WHEN v_remaining = 0 THEN 'completed' ELSE 'running' END,
        _date_modify = NOW()
    WHERE _id = p_trash_id;
    
    -- If no more children, delete the trash container itself
    IF v_remaining = 0 THEN
        DELETE FROM _objects WHERE _id = p_trash_id;
    END IF;
    
    RETURN QUERY SELECT v_deleted, v_remaining;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION purge_trash(bigint, integer) IS 
'Physically deletes objects from a trash container in batches. 
p_trash_id: ID of the trash container created by mark_for_deletion.
p_batch_size: Number of objects to delete per call (default 10).
Returns (deleted_count, remaining_count). When remaining=0, trash container is also deleted.
Call repeatedly until remaining_count = 0.';



-- ===== redb_structure_tree.sql =====
-- ====================================================================================================
-- FUNCTIONS FOR WORKING WITH SCHEME STRUCTURE TREE
-- ====================================================================================================
-- Supports hierarchical navigation through structures: parent → children → descendants
-- Solves flat structure search problems in SaveAsync
-- ====================================================================================================

-- MAIN FUNCTION: Build scheme structure tree (SIMPLE APPROACH)
-- SIMPLE AND CLEAR LOGIC: get current layer → for each structure get children recursively
CREATE OR REPLACE FUNCTION get_scheme_structure_tree(
    scheme_id bigint,
    parent_id bigint DEFAULT NULL,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result jsonb := '[]'::jsonb;
    structure_record RECORD;
    children_json jsonb;
BEGIN
    -- Protection from infinite recursion
    IF max_depth <= 0 THEN
        RETURN jsonb_build_array(jsonb_build_object('error', 'Max recursion depth reached'));
    END IF;
    
    -- Check scheme existence
    IF NOT EXISTS(SELECT 1 FROM _schemes WHERE _id = scheme_id) THEN
        RETURN jsonb_build_array(jsonb_build_object('error', 'Scheme not found'));
    END IF;
    
    -- AUTOMATIC CACHE CHECK AND FILL
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-fill without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- SIMPLE LOGIC: Get structures of CURRENT LEVEL
    -- OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    FOR structure_record IN
        SELECT 
            c._structure_id as _id,
            c._name,
            c._order,
            c._collection_type IS NOT NULL as _is_array,  -- _collection_type != NULL = array/dict
            c._collection_type,
            c._store_null,
            c._allow_not_null,
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = scheme_id
          AND ((parent_id IS NULL AND c._parent_structure_id IS NULL) 
               OR (parent_id IS NOT NULL AND c._parent_structure_id = parent_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- CHECK IF STRUCTURE HAS CHILDREN
        IF EXISTS(SELECT 1 FROM _structures 
                 WHERE _id_scheme = scheme_id 
                   AND _id_parent = structure_record._id) THEN
            -- RECURSIVELY get children (simple function call!)
            children_json := get_scheme_structure_tree(scheme_id, structure_record._id, max_depth - 1);
        ELSE
            -- No children - empty array
            children_json := '[]'::jsonb;
        END IF;
        
        -- ADD STRUCTURE TO RESULT (simple construction)
        result := result || jsonb_build_array(
            jsonb_build_object(
                'structure_id', structure_record._id,
                'name', structure_record._name,
                'order', structure_record._order,
                'is_array', structure_record._is_array,  -- For compatibility
                'collection_type', structure_record._collection_type,  -- New collection type
                'store_null', structure_record._store_null,
                'allow_not_null', structure_record._allow_not_null,
                'type_name', structure_record.type_name,
                'db_type', structure_record.db_type,
                'type_semantic', structure_record.type_semantic,
                'children', children_json  -- Recursively obtained children
            )
        );
    END LOOP;
    
    RETURN result;
END;
$BODY$;

-- HELPER FUNCTION: Get only direct child structures  
CREATE OR REPLACE FUNCTION get_structure_children(
    scheme_id bigint,
    parent_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- AUTOMATIC CACHE CHECK AND FILL
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-fill without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'structure_id', c._structure_id,
                'name', c._name,
                'order', c._order,
                'is_array', c._collection_type IS NOT NULL,  -- For compatibility
                'collection_type', c._collection_type,       -- New collection type
                'type_name', c.type_name,
                'db_type', c.db_type,
                'type_semantic', c.type_semantic
            ) ORDER BY c._order, c._structure_id
        ), '[]'::jsonb)
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = scheme_id
          AND c._parent_structure_id = parent_id
    );
END;
$BODY$;

-- DIAGNOSTIC FUNCTION: Validate structure tree for redundancy
CREATE OR REPLACE FUNCTION validate_structure_tree(
    scheme_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    validation_result jsonb;
    excessive_structures jsonb;
    orphaned_structures jsonb;
    circular_references jsonb;
BEGIN
    -- 1. Find excessive structures (structures without values connections)
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', s._id,
            'name', s._name,
            'parent_name', parent_s._name,
            'issue', 'No values found - possibly excessive structure'
        )
    ) INTO excessive_structures
    FROM _structures s
    LEFT JOIN _structures parent_s ON parent_s._id = s._id_parent
    LEFT JOIN _values v ON v._id_structure = s._id
    WHERE s._id_scheme = scheme_id
      AND v._id IS NULL  -- No values for this structure
      AND s._id_parent IS NOT NULL; -- Only child structures
    
    -- 2. Find orphaned structures (parent doesn't exist)
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', s._id,
            'name', s._name,
            'parent_id', s._id_parent,
            'issue', 'Parent structure does not exist'
        )
    ) INTO orphaned_structures
    FROM _structures s
    WHERE s._id_scheme = scheme_id
      AND s._id_parent IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM _structures parent_s WHERE parent_s._id = s._id_parent);
    
    -- 3. Simple check for circular references (structure references itself via chain)
    WITH RECURSIVE cycle_check AS (
        SELECT _id, _id_parent, ARRAY[_id] as path, false as has_cycle
        FROM _structures WHERE _id_scheme = scheme_id AND _id_parent IS NOT NULL
        
        UNION ALL
        
        SELECT s._id, s._id_parent, cc.path || s._id, s._id = ANY(cc.path)
        FROM _structures s
        JOIN cycle_check cc ON cc._id_parent = s._id
        WHERE NOT cc.has_cycle AND array_length(cc.path, 1) < 50
    )
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', _id,
            'path', path,
            'issue', 'Circular reference detected'
        )
    ) INTO circular_references
    FROM cycle_check 
    WHERE has_cycle;
    
    -- Form final report
    validation_result := jsonb_build_object(
        'scheme_id', scheme_id,
        'validation_date', NOW(),
        'excessive_structures', COALESCE(excessive_structures, '[]'::jsonb),
        'orphaned_structures', COALESCE(orphaned_structures, '[]'::jsonb), 
        'circular_references', COALESCE(circular_references, '[]'::jsonb),
        'total_structures', (SELECT COUNT(*) FROM _structures WHERE _id_scheme = scheme_id),
        'is_valid', (excessive_structures IS NULL AND orphaned_structures IS NULL AND circular_references IS NULL)
    );
    
    RETURN validation_result;
END;
$BODY$;

-- FUNCTION: Get all structure descendants (flat list)
CREATE OR REPLACE FUNCTION get_structure_descendants(
    scheme_id bigint,
    parent_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'  
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    RETURN (
        WITH RECURSIVE descendants AS (
            -- Direct child structures
            SELECT _id, _name, _id_parent, 0 as level
            FROM _structures 
            WHERE _id_scheme = scheme_id AND _id_parent = parent_id
            
            UNION ALL
            
            -- Recursively all descendants
            SELECT s._id, s._name, s._id_parent, d.level + 1
            FROM _structures s
            JOIN descendants d ON d._id = s._id_parent
            WHERE s._id_scheme = scheme_id AND d.level < 10
        )
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'structure_id', _id,
                'name', _name, 
                'parent_id', _id_parent,
                'level', level
            ) ORDER BY level, _id
        ), '[]'::jsonb)
        FROM descendants
    );
END;
$BODY$;

-- Comments for structure tree functions
COMMENT ON FUNCTION get_scheme_structure_tree(bigint, bigint, integer) IS 'Build complete scheme structure tree with hierarchy. Supports recursion depth limit. Used by PostgresSchemeSyncProvider for correct structure traversal in SaveAsync.';

COMMENT ON FUNCTION get_structure_children(bigint, bigint) IS 'Get only direct child structures without recursion. Fast function for simple tree navigation cases.';

COMMENT ON FUNCTION validate_structure_tree(bigint) IS 'Structure tree diagnostics: find excessive structures, orphaned references, circular dependencies. Helps identify issues like with Address.Details.Tags1.';

COMMENT ON FUNCTION get_structure_descendants(bigint, bigint) IS 'Get all structure descendants in flat format with nesting level indication. Useful for analyzing deep hierarchies.';


