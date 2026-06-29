/*
** REDB SQLite Free — v2-pvt SQL-generation engine (C port).
**
** Faithful port of redb.SQLite/sql/v2-pvt/*.sql (PG PL/pgSQL = the logic spec)
** producing SQLite-valid SQL. The engine builds query SQL *strings* from JSON
** specs; it does NOT execute them — the caller wraps the result (count / exists
** / get_object_json / preview). Input JSON is parsed with SQLite's built-in
** JSON1 (json_extract / json_type / json_each).
**
** SQLite-syntax reference: where the PG spec uses constructs SQLite lacks, the
** emitted SQL mirrors the Pro SQLite builder (redb.SQLite.Pro), which is the
** proven SQLite shape — we copy the SHAPE, not the parameterization (the pvt
** engine inlines literals via quote_literal; Pro binds $N):
**   PG  (array_agg(col) FILTER (cond))[1]   -> SQLite  MAX(col) FILTER (cond)
**   PG  array_agg(col) FILTER (cond)        -> SQLite  json_group_array(col) FILTER (cond)
**
** Public entry points (mirroring the PG module):
**   pvt_module_version()                         -> TEXT (semver)
**   pvt_db_type_to_value_column(db_type)         -> TEXT
**   pvt_build_column_expr(name, meta[, aio])     -> TEXT
**   pvt_build_query_sql(scheme, filter, ...)     -> TEXT   [later layers]
**
** STATUS: porting layer by layer (see project memory M3 plan). Live so far:
**   module_version, db_type_to_value_column, build_column_expr.
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "redb_ext.h"
#include "redb_pvt.h"
#include <string.h>

#ifndef PVT_MODULE_VERSION
#define PVT_MODULE_VERSION "0.6.2"
#endif

/* ------------------------------------------------------------------------- */
/* Small helpers                                                             */
/* ------------------------------------------------------------------------- */

/* json bool at path -> 0/1 (json_extract returns integer 1/0 for true/false). */
int jsonGetBool(sqlite3 *db, const char *json, const char *path){
  int found = 0;
  sqlite3_int64 v = jsonGetInt(db, json, path, &found);
  return found && v != 0;
}

/* 17_pvt_expr.sql forward declarations. pvtBuildScalarExpr/BoolExpr (+ helpers,
** + pvtBuildWindowExpr) are declared in redb_pvt.h; only the TU-local statics
** are forward-declared here. */
static char *pvtBuildExprPredicate(sqlite3 *db, const char *op, const char *args, const char *fields, const char *prefix);
static int   pvtExprIsBaseOnly(sqlite3 *db, const char *node, const char *fields);
static char *pvtBuildAggExpr(sqlite3 *db, const char *entry, const char *fields, const char *prefix);

/* ------------------------------------------------------------------------- */
/* 11_pvt_column_expr.sql                                                    */
/* ------------------------------------------------------------------------- */

/* db_type (as in _scheme_metadata_cache.db_type) -> physical _values column,
** or NULL if unsupported. Mirrors pvt_db_type_to_value_column. */
static const char *pvtDbTypeToValueColumn(const char *t){
  if(!t) return 0;
  if(!strcmp(t, "String"))         return "_String";
  if(!strcmp(t, "Long"))           return "_Long";
  if(!strcmp(t, "Double"))         return "_Double";
  if(!strcmp(t, "Numeric"))        return "_Numeric";
  if(!strcmp(t, "Boolean"))        return "_Boolean";
  if(!strcmp(t, "Guid"))           return "_Guid";
  if(!strcmp(t, "DateTimeOffset")) return "_DateTimeOffset";
  if(!strcmp(t, "ByteArray"))      return "_ByteArray";
  if(!strcmp(t, "ListItem"))       return "_ListItem";
  if(!strcmp(t, "Object"))         return "_Object";
  if(!strcmp(t, "TimeSpan"))       return "_String";          /* stored as text */
  if(!strcmp(t, "DateTime"))       return "_DateTimeOffset";
  return 0;
}

/* Build one SELECT-list pivot expression for a field. Returns a sqlite3_malloc'd
** string, or NULL on error (unsupported type / NULL meta / nested-dict misuse).
** Mirrors pvt_build_column_expr; SQLite shapes per the Pro builder.
**   p_aio (array_index_in_outer): drop `AND v._array_index IS NULL` from scalar
**   FILTERs (caller hoists it to an outer WHERE — Pro-parity inline GROUP BY). */
static char *pvtBuildColumnExpr(sqlite3 *db, const char *field_name,
                                const char *meta, int p_aio){
  if(!meta) return 0;

  char *kind     = jsonGetText(db, meta, "$.kind");
  char *li_prop  = jsonGetText(db, meta, "$.list_item_prop");
  char *dict_key = jsonGetText(db, meta, "$.dict_key");
  char *parent   = jsonGetText(db, meta, "$.parent_sid");        /* NULL if absent */
  int   is_array = jsonGetBool(db, meta, "$.is_array");

  char *out = 0;

  /* ---- Base field: straight projection from _objects. ---- */
  if(kind && !strcmp(kind, "base")){
    char *col = jsonGetText(db, meta, "$.column");
    if(col) out = sqlite3_mprintf("o.%s AS \"%w\"", col, field_name);
    sqlite3_free(col);
    goto done;
  }

  int found = 0;
  sqlite3_int64 sid = jsonGetInt(db, meta, "$.sid", &found);

  /* Nested-dict (parent_sid AND dict_key) must go through the side CTE. */
  if(parent && dict_key){ out = 0; goto done; }

  /* ---- ListItem.Value / .Alias (li join provided by the CTE builder). ---- */
  if(li_prop && (!strcmp(li_prop, "Value") || !strcmp(li_prop, "Alias"))){
    const char *lc = !strcmp(li_prop, "Value") ? "li._value" : "li._alias";
    if(is_array)
      out = sqlite3_mprintf(
        "json_group_array(%s) FILTER (WHERE v._id_structure = %lld AND v._array_index IS NOT NULL) AS \"%w\"",
        lc, (long long)sid, field_name);
    else if(p_aio)
      out = sqlite3_mprintf(
        "MAX(%s) FILTER (WHERE v._id_structure = %lld) AS \"%w\"",
        lc, (long long)sid, field_name);
    else
      out = sqlite3_mprintf(
        "MAX(%s) FILTER (WHERE v._id_structure = %lld AND v._array_index IS NULL) AS \"%w\"",
        lc, (long long)sid, field_name);
    goto done;
  }

  /* ---- Resolve typed column. ---- */
  char *db_type = jsonGetText(db, meta, "$.db_type");
  const char *col = pvtDbTypeToValueColumn(db_type ? db_type : "");
  if(!col){ sqlite3_free(db_type); out = 0; goto done; }
  if(li_prop && !strcmp(li_prop, "Id")) col = "_ListItem";   /* project the FK */
  sqlite3_free(db_type);

  /* ---- Simple dictionary: key selects the element (_array_index = '<key>'). ---- */
  if(dict_key && !parent){
    out = sqlite3_mprintf(
      "MAX(v.%s) FILTER (WHERE v._id_structure = %lld AND v._array_index = %Q) AS \"%w\"",
      col, (long long)sid, dict_key, field_name);
    goto done;
  }

  /* ---- Array pivot: SQLite json array (PG array_agg has no SQLite analog). ---- */
  if(is_array){
    out = sqlite3_mprintf(
      "json_group_array(v.%s) FILTER (WHERE v._id_structure = %lld AND v._array_index IS NOT NULL) AS \"%w\"",
      col, (long long)sid, field_name);
    goto done;
  }

  /* ---- Scalar pivot: MAX(...) FILTER (Pro shape; one row per scalar). ---- */
  if(p_aio)
    out = sqlite3_mprintf(
      "MAX(v.%s) FILTER (WHERE v._id_structure = %lld) AS \"%w\"",
      col, (long long)sid, field_name);
  else
    out = sqlite3_mprintf(
      "MAX(v.%s) FILTER (WHERE v._id_structure = %lld AND v._array_index IS NULL) AS \"%w\"",
      col, (long long)sid, field_name);

done:
  sqlite3_free(kind);
  sqlite3_free(li_prop);
  sqlite3_free(dict_key);
  sqlite3_free(parent);
  return out;
}

/* ------------------------------------------------------------------------- */
/* 01_pvt_field_path.sql (helpers used by the WHERE walker)                  */
/* ------------------------------------------------------------------------- */

/* pvt_peek_contains_key_value: string operand of a ContainsKey predicate
** (bare "key" or {"$eq":"key"}), else NULL. Returns sqlite3_malloc'd text. */
static char *pvtPeekContainsKeyValue(sqlite3 *db, const char *op_json, const char *op_type){
  if(!op_json) return 0;
  if(op_type && !strcmp(op_type, "text"))
    return sqlite3_mprintf("%s", op_json);     /* bare string operand */
  if(op_type && !strcmp(op_type, "object")){
    char *t = jsonTypeAt(db, op_json, "$.\"$eq\"");
    if(t && !strcmp(t, "text")){ sqlite3_free(t); return jsonGetText(db, op_json, "$.\"$eq\""); }
    sqlite3_free(t);
  }
  return 0;
}

/* pvt_normalize_field_name: rewrite `<Dict>.ContainsKey` + key -> `<Dict>[key]`.
** Returns a sqlite3_malloc'd path (rewritten or a copy of the input). */
static char *pvtNormalizeFieldName(const char *path, const char *op_value){
  if(!path) return 0;
  size_t pn = strlen(path), sn = strlen(".ContainsKey");
  if(pn < sn || strcmp(path + (pn - sn), ".ContainsKey") != 0 || !op_value || !*op_value)
    return sqlite3_mprintf("%s", path);
  return sqlite3_mprintf("%.*s[%s]", (int)(pn - sn), path, op_value);
}

/* pvt_normalize_base_field_name: C# base field name -> _objects column, or
** NULL if not a base field. '0$:' prefix forces the base mapping; otherwise a
** bare name only maps when it already starts with '_' (disambiguation vs
** user Props of the same name). Returns sqlite3_malloc'd text or NULL. */
static char *pvtNormalizeBaseFieldName(const char *field_name){
  if(!field_name) return 0;
  int had_prefix = 0;
  const char *n = field_name;
  if(strncmp(n, "0$:", 3) == 0){ had_prefix = 1; n += 3; }

  static const char *map[][2] = {
    {"id","_id"},{"Id","_id"},{"_id","_id"},
    {"parent_id","_id_parent"},{"ParentId","_id_parent"},{"id_parent","_id_parent"},{"_id_parent","_id_parent"},
    {"scheme_id","_id_scheme"},{"SchemeId","_id_scheme"},{"id_scheme","_id_scheme"},{"_id_scheme","_id_scheme"},
    {"owner_id","_id_owner"},{"OwnerId","_id_owner"},{"_id_owner","_id_owner"},
    {"who_change_id","_id_who_change"},{"WhoChangeId","_id_who_change"},{"_id_who_change","_id_who_change"},
    {"value_long","_value_long"},{"ValueLong","_value_long"},{"_value_long","_value_long"},
    {"value_string","_value_string"},{"ValueString","_value_string"},{"_value_string","_value_string"},
    {"value_guid","_value_guid"},{"ValueGuid","_value_guid"},{"_value_guid","_value_guid"},
    {"key","_key"},{"Key","_key"},{"_key","_key"},
    {"name","_name"},{"Name","_name"},{"_name","_name"},
    {"note","_note"},{"Note","_note"},{"_note","_note"},
    {"value_bool","_value_bool"},{"ValueBool","_value_bool"},{"_value_bool","_value_bool"},
    {"value_double","_value_double"},{"ValueDouble","_value_double"},{"_value_double","_value_double"},
    {"value_numeric","_value_numeric"},{"ValueNumeric","_value_numeric"},{"_value_numeric","_value_numeric"},
    {"value_datetime","_value_datetime"},{"ValueDatetime","_value_datetime"},{"_value_datetime","_value_datetime"},
    {"value_bytes","_value_bytes"},{"ValueBytes","_value_bytes"},{"_value_bytes","_value_bytes"},
    {"hash","_hash"},{"Hash","_hash"},{"_hash","_hash"},
    {"date_create","_date_create"},{"DateCreate","_date_create"},{"_date_create","_date_create"},
    {"date_modify","_date_modify"},{"DateModify","_date_modify"},{"_date_modify","_date_modify"},
    {"date_begin","_date_begin"},{"DateBegin","_date_begin"},{"_date_begin","_date_begin"},
    {"date_complete","_date_complete"},{"DateComplete","_date_complete"},{"_date_complete","_date_complete"},
    {0,0}
  };
  const char *col = 0;
  for(int i = 0; map[i][0]; i++) if(!strcmp(n, map[i][0])){ col = map[i][1]; break; }
  if(!col) return 0;
  /* disambiguation: honor only with prefix or leading underscore. */
  if(!had_prefix && n[0] != '_') return 0;
  return sqlite3_mprintf("%s", col);
}

/* Build a FieldInfo meta JSON via json_object (escaping handled by SQLite).
** NULL pointers -> JSON null; has_sid/has_parent gate sid/parent_sid. */
static char *pvtEmitMeta(sqlite3 *db, const char *kind, const char *column, const char *name,
                         int is_array, const char *li_prop, const char *dict_key,
                         int has_parent, sqlite3_int64 parent_sid,
                         int has_sid, sqlite3_int64 sid,
                         const char *db_type, const char *db_col){
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db,
    "SELECT json_object('kind',?1,'column',?2,'name',?3,'is_array',?4,"
    "'list_item_prop',?5,'dict_key',?6,'parent_sid',?7,'sid',?8,'db_type',?9,'db_column',?10)",
    -1, &st, 0);
  if(!st) return 0;
  sqlite3_bind_text(st, 1, kind, -1, SQLITE_TRANSIENT);
  if(column) sqlite3_bind_text(st, 2, column, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 2);
  sqlite3_bind_text(st, 3, name, -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 4, is_array ? 1 : 0);
  if(li_prop)  sqlite3_bind_text(st, 5, li_prop, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 5);
  if(dict_key) sqlite3_bind_text(st, 6, dict_key, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 6);
  if(has_parent) sqlite3_bind_int64(st, 7, parent_sid); else sqlite3_bind_null(st, 7);
  if(has_sid)    sqlite3_bind_int64(st, 8, sid);        else sqlite3_bind_null(st, 8);
  if(db_type) sqlite3_bind_text(st, 9, db_type, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 9);
  if(db_col)  sqlite3_bind_text(st, 10, db_col, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 10);
  char *r = 0;
  if(sqlite3_step(st) == SQLITE_ROW) r = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st, 0));
  sqlite3_finalize(st);
  return r;
}

/* Look up a structure in _scheme_metadata_cache. Returns 1 if found and sets
** outputs (*db_type is sqlite3_malloc'd; caller frees). */
static int pvtLookupStruct(sqlite3 *db, sqlite3_int64 scheme, const char *name,
                           int has_parent, sqlite3_int64 parent_sid,
                           sqlite3_int64 *out_sid, char **out_db_type, int *out_is_array){
  const char *cond = has_parent ? "_parent_structure_id=?3" : "_parent_structure_id IS NULL";
  char *sql = sqlite3_mprintf(
    "SELECT _structure_id, db_type, (_collection_type IS NOT NULL) "
    "FROM _scheme_metadata_cache WHERE _scheme_id=?1 AND _name=?2 AND %s LIMIT 1", cond);
  if(!sql) return 0;
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db, sql, -1, &st, 0);
  sqlite3_free(sql);
  if(!st) return 0;
  sqlite3_bind_int64(st, 1, scheme);
  sqlite3_bind_text(st, 2, name, -1, SQLITE_TRANSIENT);
  if(has_parent) sqlite3_bind_int64(st, 3, parent_sid);
  int found = 0;
  if(sqlite3_step(st) == SQLITE_ROW){
    found = 1;
    if(out_sid) *out_sid = sqlite3_column_int64(st, 0);
    if(out_db_type){ const char *t = (const char*)sqlite3_column_text(st, 1); *out_db_type = sqlite3_mprintf("%s", t ? t : ""); }
    if(out_is_array) *out_is_array = sqlite3_column_int(st, 2);
  }
  sqlite3_finalize(st);
  return found;
}

/* pvt_resolve_field_path: logical path -> FieldInfo meta JSON (sqlite3_malloc'd),
** or NULL if it cannot be matched. Mirrors SchemeFieldResolver. */
static char *pvtResolveFieldPath(sqlite3 *db, sqlite3_int64 scheme, const char *path){
  if(!path || !*path) return 0;

  /* 0. Base field. */
  char *base_col = pvtNormalizeBaseFieldName(path);
  if(base_col){
    char *m = pvtEmitMeta(db, "base", base_col, path, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    sqlite3_free(base_col);
    return m;
  }

  /* 1. Dictionary path: root[key] or root[key].child  (key must be non-empty). */
  const char *lb = strchr(path, '[');
  if(lb && lb > path && lb[1] != ']'){
    const char *rb = strchr(lb, ']');
    if(rb && rb > lb + 1){
      char *root = sqlite3_mprintf("%.*s", (int)(lb - path), path);
      char *key  = sqlite3_mprintf("%.*s", (int)(rb - lb - 1), lb + 1);
      const char *rest = rb + 1;
      sqlite3_int64 sid = 0; char *dbt = 0; int isarr = 0;
      char *res = 0;
      if(pvtLookupStruct(db, scheme, root, 0, 0, &sid, &dbt, &isarr)){
        if(rest[0] == '.'){
          sqlite3_int64 psid = sid; char *cdbt = 0; int cisarr = 0; sqlite3_int64 csid = 0;
          if(pvtLookupStruct(db, scheme, rest + 1, 1, psid, &csid, &cdbt, &cisarr)){
            const char *col = pvtDbTypeToValueColumn(cdbt ? cdbt : "");
            res = pvtEmitMeta(db, "field", 0, path, cisarr, 0, key, 1, psid, 1, csid, cdbt, col);
          }
          sqlite3_free(cdbt);
        }else{
          const char *col = pvtDbTypeToValueColumn(dbt ? dbt : "");
          res = pvtEmitMeta(db, "field", 0, path, 0, 0, key, 0, 0, 1, sid, dbt, col);
        }
      }
      sqlite3_free(dbt); sqlite3_free(root); sqlite3_free(key);
      return res;   /* may be NULL on miss */
    }
  }

  /* 2. Dotted paths. */
  const char *dot = strchr(path, '.');
  if(dot){
    /* split into parts */
    int nparts = 1;
    for(const char *p = path; *p; p++) if(*p == '.') nparts++;
    char **parts = (char**)sqlite3_malloc(sizeof(char*) * nparts);
    { int idx = 0; const char *seg = path;
      for(const char *p = path; ; p++){
        if(*p == '.' || *p == 0){ parts[idx++] = sqlite3_mprintf("%.*s", (int)(p - seg), seg); seg = p + 1; if(*p == 0) break; }
      }
    }
    const char *last = parts[nparts - 1];
    char *res = 0;
    int is_li_acc = (nparts == 2) && (!strcmp(last,"Id") || !strcmp(last,"Value") || !strcmp(last,"Alias"));

    /* 2a. Roles[].Value / .Alias / .Id (ListItem array accessor). */
    if(!res && is_li_acc){
      size_t l0 = strlen(parts[0]);
      if(l0 >= 2 && parts[0][l0-2]=='[' && parts[0][l0-1]==']'){
        char *root_li = sqlite3_mprintf("%.*s", (int)(l0 - 2), parts[0]);
        sqlite3_int64 sid = 0; char *dbt = 0; int isarr = 0;
        if(pvtLookupStruct(db, scheme, root_li, 0, 0, &sid, &dbt, &isarr)
           && dbt && !strcmp(dbt, "ListItem") && isarr){
          res = pvtEmitMeta(db, "field", 0, path, 1, last, 0, 0, 0, 1, sid, "ListItem", "_ListItem");
        }
        sqlite3_free(dbt); sqlite3_free(root_li);
      }
    }
    /* 2b. Status.Value / .Alias / .Id (ListItem scalar accessor). */
    if(!res && is_li_acc){
      sqlite3_int64 sid = 0; char *dbt = 0; int isarr = 0;
      if(pvtLookupStruct(db, scheme, parts[0], 0, 0, &sid, &dbt, &isarr)
         && dbt && !strcmp(dbt, "ListItem")){
        res = pvtEmitMeta(db, "field", 0, path, isarr, last, 0, 0, 0, 1, sid, "ListItem", "_ListItem");
      }
      sqlite3_free(dbt);
    }
    /* 2c. Generic nested: walk the parent chain. */
    if(!res){
      sqlite3_int64 cur = 0; char *dbt = 0; int isarr = 0; int ok = 1;
      if(!pvtLookupStruct(db, scheme, parts[0], 0, 0, &cur, 0, 0)) ok = 0;
      for(int i = 1; ok && i < nparts; i++){
        sqlite3_int64 nx = 0;
        char *d = 0; int a = 0;
        if(!pvtLookupStruct(db, scheme, parts[i], 1, cur, &nx, &d, &a)){ ok = 0; sqlite3_free(d); break; }
        cur = nx; sqlite3_free(dbt); dbt = d; isarr = a;
      }
      if(ok){
        const char *col = pvtDbTypeToValueColumn(dbt ? dbt : "");
        res = pvtEmitMeta(db, "field", 0, path, isarr, 0, 0, 0, 0, 1, cur, dbt, col);
      }
      sqlite3_free(dbt);
    }
    for(int i = 0; i < nparts; i++) sqlite3_free(parts[i]);
    sqlite3_free(parts);
    return res;
  }

  /* 3. Bare root field (maybe Foo[]). */
  {
    char *base = sqlite3_mprintf("%s", path);
    int force_array = 0;
    size_t bl = strlen(base);
    if(bl >= 2 && base[bl-2]=='[' && base[bl-1]==']'){ base[bl-2] = 0; force_array = 1; }
    sqlite3_int64 sid = 0; char *dbt = 0; int isarr = 0;
    char *res = 0;
    if(pvtLookupStruct(db, scheme, base, 0, 0, &sid, &dbt, &isarr)){
      if(force_array) isarr = 1;
      if(dbt && !strcmp(dbt, "ListItem"))
        res = pvtEmitMeta(db, "field", 0, path, isarr, "Id", 0, 0, 0, 1, sid, "ListItem", "_ListItem");
      else{
        const char *col = pvtDbTypeToValueColumn(dbt ? dbt : "");
        res = pvtEmitMeta(db, "field", 0, path, isarr, 0, 0, 0, 0, 1, sid, dbt, col);
      }
    }
    sqlite3_free(dbt); sqlite3_free(base);
    return res;
  }
}

/* ------------------------------------------------------------------------- */
/* 10_pvt_field_collection.sql — collect_fields + null/absence checks        */
/* ------------------------------------------------------------------------- */

typedef struct {
  sqlite3 *db;
  sqlite3_int64 scheme;
  char **seen; int nseen, cap;
  sqlite3_str *body;
  int emitted;
  int err;
} PvtCollect;

static int pvtSeen(PvtCollect *c, const char *k){
  for(int i = 0; i < c->nseen; i++) if(!strcmp(c->seen[i], k)) return 1;
  return 0;
}
static void pvtSeenAdd(PvtCollect *c, const char *k){
  if(c->nseen == c->cap){ c->cap = c->cap ? c->cap * 2 : 8;
    c->seen = (char**)sqlite3_realloc(c->seen, sizeof(char*) * c->cap); }
  c->seen[c->nseen++] = sqlite3_mprintf("%s", k);
}

static char *pvtJsonQuote(sqlite3 *db, const char *s){
  sqlite3_stmt *st = 0; char *r = 0;
  sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, 0);
  if(st){ sqlite3_bind_text(st, 1, s, -1, SQLITE_TRANSIENT);
    if(sqlite3_step(st) == SQLITE_ROW) r = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); }
  return r;
}

/* merge `patch_json` into `base` (json_patch); frees base, returns new. */
static char *pvtPatch(sqlite3 *db, char *base, const char *patch_json){
  sqlite3_stmt *st = 0; char *r = 0;
  sqlite3_prepare_v2(db, "SELECT json_patch(?1, json(?2))", -1, &st, 0);
  if(st){ sqlite3_bind_text(st, 1, base, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, patch_json, -1, SQLITE_TRANSIENT);
    if(sqlite3_step(st) == SQLITE_ROW) r = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); }
  sqlite3_free(base);
  return r;
}

static void pvtEmitEntry(PvtCollect *c, const char *key, char *meta /*consumed*/){
  if(!meta){ c->err = 1; return; }
  if(c->emitted) sqlite3_str_append(c->body, ",", 1);
  char *qk = pvtJsonQuote(c->db, key);
  sqlite3_str_appendf(c->body, "%s:%s", qk ? qk : "\"?\"", meta);
  sqlite3_free(qk); sqlite3_free(meta);
  c->emitted++;
  pvtSeenAdd(c, key);
}

static void pvtAddField(PvtCollect *c, const char *path, const char *op_value){
  if(c->err || !path) return;
  size_t pl = strlen(path);

  /* .$length / .$count -> register base + modifier entry. */
  const char *suf = 0, *mod = 0;
  if(pl > 8 && !strcmp(path + pl - 8, ".$length")){ suf = ".$length"; mod = "length"; }
  else if(pl > 7 && !strcmp(path + pl - 7, ".$count")){ suf = ".$count"; mod = "count"; }
  if(suf){
    char *base = sqlite3_mprintf("%.*s", (int)(pl - strlen(suf)), path);
    if(!pvtSeen(c, base)){
      char *bm = pvtResolveFieldPath(c->db, c->scheme, base);
      if(!bm){ c->err = 1; sqlite3_free(base); return; }
      pvtEmitEntry(c, base, bm);
    }
    if(!pvtSeen(c, path)){
      char *bm = pvtResolveFieldPath(c->db, c->scheme, base);
      if(!bm){ c->err = 1; sqlite3_free(base); return; }
      char *patch = sqlite3_mprintf("{\"length_modifier\":1,\"modifier_kind\":%Q,\"base_name\":%Q}", mod, base);
      bm = pvtPatch(c->db, bm, patch);
      sqlite3_free(patch);
      pvtEmitEntry(c, path, bm);
    }
    sqlite3_free(base);
    return;
  }

  char *norm = pvtNormalizeFieldName(path, op_value);
  if(pvtSeen(c, norm)){ sqlite3_free(norm); return; }
  char *meta = pvtResolveFieldPath(c->db, c->scheme, norm);
  if(!meta){ c->err = 1; sqlite3_free(norm); return; }
  if(strcmp(norm, path) != 0)
    meta = pvtPatch(c->db, meta, "{\"was_contains_key\":1}");
  pvtEmitEntry(c, norm, meta);
  sqlite3_free(norm);
}

static void pvtWalkFilter(PvtCollect *c, const char *node){
  if(c->err || !node) return;
  char *t = jsonTypeAt(c->db, node, "$");
  if(!t || strcmp(t, "object") != 0){ sqlite3_free(t); return; }
  sqlite3_free(t);

  /* $field expression shortcut; $const carries no field. */
  char *ft = jsonTypeAt(c->db, node, "$.\"$field\"");
  if(ft){ sqlite3_free(ft); char *f = jsonGetText(c->db, node, "$.\"$field\""); if(f) pvtAddField(c, f, 0); sqlite3_free(f); return; }
  char *ct = jsonTypeAt(c->db, node, "$.\"$const\"");
  if(ct){ sqlite3_free(ct); return; }

  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(c->db, "SELECT key, type, value FROM json_each(?1)", -1, &it, 0);
  if(!it) return;
  sqlite3_bind_text(it, 1, node, -1, SQLITE_TRANSIENT);
  while(!c->err && sqlite3_step(it) == SQLITE_ROW){
    const char *k = (const char*)sqlite3_column_text(it, 0);
    const char *ty = (const char*)sqlite3_column_text(it, 1);
    char *v = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 2));
    if(k && k[0] == '$'){
      char lk[16]; pvtLc(lk, k, sizeof lk);
      if(!strcmp(lk,"$case") && ty && !strcmp(ty,"array")){
        /* $case branches: descend into when/then/else VALUES, not the keys. */
        sqlite3_stmt *ai = 0; sqlite3_prepare_v2(c->db,"SELECT value FROM json_each(?1)",-1,&ai,0);
        sqlite3_bind_text(ai,1,v,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(ai)==SQLITE_ROW){ char *e=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(ai,0));
          for(int b=0;b<3;b++){ const char *bp=b==0?"$.when":b==1?"$.then":"$.else"; char *bv=jsonGetText(c->db,e,bp); if(bv) pvtWalkFilter(c,bv); sqlite3_free(bv); }
          sqlite3_free(e); }
        sqlite3_finalize(ai);
      }
      else if(ty && !strcmp(ty, "array")){
        sqlite3_stmt *ai = 0;
        sqlite3_prepare_v2(c->db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
        sqlite3_bind_text(ai, 1, v, -1, SQLITE_TRANSIENT);
        while(sqlite3_step(ai) == SQLITE_ROW){ char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0)); pvtWalkFilter(c, e); sqlite3_free(e); }
        sqlite3_finalize(ai);
      }else if(ty && !strcmp(ty, "object")){
        pvtWalkFilter(c, v);
      }
    }else{
      char *opv = 0;
      size_t kl = k ? strlen(k) : 0;
      if(kl >= 12 && !strcmp(k + kl - 12, ".ContainsKey")) opv = pvtPeekContainsKeyValue(c->db, v, ty);
      pvtAddField(c, k, opv);
      sqlite3_free(opv);
    }
    sqlite3_free(v);
  }
  sqlite3_finalize(it);
}

/* pvt_collect_fields -> fields meta JSON (sqlite3_malloc'd), or NULL on error. */
static char *pvtCollectFields(sqlite3 *db, sqlite3_int64 scheme,
                              const char *filter, const char *order, int include_all){
  PvtCollect c; memset(&c, 0, sizeof(c));
  c.db = db; c.scheme = scheme; c.body = sqlite3_str_new(db);

  if(include_all){
    sqlite3_stmt *st = 0;
    sqlite3_prepare_v2(db,
      "SELECT DISTINCT _name FROM _scheme_metadata_cache WHERE _scheme_id=?1 AND _parent_structure_id IS NULL",
      -1, &st, 0);
    if(st){ sqlite3_bind_int64(st, 1, scheme);
      while(!c.err && sqlite3_step(st) == SQLITE_ROW){
        const char *nm = (const char*)sqlite3_column_text(st, 0);
        if(nm && !pvtSeen(&c, nm)) pvtEmitEntry(&c, nm, pvtResolveFieldPath(db, scheme, nm));
      }
      sqlite3_finalize(st);
    }
  }else{
    if(filter) pvtWalkFilter(&c, filter);
    if(!c.err && order){
      char *ot = jsonTypeAt(db, order, "$");
      if(ot && !strcmp(ot, "array")){
        sqlite3_stmt *oi = 0;
        sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &oi, 0);
        sqlite3_bind_text(oi, 1, order, -1, SQLITE_TRANSIENT);
        while(!c.err && sqlite3_step(oi) == SQLITE_ROW){
          char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(oi, 0));
          char *f = jsonGetText(db, e, "$.field");
          if(!f) f = jsonGetText(db, e, "$.field_path");
          if(f && !pvtSeen(&c, f)) pvtEmitEntry(&c, f, pvtResolveFieldPath(db, scheme, f));
          else if(!f){ char *ex = jsonGetText(db, e, "$.\"$expr\""); if(ex) pvtWalkFilter(&c, ex); sqlite3_free(ex); }
          sqlite3_free(f); sqlite3_free(e);
        }
        sqlite3_finalize(oi);
      }
      sqlite3_free(ot);
    }
  }

  char *res = 0;
  char *body = sqlite3_str_finish(c.body);
  if(!c.err) res = sqlite3_mprintf("{%s}", body ? body : "");
  sqlite3_free(body);
  for(int i = 0; i < c.nseen; i++) sqlite3_free(c.seen[i]);
  sqlite3_free(c.seen);
  return res;
}

/* null/absence check op recognizer (case-insensitive). */
static int pvtIsNullOp(const char *k, int absence){
  if(!k) return 0;
  char b[32]; int i = 0;
  for(; k[i] && i < 31; i++){ char ch = k[i]; b[i] = (ch >= 'A' && ch <= 'Z') ? ch + 32 : ch; }
  b[i] = 0;
  if(!strcmp(b,"$null") || !strcmp(b,"$isnull") || !strcmp(b,"$exists")) return 1;
  if(!absence && !strcmp(b,"$notnull")) return 1;
  return 0;
}

int pvtHasCheck(sqlite3 *db, const char *filter, int absence){
  if(!filter) return 0;
  char *t = jsonTypeAt(db, filter, "$");
  int isobj = t && !strcmp(t, "object");
  sqlite3_free(t);
  if(!isobj) return 0;

  int hit = 0;
  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT key, type, value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it, 1, filter, -1, SQLITE_TRANSIENT);
  while(!hit && sqlite3_step(it) == SQLITE_ROW){
    const char *k = (const char*)sqlite3_column_text(it, 0);
    const char *ty = (const char*)sqlite3_column_text(it, 1);
    char *v = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 2));
    if(k && k[0] == '$'){
      if(pvtIsNullOp(k, absence)) hit = 1;
      else if(ty && !strcmp(ty, "array")){
        sqlite3_stmt *ai = 0;
        sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
        sqlite3_bind_text(ai, 1, v, -1, SQLITE_TRANSIENT);
        while(!hit && sqlite3_step(ai) == SQLITE_ROW){ char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0)); if(pvtHasCheck(db, e, absence)) hit = 1; sqlite3_free(e); }
        sqlite3_finalize(ai);
      }else if(ty && !strcmp(ty, "object")){
        if(pvtHasCheck(db, v, absence)) hit = 1;
      }
    }else if(ty && !strcmp(ty, "object")){
      sqlite3_stmt *si = 0;
      sqlite3_prepare_v2(db, "SELECT key, type FROM json_each(?1)", -1, &si, 0);
      sqlite3_bind_text(si, 1, v, -1, SQLITE_TRANSIENT);
      while(!hit && sqlite3_step(si) == SQLITE_ROW){
        const char *sk = (const char*)sqlite3_column_text(si, 0);
        const char *sty = (const char*)sqlite3_column_text(si, 1);
        if(pvtIsNullOp(sk, absence)) hit = 1;
        else if(sty && !strcmp(sty, "null") && sk &&
                (!strcmp(sk,"$eq") || (!absence && !strcmp(sk,"$ne")))) hit = 1;
      }
      sqlite3_finalize(si);
    }else if(ty && !strcmp(ty, "null")){
      hit = 1;
    }
    sqlite3_free(v);
  }
  sqlite3_finalize(it);
  return hit;
}

/* ------------------------------------------------------------------------- */
/* 13_pvt_condition.sql                                                      */
/* ------------------------------------------------------------------------- */

/* Does the resolved column store TEXT (literal must be single-quoted) vs a
** numeric affinity (literal emitted bare)? Numbers/bools bare is REQUIRED in
** SQLite — a REAL/INTEGER column compared to a quoted TEXT literal is always
** false (type-ordering: numbers < text). */
static int pvtColIsText(const char *kind, const char *column,
                        const char *db_type, const char *li_prop){
  if(li_prop && (!strcmp(li_prop, "Value") || !strcmp(li_prop, "Alias"))) return 1;
  if(li_prop && !strcmp(li_prop, "Id")) return 0;
  if(kind && !strcmp(kind, "base")){
    if(column && (!strcmp(column,"_name") || !strcmp(column,"_note") ||
                  !strcmp(column,"_value_string") || !strcmp(column,"_value_guid") ||
                  !strcmp(column,"_value_datetime") || !strcmp(column,"_date_create") ||
                  !strcmp(column,"_date_modify") || !strcmp(column,"_date_begin") ||
                  !strcmp(column,"_date_complete") || !strcmp(column,"_hash") ||
                  !strcmp(column,"_value_bytes"))) return 1;
    return 0;
  }
  if(db_type && (!strcmp(db_type,"String") || !strcmp(db_type,"Guid") ||
                 !strcmp(db_type,"DateTimeOffset") || !strcmp(db_type,"ByteArray") ||
                 !strcmp(db_type,"TimeSpan") || !strcmp(db_type,"DateTime"))) return 1;
  return 0;
}

/* Datetime columns store REAL Julian day (UTC). True for base date columns
** (_date_create/_modify/_begin/_complete, _value_datetime) and for prop
** db_type DateTime/DateTimeOffset. */
static int pvtColIsDateTime(const char *kind, const char *column, const char *db_type){
  if(kind && !strcmp(kind, "base"))
    return column && (!strcmp(column,"_value_datetime") || !strcmp(column,"_date_create") ||
                      !strcmp(column,"_date_modify") || !strcmp(column,"_date_begin") ||
                      !strcmp(column,"_date_complete"));
  return db_type && (!strcmp(db_type,"DateTimeOffset") || !strcmp(db_type,"DateTime"));
}

/* Append a JSON operand as a SQL literal, typed per the LHS column. */
static void pvtAppendOperand(sqlite3_str *o, const char *txt, const char *jtype, int is_text, int is_datetime){
  if(jtype){
    if(!strcmp(jtype, "true"))  { sqlite3_str_append(o, "1", 1); return; }
    if(!strcmp(jtype, "false")) { sqlite3_str_append(o, "0", 1); return; }
    if(!strcmp(jtype, "integer") || !strcmp(jtype, "real")) {
      sqlite3_str_appendf(o, "%s", txt ? txt : "0"); return;
    }
  }
  /* datetime operand: ISO string -> UTC Julian (julianday parses any offset).
  ** Wrapped on the VALUE (constant), not the column, so it stays index-sargable. */
  if(is_datetime){ sqlite3_str_appendf(o, "julianday(%Q)", txt ? txt : ""); return; }
  if(is_text) sqlite3_str_appendf(o, "%Q", txt ? txt : "");
  else        sqlite3_str_appendf(o, "%s", txt ? txt : "NULL");
}

/* LIKE pattern literal with prefix/suffix wildcards added in C, then quoted. */
static void pvtAppendLikePattern(sqlite3_str *o, const char *val, int lead, int trail){
  char *pat = sqlite3_mprintf("%s%s%s", lead ? "%" : "", val ? val : "", trail ? "%" : "");
  sqlite3_str_appendf(o, "%Q", pat);
  sqlite3_free(pat);
}

/* Build the AND-joined predicate fragment for a single leaf field.
** Returns sqlite3_malloc'd text, or NULL on unsupported op / bad meta. */
static char *pvtBuildFieldCondition(sqlite3 *db, const char *field_name, const char *meta,
                                    const char *op_json, const char *op_type,
                                    const char *base_prefix){
  if(!meta) return 0;
  if(!base_prefix) base_prefix = "";

  char *kind    = jsonGetText(db, meta, "$.kind");
  char *li_prop = jsonGetText(db, meta, "$.list_item_prop");
  char *db_type = jsonGetText(db, meta, "$.db_type");
  char *column  = jsonGetText(db, meta, "$.column");
  char *base_nm = jsonGetText(db, meta, "$.base_name");
  int is_array  = jsonGetBool(db, meta, "$.is_array");
  int length_mod= jsonGetBool(db, meta, "$.length_modifier");
  int is_text   = pvtColIsText(kind, column, db_type, li_prop);
  int is_datetime = pvtColIsDateTime(kind, column, db_type);
  int is_base   = kind && !strcmp(kind, "base");

  /* LHS column expression. */
  sqlite3_str *colb = sqlite3_str_new(db);
  if(length_mod){
    const char *tgt_open = is_array ? "COALESCE(json_array_length(" : "COALESCE(length(";
    sqlite3_str_appendf(colb, "%s", tgt_open);
    if(is_base) sqlite3_str_appendf(colb, "%s%s", base_prefix, column ? column : "_id");
    else        sqlite3_str_appendf(colb, "\"%w\"", base_nm ? base_nm : field_name);
    sqlite3_str_append(colb, "), 0)", 5);
  }else if(is_base){
    sqlite3_str_appendf(colb, "%s%s", base_prefix, column ? column : "_id");
  }else{
    sqlite3_str_appendf(colb, "\"%w\"", field_name);
  }
  char *vcol = sqlite3_str_finish(colb);   /* sqlite3_free later */

  sqlite3_str *out = sqlite3_str_new(db);
  int ok = 1, nparts = 0;

  /* Shorthand: non-object operand == {"$eq": operand}. */
  if(!op_type || strcmp(op_type, "object") != 0){
    if(op_type && !strcmp(op_type, "null")){
      sqlite3_str_appendf(out, "%s IS NULL", vcol);
    }else{
      sqlite3_str_appendf(out, "%s = ", vcol);
      pvtAppendOperand(out, op_json, op_type, is_text, is_datetime);
    }
    goto finish;
  }

  /* Iterate the operator object: AND of each present op. */
  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT key, type, value FROM json_each(?1)", -1, &it, 0);
  if(!it){ ok = 0; goto finish; }
  sqlite3_bind_text(it, 1, op_json, -1, SQLITE_TRANSIENT);

  while(ok && sqlite3_step(it) == SQLITE_ROW){
    char *opk  = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 0));
    char *otyp = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 1));
    /* operand value as text; for object/array json_each.value is its JSON text */
    const char *oval_raw = (const char*)sqlite3_column_text(it, 2);
    char *oval = sqlite3_mprintf("%s", oval_raw ? oval_raw : "");
    /* lower(opk) */
    for(char *p = opk; *p; p++) if(*p >= 'A' && *p <= 'Z') *p += 32;

    if(nparts) sqlite3_str_append(out, " AND ", 5);
    nparts++;

    int eq = !strcmp(opk,"$eq"), ne = !strcmp(opk,"$ne");
    if(eq || ne || !strcmp(opk,"$gt") || !strcmp(opk,"$gte") || !strcmp(opk,"$lt") || !strcmp(opk,"$lte")){
      const char *sym = eq?"=":ne?"<>":!strcmp(opk,"$gt")?">":!strcmp(opk,"$gte")?">=":!strcmp(opk,"$lt")?"<":"<=";
      if(!strcmp(otyp,"null")){
        sqlite3_str_appendf(out, "%s IS %sNULL", vcol, ne ? "NOT " : "");
      }else if(is_array && !length_mod && (eq || ne)){
        /* array LHS: scalar = ANY -> EXISTS over json_each */
        if(eq){
          sqlite3_str_appendf(out, "EXISTS (SELECT 1 FROM json_each(%s) WHERE value = ", vcol);
          pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
          sqlite3_str_append(out, ")", 1);
        }else{
          sqlite3_str_appendf(out, "(%s IS NULL OR NOT EXISTS (SELECT 1 FROM json_each(%s) WHERE value = ", vcol, vcol);
          pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
          sqlite3_str_append(out, "))", 2);
        }
      }else{
        sqlite3_str_appendf(out, "%s %s ", vcol, sym);
        pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
      }
    }
    else if(!strcmp(opk,"$in") || !strcmp(opk,"$nin")){
      int nin = !strcmp(opk,"$nin");
      if(strcmp(otyp,"array") != 0){ ok = 0; }
      else if(is_array && !length_mod){
        /* array LHS overlap via json_each intersection */
        if(nin)
          sqlite3_str_appendf(out,
            "(%s IS NULL OR NOT EXISTS (SELECT 1 FROM json_each(%s) WHERE value IN (SELECT value FROM json_each(%Q))))",
            vcol, vcol, oval);
        else
          sqlite3_str_appendf(out,
            "EXISTS (SELECT 1 FROM json_each(%s) WHERE value IN (SELECT value FROM json_each(%Q)))",
            vcol, oval);
      }else{
        sqlite3_str_appendf(out, "%s %s (SELECT %svalue%s FROM json_each(%Q))",
          vcol, nin ? "NOT IN" : "IN",
          is_datetime ? "julianday(" : "", is_datetime ? ")" : "", oval);
      }
    }
    else if(!strcmp(opk,"$like") || !strcmp(opk,"$ilike")){
      /* SQLite LIKE is ASCII case-insensitive; both map to LIKE. */
      sqlite3_str_appendf(out, "%s LIKE %Q", vcol, oval);
    }
    else if(!strcmp(opk,"$startswith") || !strcmp(opk,"$startswithignorecase")){
      sqlite3_str_appendf(out, "%s LIKE ", vcol); pvtAppendLikePattern(out, oval, 0, 1);
    }
    else if(!strcmp(opk,"$endswith") || !strcmp(opk,"$endswithignorecase")){
      sqlite3_str_appendf(out, "%s LIKE ", vcol); pvtAppendLikePattern(out, oval, 1, 0);
    }
    else if(!strcmp(opk,"$contains") || !strcmp(opk,"$containsignorecase")){
      sqlite3_str_appendf(out, "%s LIKE ", vcol); pvtAppendLikePattern(out, oval, 1, 1);
    }
    else if(!strcmp(opk,"$null") || !strcmp(opk,"$isnull")){
      int truthy = (!strcmp(otyp,"true")) || (!strcmp(oval,"true")) || (!strcmp(oval,"1"));
      sqlite3_str_appendf(out, "%s IS %sNULL", vcol, truthy ? "" : "NOT ");
    }
    else if(!strcmp(opk,"$notnull") || !strcmp(opk,"$exists")){
      int truthy = (!strcmp(otyp,"true")) || (!strcmp(oval,"true")) || (!strcmp(oval,"1"));
      sqlite3_str_appendf(out, "%s IS %sNULL", vcol, truthy ? "NOT " : "");
    }
    else if(!strcmp(opk,"$arraycontains")){
      sqlite3_str_appendf(out, "EXISTS (SELECT 1 FROM json_each(%s) WHERE value = ", vcol);
      pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
      sqlite3_str_append(out, ")", 1);
    }
    else if(!strcmp(opk,"$arrayany") || !strcmp(opk,"$arrayempty")){
      int truthy = (!strcmp(otyp,"true")) || (!strcmp(oval,"true")) || (!strcmp(oval,"1"));
      int want_nonempty = (!strcmp(opk,"$arrayany")) ? truthy : !truthy;
      sqlite3_str_appendf(out, "COALESCE(json_array_length(%s),0) %s 0", vcol, want_nonempty ? ">" : "=");
    }
    else if(!strcmp(opk,"$arraycount") || !strcmp(opk,"$arraycountgt") || !strcmp(opk,"$arraycountgte") ||
            !strcmp(opk,"$arraycountlt") || !strcmp(opk,"$arraycountlte")){
      const char *sym = !strcmp(opk,"$arraycount")?"=":!strcmp(opk,"$arraycountgt")?">":
                        !strcmp(opk,"$arraycountgte")?">=":!strcmp(opk,"$arraycountlt")?"<":"<=";
      sqlite3_str_appendf(out, "COALESCE(json_array_length(%s),0) %s %s", vcol, sym, oval);
    }
    else if(!strcmp(opk,"$arrayfirst")){
      sqlite3_str_appendf(out, "json_extract(%s, '$[0]') = ", vcol);
      pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
    }
    else if(!strcmp(opk,"$arraylast")){
      sqlite3_str_appendf(out, "json_extract(%s, '$[#-1]') = ", vcol);
      pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
    }
    else if(!strcmp(opk,"$arrayat")){
      /* operand {"index":N,"value":V} */
      int f = 0; sqlite3_int64 idx = jsonGetInt(db, oval, "$.index", &f);
      char *vt = jsonTypeAt(db, oval, "$.value"); char *vv = jsonGetText(db, oval, "$.value");
      sqlite3_str_appendf(out, "json_extract(%s, '$[%lld]') = ", vcol, (long long)idx);
      pvtAppendOperand(out, vv, vt, is_text, is_datetime);
      sqlite3_free(vt); sqlite3_free(vv);
    }
    else if(!strcmp(opk,"$arraystartswith")){
      sqlite3_str_appendf(out, "json_extract(%s, '$[0]') LIKE ", vcol);
      pvtAppendLikePattern(out, oval, 0, 1);
    }
    else if(!strcmp(opk,"$arrayendswith")){
      sqlite3_str_appendf(out, "json_extract(%s, '$[#-1]') LIKE ", vcol);
      pvtAppendLikePattern(out, oval, 1, 0);
    }
    else if(!strcmp(opk,"$arraymatches")){
      sqlite3_str_appendf(out, "EXISTS (SELECT 1 FROM json_each(%s) WHERE value LIKE %Q)", vcol, oval ? oval : "");
    }
    else if(!strcmp(opk,"$arraysum")||!strcmp(opk,"$arraymin")||!strcmp(opk,"$arraymax")||!strcmp(opk,"$arrayavg")){
      const char *fn = !strcmp(opk,"$arraysum")?"SUM":!strcmp(opk,"$arraymin")?"MIN":!strcmp(opk,"$arraymax")?"MAX":"AVG";
      sqlite3_str_appendf(out, "(SELECT %s(value) FROM json_each(%s)) = ", fn, vcol);
      pvtAppendOperand(out, oval, otyp, is_text, is_datetime);
    }
    else{
      /* $regex / $iregex / $fts / $expr-form — not yet ported. */
      ok = 0;
    }

    sqlite3_free(opk); sqlite3_free(otyp); sqlite3_free(oval);
  }
  sqlite3_finalize(it);
  if(nparts == 0) ok = 0;

finish:;
  char *res = 0;
  if(ok){
    /* wrap multi-op in parens for AND precedence safety */
    if(nparts > 1){
      char *body = sqlite3_str_finish(out);
      res = sqlite3_mprintf("(%s)", body);
      sqlite3_free(body);
    }else{
      res = sqlite3_str_finish(out);
    }
  }else{
    sqlite3_free(sqlite3_str_finish(out));
  }
  sqlite3_free(vcol);
  sqlite3_free(kind); sqlite3_free(li_prop); sqlite3_free(db_type);
  sqlite3_free(column); sqlite3_free(base_nm);
  return res;
}

/* ------------------------------------------------------------------------- */
/* 14_pvt_where.sql — recursive filter walker                                */
/* ------------------------------------------------------------------------- */

char *pvtBuildWhereFromJson(sqlite3 *db, const char *filter, const char *fields,
                                   const char *base_prefix);

/* Join an accumulating result with a child fragment using sep. Frees child. */
static char *pvtJoin(char *acc, char *child, const char *sep){
  if(!child) return acc;            /* propagate: caller checks NULL separately */
  if(!acc) return child;
  char *r = sqlite3_mprintf("%s%s%s", acc, sep, child);
  sqlite3_free(acc); sqlite3_free(child);
  return r;
}

/* $level: absolute hierarchy level = count of ancestors in the parent chain up to
   the root. Level 0 = root (no parent), level 1 = direct child of root, etc.
   Mirrors PG pvt_build_level_condition (06a_pvt_legacy_helpers.sql): a correlated
   recursive subquery anchored on the outer _objects alias `o`. Accepts a bare
   integer ("$level": 2 -> "= 2") or an operator object ("$level": {"$gt": 2}). */
static char *pvtLevelExpr(sqlite3 *db, const char *base_prefix){
  /* Anchor references the OUTER row's parent. In the narrow path the outer alias is
     `o.`; in the wide/force_outer path the columns are exposed bare by _pvt_cte. */
  if(!base_prefix) base_prefix = "";
  return sqlite3_mprintf(
    "(SELECT COUNT(*) FROM ("
    "WITH RECURSIVE _lvl_anc(parent_id) AS ("
    "SELECT %s_id_parent "
    "UNION ALL "
    "SELECT p._id_parent FROM _objects p JOIN _lvl_anc ON p._id = _lvl_anc.parent_id "
    "WHERE p._id_parent IS NOT NULL) "
    "SELECT parent_id FROM _lvl_anc WHERE parent_id IS NOT NULL))", base_prefix);
}

static char *pvtBuildLevelCondition(sqlite3 *db, const char *v, const char *kt,
                                    const char *base_prefix){
  char *lvl = pvtLevelExpr(db, base_prefix);
  char *res = 0;
  if(kt && !strcmp(kt, "object")){
    sqlite3_stmt *it = 0;
    sqlite3_prepare_v2(db, "SELECT key, value FROM json_each(?1)", -1, &it, 0);
    if(it){
      sqlite3_bind_text(it, 1, v, -1, SQLITE_TRANSIENT);
      char *acc = 0;
      while(sqlite3_step(it) == SQLITE_ROW){
        const char *opk = (const char*)sqlite3_column_text(it, 0);
        const char *opv = (const char*)sqlite3_column_text(it, 1);
        char lk[12]; pvtLc(lk, opk ? opk : "", sizeof lk);
        const char *sym =
          !strcmp(lk,"$gt")  ? ">"  : !strcmp(lk,"$gte") ? ">=" :
          !strcmp(lk,"$lt")  ? "<"  : !strcmp(lk,"$lte") ? "<=" :
          !strcmp(lk,"$eq")  ? "="  : !strcmp(lk,"$ne")  ? "!=" : 0;
        if(!sym) continue;
        char *frag = sqlite3_mprintf("%s %s %s", lvl, sym, (opv && *opv) ? opv : "0");
        acc = pvtJoin(acc, frag, " AND ");
      }
      sqlite3_finalize(it);
      if(acc){ res = sqlite3_mprintf("(%s)", acc); sqlite3_free(acc); }
    }
  }else{
    res = sqlite3_mprintf("%s = %s", lvl, (v && *v) ? v : "0");
  }
  sqlite3_free(lvl);
  return res;
}

char *pvtBuildWhereFromJson(sqlite3 *db, const char *filter, const char *fields,
                                   const char *base_prefix){
  if(!base_prefix) base_prefix = "";
  if(!filter) return sqlite3_mprintf("TRUE");
  {
    char *ft = jsonTypeAt(db, filter, "$");
    int empty = 0;
    if(ft && strcmp(ft, "object") != 0){ sqlite3_free(ft); return 0; }
    sqlite3_free(ft);
    /* {} -> TRUE */
    int found = 0; (void)found;
    char *probe = jsonGetText(db, filter, "$");
    if(probe && !strcmp(probe, "{}")) empty = 1;
    sqlite3_free(probe);
    if(empty) return sqlite3_mprintf("TRUE");
  }

  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT key, type, value FROM json_each(?1)", -1, &it, 0);
  if(!it) return 0;
  sqlite3_bind_text(it, 1, filter, -1, SQLITE_TRANSIENT);

  char *parts = 0;
  int err = 0, n = 0;

  while(!err && sqlite3_step(it) == SQLITE_ROW){
    char *k    = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 0));
    char *kt   = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 1));
    char *v    = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 2));
    char *kl   = sqlite3_mprintf("%s", k);
    for(char *p = kl; *p; p++) if(*p >= 'A' && *p <= 'Z') *p += 32;

    char *frag = 0;

    if(!strcmp(kl, "$and") || !strcmp(kl, "$or")){
      const char *jsep = !strcmp(kl, "$and") ? " AND " : " OR ";
      const char *empty_lit = !strcmp(kl, "$and") ? "TRUE" : "FALSE";
      if(strcmp(kt, "array") != 0){ err = 1; }
      else{
        sqlite3_stmt *ai = 0;
        sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
        sqlite3_bind_text(ai, 1, v, -1, SQLITE_TRANSIENT);
        char *acc = 0; int cnt = 0, cerr = 0;
        while(sqlite3_step(ai) == SQLITE_ROW){
          char *elem = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0));
          char *cf = pvtBuildWhereFromJson(db, elem, fields, base_prefix);
          sqlite3_free(elem);
          if(!cf){ cerr = 1; sqlite3_free(acc); acc = 0; break; }
          acc = pvtJoin(acc, cf, jsep); cnt++;
        }
        sqlite3_finalize(ai);
        if(cerr) err = 1;
        else if(cnt == 0) frag = sqlite3_mprintf("%s", empty_lit);
        else { frag = sqlite3_mprintf("(%s)", acc); sqlite3_free(acc); }
      }
    }
    else if(!strcmp(kl, "$not")){
      char *cf = pvtBuildWhereFromJson(db, v, fields, base_prefix);
      if(!cf) err = 1; else { frag = sqlite3_mprintf("NOT (%s)", cf); sqlite3_free(cf); }
    }
    else if(!strcmp(kl,"$expr")){
      frag = pvtBuildBoolExpr(db, v, fields, base_prefix);
      if(!frag) err = 1;
    }
    else if(!strcmp(kl,"$eq")||!strcmp(kl,"$ne")||!strcmp(kl,"$lt")||!strcmp(kl,"$lte")||
            !strcmp(kl,"$gt")||!strcmp(kl,"$gte")||!strcmp(kl,"$like")||!strcmp(kl,"$ilike")||
            !strcmp(kl,"$in")||!strcmp(kl,"$nin")||!strcmp(kl,"$between")||
            !strcmp(kl,"$null")||!strcmp(kl,"$notnull")||!strcmp(kl,"$isnull")||!strcmp(kl,"$exists")||
            !strcmp(kl,"$contains")||!strcmp(kl,"$startswith")||!strcmp(kl,"$endswith")||
            !strcmp(kl,"$containsignorecase")||!strcmp(kl,"$startswithignorecase")||!strcmp(kl,"$endswithignorecase")){
      /* filter-level expression-form predicate (operands carry $field/$const). */
      frag = pvtBuildExprPredicate(db, k, v, fields, base_prefix);
      if(!frag) err = 1;
    }
    else if(!strcmp(kl,"$level")){
      frag = pvtBuildLevelCondition(db, v, kt, base_prefix);
      if(!frag) err = 1;
    }
    else if(kl[0]=='$'){
      /* hierarchical ($hasAncestor/...) + $regex/$fts: not yet ported (06/REGEXP). */
      err = 1;
    }
    else{
      /* field leaf — ContainsKey normalization then per-field condition. */
      char *peek = pvtPeekContainsKeyValue(db, v, kt);
      char *norm = pvtNormalizeFieldName(k, peek);
      char *mpath = sqlite3_mprintf("$.\"%w\"", norm);   /* fields[norm] */
      char *meta = jsonGetText(db, fields, mpath);
      sqlite3_free(mpath);
      if(!meta){ err = 1; }
      else{
        int was_ck = jsonGetBool(db, meta, "$.was_contains_key");
        if(strcmp(norm, k) != 0 && was_ck){
          frag = sqlite3_mprintf("\"%w\" IS NOT NULL", norm);
        }else{
          frag = pvtBuildFieldCondition(db, norm, meta, v, kt, base_prefix);
          if(!frag) err = 1;
        }
      }
      sqlite3_free(peek); sqlite3_free(norm); sqlite3_free(meta);
    }

    if(!err && frag){ parts = pvtJoin(parts, frag, " AND "); n++; }
    sqlite3_free(k); sqlite3_free(kt); sqlite3_free(v); sqlite3_free(kl);
  }
  sqlite3_finalize(it);

  if(err){ sqlite3_free(parts); return 0; }
  if(n == 0){ sqlite3_free(parts); return sqlite3_mprintf("TRUE"); }
  if(n == 1) return parts;
  { char *r = sqlite3_mprintf("(%s)", parts); sqlite3_free(parts); return r; }
}

/* ------------------------------------------------------------------------- */
/* 12_pvt_cte_builder.sql                                                    */
/* ------------------------------------------------------------------------- */

#define PVT_BASE_COLS \
  "o._id, o._id_parent, o._id_scheme, o._id_owner, o._id_who_change, " \
  "o._name, o._date_create, o._date_modify, o._date_begin, o._date_complete, " \
  "o._key, o._note, o._hash, " \
  "o._value_long, o._value_string, o._value_guid, o._value_bool, " \
  "o._value_double, o._value_numeric, o._value_datetime, o._value_bytes"

/* "[1,2,3]" -> "1, 2, 3" (bare int IN-list). */
static char *pvtInListFromJsonArray(sqlite3 *db, const char *arr){
  sqlite3_str *o = sqlite3_str_new(db);
  sqlite3_stmt *st = 0; int first = 1;
  sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &st, 0);
  if(st){ sqlite3_bind_text(st, 1, arr, -1, SQLITE_TRANSIENT);
    while(sqlite3_step(st) == SQLITE_ROW){
      if(!first) sqlite3_str_append(o, ", ", 2); first = 0;
      sqlite3_str_appendf(o, "%lld", (long long)sqlite3_column_int64(st, 0));
    }
    sqlite3_finalize(st);
  }
  return sqlite3_str_finish(o);
}

/* pvt_build_cte_sql — see 12_pvt_cte_builder.sql. SQLite translations:
**   ANY(ARRAY[..]::bigint[]) -> IN (..); array_agg(x)[1] -> MAX(x);
**   array_agg(x) -> json_group_array(x); WITH RECURSIVE for tree walks.
** Returns sqlite3_malloc'd SQL, or NULL on unsupported (nested-dict groups). */
char *pvtBuildCteSql(sqlite3 *db, sqlite3_int64 scheme, const char *fields,
                     const char *source_mode, const char *tree_ids_json,
                            int has_max_depth, int max_depth, int force_outer,
                            const char *extra_where, int narrow, int include_seed,
                            int polymorphic, const char *residual_where){
  if(!source_mode) source_mode = "flat";
  int is_flat = !strcmp(source_mode, "flat");

  /* --- partition fields --- */
  sqlite3_str *pcols = sqlite3_str_new(db);   /* each entry leads with ",\n            " */
  sqlite3_str *aliasw = sqlite3_str_new(db);  /* ", pvt.\"alias\"" per projected col */
  sqlite3_int64 *sids = 0; int nsids = 0, capsids = 0;
  char **nd_name = 0; char **nd_meta = 0; int nnd = 0, capnd = 0;   /* nested-dict fields */
  int has_listitem_join = 0, err = 0;

  if(fields){
    sqlite3_stmt *it = 0;
    sqlite3_prepare_v2(db, "SELECT key, value FROM json_each(?1)", -1, &it, 0);
    if(it){ sqlite3_bind_text(it, 1, fields, -1, SQLITE_TRANSIENT);
      while(!err && sqlite3_step(it) == SQLITE_ROW){
        const char *fname = (const char*)sqlite3_column_text(it, 0);
        char *meta = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 1));
        char *kind = jsonGetText(db, meta, "$.kind");
        int len_mod = jsonGetBool(db, meta, "$.length_modifier");
        char *dictk = jsonGetText(db, meta, "$.dict_key");
        char *psid  = jsonGetText(db, meta, "$.parent_sid");
        char *liprop= jsonGetText(db, meta, "$.list_item_prop");
        int found = 0; sqlite3_int64 sid = jsonGetInt(db, meta, "$.sid", &found);

        int is_base = kind && !strcmp(kind, "base");
        if(is_base || len_mod){ /* skip */ }
        else if(dictk && psid){
          /* nested-dict (dict-of-class child) -> own side CTE, joined into outer. */
          if(nnd == capnd){ capnd = capnd ? capnd*2 : 4;
            nd_name = (char**)sqlite3_realloc(nd_name, sizeof(char*)*capnd);
            nd_meta = (char**)sqlite3_realloc(nd_meta, sizeof(char*)*capnd); }
          nd_name[nnd] = sqlite3_mprintf("%s", fname);
          nd_meta[nnd] = sqlite3_mprintf("%s", meta);
          nnd++;
          sqlite3_str_appendf(aliasw, ", pvt.\"%w\"", fname);
        }
        else{
          if(found && !psid){
            if(nsids == capsids){ capsids = capsids ? capsids*2 : 8; sids = (sqlite3_int64*)sqlite3_realloc(sids, sizeof(sqlite3_int64)*capsids); }
            sids[nsids++] = sid;
          }
          char *expr = pvtBuildColumnExpr(db, fname, meta, 0);
          if(!expr) err = 1;
          else{
            sqlite3_str_appendf(pcols, ",\n            %s", expr);
            sqlite3_str_appendf(aliasw, ", pvt.\"%w\"", fname);
            sqlite3_free(expr);
            if(liprop && (!strcmp(liprop,"Value")||!strcmp(liprop,"Alias"))) has_listitem_join = 1;
          }
        }
        sqlite3_free(kind); sqlite3_free(dictk); sqlite3_free(psid); sqlite3_free(liprop); sqlite3_free(meta);
      }
      sqlite3_finalize(it);
    }
  }
  if(err){
    sqlite3_free(sqlite3_str_finish(pcols)); sqlite3_free(sqlite3_str_finish(aliasw)); sqlite3_free(sids);
    for(int i=0;i<nnd;i++){ sqlite3_free(nd_name[i]); sqlite3_free(nd_meta[i]); } sqlite3_free(nd_name); sqlite3_free(nd_meta);
    return 0;
  }

  char *pivot_cols = sqlite3_str_finish(pcols);   /* may be "" */
  char *alias_wrap = sqlite3_str_finish(aliasw);

  /* --- tree CTE + restriction --- */
  char *tree_cte = 0;      /* "_pvt_tree(...) AS (...)" or NULL */
  char *tree_filter = 0;   /* "o._id IN (SELECT _id FROM _pvt_tree ...)" or NULL */
  int has_recursive = 0;
  char *seed = (tree_ids_json && strcmp(tree_ids_json,"null")!=0) ? pvtInListFromJsonArray(db, tree_ids_json) : 0;
  char scheme_pred[64] = "";
  if(!is_flat && !polymorphic) sqlite3_snprintf(sizeof(scheme_pred), scheme_pred, " AND o._id_scheme = %lld", (long long)scheme);

  if(!strcmp(source_mode,"tree") || !strcmp(source_mode,"tree_descendants")){
    if(!seed){ sqlite3_free(pivot_cols); sqlite3_free(alias_wrap); sqlite3_free(sids); return 0; }
    char *cap = has_max_depth ? sqlite3_mprintf("t.depth < %d", max_depth) : sqlite3_mprintf("TRUE");
    int needw = has_max_depth || !polymorphic;
    tree_cte = sqlite3_mprintf(
      "_pvt_tree(_id, depth) AS (\n    SELECT _id, 0 FROM _objects WHERE _id IN (%s)\n    UNION ALL\n"
      "    SELECT o._id, t.depth + 1 FROM _objects o JOIN _pvt_tree t ON o._id_parent = t._id%s%s%s\n)",
      seed, needw?" WHERE ":"", needw?cap:"", (!polymorphic)?scheme_pred:"");
    sqlite3_free(cap);
    has_recursive = 1;
    tree_filter = include_seed
      ? sqlite3_mprintf("o._id IN (SELECT _id FROM _pvt_tree)")
      : sqlite3_mprintf("o._id IN (SELECT _id FROM _pvt_tree WHERE depth > 0)");
  }else if(!strcmp(source_mode,"tree_children")){
    if(!seed){ sqlite3_free(pivot_cols); sqlite3_free(alias_wrap); sqlite3_free(sids); return 0; }
    tree_cte = sqlite3_mprintf(
      "_pvt_tree(_id, depth) AS (\n    SELECT o._id, 1 FROM _objects o WHERE o._id_parent IN (%s)%s\n)",
      seed, (!polymorphic)?scheme_pred:"");
    tree_filter = sqlite3_mprintf("o._id IN (SELECT _id FROM _pvt_tree)");
  }else if(!strcmp(source_mode,"tree_roots")){
    tree_cte = sqlite3_mprintf(
      "_pvt_tree(_id, depth) AS (\n    SELECT o._id, 0 FROM _objects o WHERE o._id_parent IS NULL AND o._id_scheme = %lld%s%s\n)",
      (long long)scheme, seed?" AND o._id IN (":"", seed?seed:"");
    if(seed){ char *t = sqlite3_mprintf("%s)", tree_cte); sqlite3_free(tree_cte); tree_cte = t; }
    tree_filter = sqlite3_mprintf("o._id IN (SELECT _id FROM _pvt_tree)");
  }else if(!strcmp(source_mode,"tree_leaves")){
    tree_cte = sqlite3_mprintf(
      "_pvt_tree(_id, depth) AS (\n    SELECT o._id, 0 FROM _objects o WHERE o._id_scheme = %lld"
      " AND NOT EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = o._id)%s%s\n)",
      (long long)scheme, seed?" AND o._id IN (":"", seed?seed:"");
    if(seed){ char *t = sqlite3_mprintf("%s)", tree_cte); sqlite3_free(tree_cte); tree_cte = t; }
    tree_filter = sqlite3_mprintf("o._id IN (SELECT _id FROM _pvt_tree)");
  }else if(!strcmp(source_mode,"tree_ancestors")){
    if(!seed){ sqlite3_free(pivot_cols); sqlite3_free(alias_wrap); sqlite3_free(sids); return 0; }
    char *cap = has_max_depth ? sqlite3_mprintf(" AND t.depth < %d", max_depth) : sqlite3_mprintf("");
    tree_cte = sqlite3_mprintf(
      "_pvt_tree(_id, depth) AS (\n    SELECT seed._id_parent, 1 FROM _objects seed WHERE seed._id IN (%s) AND seed._id_parent IS NOT NULL\n    UNION ALL\n"
      "    SELECT o._id_parent, t.depth + 1 FROM _objects o JOIN _pvt_tree t ON o._id = t._id WHERE o._id_parent IS NOT NULL%s%s\n)",
      seed, cap, (!polymorphic)?scheme_pred:"");
    sqlite3_free(cap);
    has_recursive = 1;
    tree_filter = sqlite3_mprintf("o._id IN (SELECT _id FROM _pvt_tree)");
  }
  sqlite3_free(seed);

  /* --- wide WHERE (scheme + tree + pushdown) --- */
  sqlite3_str *wb = sqlite3_str_new(db);
  sqlite3_str_appendf(wb, "o._id_scheme = %lld", (long long)scheme);
  if(tree_filter) sqlite3_str_appendf(wb, " AND %s", tree_filter);
  if(extra_where && *extra_where) sqlite3_str_appendf(wb, " AND %s", extra_where);
  char *wide_where = sqlite3_str_finish(wb);

  /* --- inner pivot body: narrow vs wide --- */
  int can_narrow = narrow && !force_outer && nsids > 0;
  char *inner = 0;
  if(can_narrow){
    /* dedup sids -> IN text */
    sqlite3_str *sb = sqlite3_str_new(db); int first = 1;
    for(int i = 0; i < nsids; i++){ int dup = 0; for(int j = 0; j < i; j++) if(sids[j]==sids[i]){dup=1;break;} if(dup) continue;
      if(!first) sqlite3_str_append(sb, ", ", 2); first = 0; sqlite3_str_appendf(sb, "%lld", (long long)sids[i]); }
    char *sidtext = sqlite3_str_finish(sb);
    sqlite3_str *ob = sqlite3_str_new(db);
    sqlite3_str_appendf(ob, "(SELECT o._id FROM _objects o WHERE o._id_scheme = %lld", (long long)scheme);
    if(extra_where && *extra_where) sqlite3_str_appendf(ob, " AND %s", extra_where);
    if(tree_filter) sqlite3_str_appendf(ob, " AND %s", tree_filter);
    sqlite3_str_append(ob, ")", 1);
    char *objsubq = sqlite3_str_finish(ob);
    inner = sqlite3_mprintf(
      "SELECT\n            v._id_object%s\n        FROM _values v%s\n        WHERE v._id_structure IN (%s)\n          AND v._id_object IN %s\n        GROUP BY v._id_object",
      pivot_cols,
      has_listitem_join ? "\n        LEFT JOIN _list_items li ON li._id = v._ListItem" : "",
      sidtext, objsubq);
    sqlite3_free(sidtext); sqlite3_free(objsubq);
  }else{
    inner = sqlite3_mprintf(
      "SELECT\n            %s%s\n        FROM _objects o\n        %s _values v ON v._id_object = o._id%s\n        WHERE %s\n        GROUP BY %s",
      PVT_BASE_COLS, pivot_cols,
      force_outer ? "LEFT JOIN" : "INNER JOIN",
      has_listitem_join ? "\n        LEFT JOIN _list_items li ON li._id = v._ListItem" : "",
      wide_where, PVT_BASE_COLS);
  }

  /* --- nested-dict side CTEs (dict-of-class) --- */
  char *nd_ctes = 0;
  if(nnd > 0){
    sqlite3_str *of = sqlite3_str_new(db);   /* object-set restriction folded into each CTE */
    sqlite3_str_appendf(of, " AND dp._id_object IN (SELECT o._id FROM _objects o WHERE o._id_scheme = %lld", (long long)scheme);
    if(extra_where && *extra_where) sqlite3_str_appendf(of, " AND %s", extra_where);
    if(tree_filter) sqlite3_str_appendf(of, " AND %s", tree_filter);
    sqlite3_str_append(of, ")", 1);
    char *objf = sqlite3_str_finish(of);

    sqlite3_str *ctes = sqlite3_str_new(db);
    sqlite3_str *ndcols = sqlite3_str_new(db);
    for(int i = 0; i < nnd; i++){
      char *m = nd_meta[i]; const char *al = nd_name[i];
      int f = 0; sqlite3_int64 csid = jsonGetInt(db, m, "$.sid", &f);
      char *dbt = jsonGetText(db, m, "$.db_type");
      const char *col = pvtDbTypeToValueColumn(dbt ? dbt : "");
      int is_arr = jsonGetBool(db, m, "$.is_array");
      char *psidv = jsonGetText(db, m, "$.parent_sid");
      char *dk = jsonGetText(db, m, "$.dict_key");
      char *childcol = is_arr
        ? sqlite3_mprintf("json_group_array(nv.%s) FILTER (WHERE nv._id_structure = %lld AND nv._array_index IS NOT NULL)", col?col:"_String", (long long)csid)
        : sqlite3_mprintf("MAX(nv.%s) FILTER (WHERE nv._id_structure = %lld AND nv._array_index IS NULL)", col?col:"_String", (long long)csid);
      if(i) sqlite3_str_append(ctes, ",\n", 2);
      sqlite3_str_appendf(ctes,
        "nested_dict_%d AS (\n        SELECT dp._id_object, %s AS \"%w\"\n        FROM _values dp\n        LEFT JOIN _values nv ON nv._array_parent_id = dp._id AND nv._id_structure = %lld\n        WHERE dp._id_structure = %s AND dp._array_index = %Q%s\n        GROUP BY dp._id_object\n    )",
        i+1, childcol, al, (long long)csid, psidv?psidv:"0", dk?dk:"", objf);
      sqlite3_str_appendf(ndcols, ", nested_dict_%d.\"%w\" AS \"%w\"", i+1, al, al);
      sqlite3_free(childcol); sqlite3_free(dbt); sqlite3_free(psidv); sqlite3_free(dk);
    }
    nd_ctes = sqlite3_str_finish(ctes);
    char *ndcols_s = sqlite3_str_finish(ndcols);

    int pivot_empty = (!pivot_cols || pivot_cols[0] == 0);
    if(narrow && !force_outer && pivot_empty){
      /* nested-only: flatten the nested CTEs (no base/scalar pivot). */
      sqlite3_str *fb = sqlite3_str_new(db);
      sqlite3_str_appendf(fb, "SELECT nested_dict_1._id_object%s\n        FROM nested_dict_1", ndcols_s);
      for(int i = 2; i <= nnd; i++) sqlite3_str_appendf(fb, "\n        LEFT JOIN nested_dict_%d ON nested_dict_%d._id_object = nested_dict_1._id_object", i, i);
      sqlite3_free(inner); inner = sqlite3_str_finish(fb);
    }else{
      /* mixed: wrap the scalar/base pivot + LEFT JOIN every nested group. */
      const char *key = can_narrow ? "_id_object" : "_id";
      sqlite3_str *pb = sqlite3_str_new(db);
      sqlite3_str_appendf(pb, "SELECT pi.*%s\n        FROM (%s) pi", ndcols_s, inner);
      for(int i = 1; i <= nnd; i++) sqlite3_str_appendf(pb, "\n        LEFT JOIN nested_dict_%d ON nested_dict_%d._id_object = pi.%s", i, i, key);
      sqlite3_free(inner); inner = sqlite3_str_finish(pb);
    }
    sqlite3_free(ndcols_s); sqlite3_free(objf);
  }

  /* --- residual WHERE pushdown wrapper --- */
  char *body = inner;
  if(residual_where && *residual_where && strcmp(residual_where,"TRUE")!=0){
    char *wrapped = sqlite3_mprintf(
      "SELECT pvt._id_object%s\n        FROM (\n        %s\n        ) pvt WHERE %s",
      alias_wrap, body, residual_where);
    sqlite3_free(body); body = wrapped;
  }

  /* --- assemble --- */
  sqlite3_str *out = sqlite3_str_new(db);
  sqlite3_str_appendf(out, "%s", has_recursive ? "WITH RECURSIVE " : "WITH ");
  if(tree_cte) sqlite3_str_appendf(out, "%s,\n", tree_cte);
  if(nd_ctes && *nd_ctes) sqlite3_str_appendf(out, "%s,\n", nd_ctes);
  sqlite3_str_appendf(out, "_pvt_cte AS (\n        %s\n    )", body);
  char *res = sqlite3_str_finish(out);

  sqlite3_free(pivot_cols); sqlite3_free(alias_wrap); sqlite3_free(sids);
  sqlite3_free(tree_cte); sqlite3_free(tree_filter); sqlite3_free(wide_where); sqlite3_free(body);
  sqlite3_free(nd_ctes);
  for(int i=0;i<nnd;i++){ sqlite3_free(nd_name[i]); sqlite3_free(nd_meta[i]); } sqlite3_free(nd_name); sqlite3_free(nd_meta);
  return res;
}

/* ------------------------------------------------------------------------- */
/* 15_pvt_order.sql                                                          */
/* ------------------------------------------------------------------------- */

/* compile one order/distinct entry's column expr; NULL on $expr (stub). */
char *pvtCompileOrderCol(sqlite3 *db, const char *elem, const char *fields,
                         const char *base_prefix){
  char *xt = jsonTypeAt(db, elem, "$.\"$expr\"");
  if(xt){ sqlite3_free(xt);
    char *e = jsonGetText(db, elem, "$.\"$expr\"");
    char *r = e ? pvtBuildScalarExpr(db, e, fields, base_prefix) : 0;
    sqlite3_free(e); return r; }
  char *field = jsonGetText(db, elem, "$.field");
  if(!field) field = jsonGetText(db, elem, "$.field_path");
  if(!field) return 0;
  char *mpath = sqlite3_mprintf("$.\"%w\"", field);
  char *meta = jsonGetText(db, fields, mpath);
  sqlite3_free(mpath);
  char *res = 0;
  if(meta){
    char *kind = jsonGetText(db, meta, "$.kind");
    char *col  = jsonGetText(db, meta, "$.column");
    if(kind && !strcmp(kind, "base")) res = sqlite3_mprintf("%s%s", base_prefix, col ? col : "_id");
    else                              res = sqlite3_mprintf("\"%w\"", field);
    sqlite3_free(kind); sqlite3_free(col);
  }
  sqlite3_free(field); sqlite3_free(meta);
  return res;   /* NULL if field had no metadata */
}

/* pvt_build_order_conditions: "\nORDER BY ..." or "". distinct_on ignored
** (SQLite has no DISTINCT ON; TODO ROW_NUMBER). */
char *pvtBuildOrderConditions(sqlite3 *db, const char *order, const char *fields,
                              const char *base_prefix){
  if(!base_prefix) base_prefix = "";
  if(!order) return sqlite3_mprintf("");
  char *ot = jsonTypeAt(db, order, "$");
  int isarr = ot && !strcmp(ot, "array");
  sqlite3_free(ot);
  if(!isarr) return sqlite3_mprintf("");

  sqlite3_str *parts = sqlite3_str_new(db);
  int n = 0, err = 0;
  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it, 1, order, -1, SQLITE_TRANSIENT);
  while(!err && sqlite3_step(it) == SQLITE_ROW){
    char *elem = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 0));
    char *col = pvtCompileOrderCol(db, elem, fields, base_prefix);
    if(!col){ err = 1; sqlite3_free(elem); break; }
    char *dir = jsonGetText(db, elem, "$.dir");
    if(!dir) dir = jsonGetText(db, elem, "$.direction");
    char *nulls = jsonGetText(db, elem, "$.nulls");
    int desc = dir && (dir[0]=='d' || dir[0]=='D');
    if(n) sqlite3_str_append(parts, ", ", 2);
    sqlite3_str_appendf(parts, "%s %s", col, desc ? "DESC" : "ASC");
    if(nulls){ if(nulls[0]=='f'||nulls[0]=='F') sqlite3_str_append(parts, " NULLS FIRST", 12);
               else if(nulls[0]=='l'||nulls[0]=='L') sqlite3_str_append(parts, " NULLS LAST", 11); }
    n++;
    sqlite3_free(col); sqlite3_free(dir); sqlite3_free(nulls); sqlite3_free(elem);
  }
  sqlite3_finalize(it);
  if(err || n == 0){ sqlite3_free(sqlite3_str_finish(parts)); return sqlite3_mprintf(""); }
  char *body = sqlite3_str_finish(parts);
  char *res = sqlite3_mprintf("\nORDER BY %s", body);
  sqlite3_free(body);
  return res;
}

/* ------------------------------------------------------------------------- */
/* 16_pvt_split.sql — base-field pushdown splitter                           */
/* ------------------------------------------------------------------------- */

void pvtSplitFilter(sqlite3 *db, const char *filter, const char *fields,
                    char **out_push, char **out_resid);

static char *pvtSingleton(sqlite3 *db, const char *k, const char *v_json){
  sqlite3_stmt *st = 0; char *r = 0;
  sqlite3_prepare_v2(db, "SELECT json_object(?1, json(?2))", -1, &st, 0);
  if(st){ sqlite3_bind_text(st, 1, k, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, v_json, -1, SQLITE_TRANSIENT);
    if(sqlite3_step(st) == SQLITE_ROW) r = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); }
  return r;
}

void pvtSplitFilter(sqlite3 *db, const char *filter, const char *fields,
                    char **out_push, char **out_resid){
  *out_push = 0; *out_resid = 0;
  if(!filter) return;
  char *ft = jsonTypeAt(db, filter, "$");
  int isobj = ft && !strcmp(ft, "object");
  sqlite3_free(ft);
  if(!isobj) return;
  char *probe = jsonGetText(db, filter, "$");
  int empty = probe && !strcmp(probe, "{}");
  sqlite3_free(probe);
  if(empty) return;

  /* gather keys */
  int key_count = 0; char *logical = 0; char *logical_val = 0;
  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT key, value, type FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it, 1, filter, -1, SQLITE_TRANSIENT);
  char *k0 = 0, *v0 = 0, *t0 = 0;   /* remember single leaf */
  while(sqlite3_step(it) == SQLITE_ROW){
    const char *k = (const char*)sqlite3_column_text(it, 0);
    key_count++;
    char lk[8]; int i = 0; for(; k && k[i] && i < 7; i++){ char ch = k[i]; lk[i] = (ch>='A'&&ch<='Z')?ch+32:ch; } lk[i] = 0;
    if(!strcmp(lk,"$and") || !strcmp(lk,"$or") || !strcmp(lk,"$not")){
      sqlite3_free(logical); logical = sqlite3_mprintf("%s", lk);
      sqlite3_free(logical_val); logical_val = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 1));
    }
    if(!k0){ k0 = sqlite3_mprintf("%s", k); v0 = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 1)); t0 = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 2)); }
  }
  sqlite3_finalize(it);

  /* 1) logical singleton */
  if(logical && key_count == 1){
    if(!strcmp(logical, "$and")){
      sqlite3_str *pushes = sqlite3_str_new(db); int np = 0;
      sqlite3_str *resids = sqlite3_str_new(db); int nr = 0;
      sqlite3_stmt *ai = 0; sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
      sqlite3_bind_text(ai, 1, logical_val, -1, SQLITE_TRANSIENT);
      while(sqlite3_step(ai) == SQLITE_ROW){
        char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0));
        char *cp = 0, *cr = 0; pvtSplitFilter(db, e, fields, &cp, &cr);
        if(cp){ if(np) sqlite3_str_append(pushes, " AND ", 5); sqlite3_str_appendf(pushes, "%s", cp); np++; sqlite3_free(cp); }
        if(cr){ if(nr) sqlite3_str_append(resids, ",", 1); sqlite3_str_appendf(resids, "%s", cr); nr++; sqlite3_free(cr); }
        sqlite3_free(e);
      }
      sqlite3_finalize(ai);
      char *pb = sqlite3_str_finish(pushes);
      if(np == 1) *out_push = sqlite3_mprintf("%s", pb);
      else if(np > 1) *out_push = sqlite3_mprintf("(%s)", pb);
      sqlite3_free(pb);
      char *rb = sqlite3_str_finish(resids);
      if(nr == 1) *out_resid = sqlite3_mprintf("%s", rb);
      else if(nr > 1) *out_resid = sqlite3_mprintf("{\"$and\":[%s]}", rb);
      sqlite3_free(rb);
    }else if(!strcmp(logical, "$or")){
      int all_push = 1; sqlite3_str *ors = sqlite3_str_new(db); int no = 0;
      sqlite3_stmt *ai = 0; sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
      sqlite3_bind_text(ai, 1, logical_val, -1, SQLITE_TRANSIENT);
      while(sqlite3_step(ai) == SQLITE_ROW){
        char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0));
        char *cp = 0, *cr = 0; pvtSplitFilter(db, e, fields, &cp, &cr);
        if(cr || !cp){ all_push = 0; sqlite3_free(cp); sqlite3_free(cr); sqlite3_free(e); break; }
        if(no) sqlite3_str_append(ors, " OR ", 4); sqlite3_str_appendf(ors, "%s", cp); no++;
        sqlite3_free(cp); sqlite3_free(cr); sqlite3_free(e);
      }
      sqlite3_finalize(ai);
      char *ob = sqlite3_str_finish(ors);
      if(all_push && no > 0){ *out_push = sqlite3_mprintf("(%s)", ob); *out_resid = 0; }
      else { *out_push = 0; *out_resid = sqlite3_mprintf("%s", filter); }
      sqlite3_free(ob);
    }else{ /* $not */
      char *cp = 0, *cr = 0; pvtSplitFilter(db, logical_val, fields, &cp, &cr);
      if(!cr && cp){ *out_push = sqlite3_mprintf("NOT (%s)", cp); *out_resid = 0; }
      else { *out_push = 0; *out_resid = sqlite3_mprintf("%s", filter); }
      sqlite3_free(cp); sqlite3_free(cr);
    }
    sqlite3_free(logical); sqlite3_free(logical_val);
    sqlite3_free(k0); sqlite3_free(v0); sqlite3_free(t0);
    return;
  }
  sqlite3_free(logical); sqlite3_free(logical_val);

  /* 2) multi-key implicit $and */
  if(key_count > 1){
    sqlite3_str *pushes = sqlite3_str_new(db); int np = 0;
    sqlite3_str *resids = sqlite3_str_new(db); int nr = 0;
    /* Re-encode each value as a valid JSON atom so pvtSingleton's json() doesn't choke. json_each's
    ** `value` column drops type for nulls (SQL NULL) and strips quotes from text, which previously made
    ** a shorthand leaf like {"0$:ParentId":null} (or a bare text value) reconstruct as invalid JSON and
    ** vanish — silently dropping that condition when the filter had more than one key. */
    sqlite3_stmt *ei = 0;
    sqlite3_prepare_v2(db,
      "SELECT key, CASE type WHEN 'text' THEN json_quote(value) WHEN 'null' THEN 'null' "
      "WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE value END "
      "FROM json_each(?1)", -1, &ei, 0);
    sqlite3_bind_text(ei, 1, filter, -1, SQLITE_TRANSIENT);
    while(sqlite3_step(ei) == SQLITE_ROW){
      const char *k = (const char*)sqlite3_column_text(ei, 0);
      char *v = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ei, 1));
      char *single = pvtSingleton(db, k, v);
      char *cp = 0, *cr = 0; pvtSplitFilter(db, single, fields, &cp, &cr);
      if(cp){ if(np) sqlite3_str_append(pushes, " AND ", 5); sqlite3_str_appendf(pushes, "%s", cp); np++; sqlite3_free(cp); }
      if(cr){ if(nr) sqlite3_str_append(resids, ",", 1); sqlite3_str_appendf(resids, "%s", cr); nr++; sqlite3_free(cr); }
      sqlite3_free(v); sqlite3_free(single);
    }
    sqlite3_finalize(ei);
    char *pb = sqlite3_str_finish(pushes);
    if(np == 1) *out_push = sqlite3_mprintf("%s", pb);
    else if(np > 1) *out_push = sqlite3_mprintf("(%s)", pb);
    sqlite3_free(pb);
    char *rb = sqlite3_str_finish(resids);
    if(nr == 1) *out_resid = sqlite3_mprintf("%s", rb);
    else if(nr > 1) *out_resid = sqlite3_mprintf("{\"$and\":[%s]}", rb);
    sqlite3_free(rb);
    sqlite3_free(k0); sqlite3_free(v0); sqlite3_free(t0);
    return;
  }

  /* 3) single leaf */
  {
    char lk[16]; int i = 0; for(; k0 && k0[i] && i < 15; i++){ char ch = k0[i]; lk[i] = (ch>='A'&&ch<='Z')?ch+32:ch; } lk[i] = 0;
    int is_expr = !strcmp(lk,"$expr");
    int is_exprform = !strcmp(lk,"$eq")||!strcmp(lk,"$ne")||!strcmp(lk,"$lt")||!strcmp(lk,"$lte")||
                      !strcmp(lk,"$gt")||!strcmp(lk,"$gte")||!strcmp(lk,"$like")||!strcmp(lk,"$ilike")||
                      !strcmp(lk,"$in")||!strcmp(lk,"$nin")||!strcmp(lk,"$between")||
                      !strcmp(lk,"$null")||!strcmp(lk,"$notnull")||!strcmp(lk,"$isnull")||!strcmp(lk,"$exists")||
                      !strcmp(lk,"$contains")||!strcmp(lk,"$startswith")||!strcmp(lk,"$endswith")||
                      !strcmp(lk,"$containsignorecase")||!strcmp(lk,"$startswithignorecase")||!strcmp(lk,"$endswithignorecase");
    if(is_expr || is_exprform){
      /* push iff every $field inside resolves to a base column. */
      if(pvtExprIsBaseOnly(db, v0, fields)){
        *out_push = is_expr ? pvtBuildBoolExpr(db, v0, fields, "o.")
                            : pvtBuildExprPredicate(db, k0, v0, fields, "o.");
        *out_resid = 0;
        if(!*out_push) *out_resid = sqlite3_mprintf("%s", filter);   /* unsupported -> residual */
      }else{ *out_push = 0; *out_resid = sqlite3_mprintf("%s", filter); }
    }
    else if(k0 && k0[0] == '$'){ *out_push = 0; *out_resid = sqlite3_mprintf("%s", filter); }
    else{
      char *peek = pvtPeekContainsKeyValue(db, v0, t0);
      char *norm = pvtNormalizeFieldName(k0, peek);
      char *mpath = sqlite3_mprintf("$.\"%w\"", norm);
      char *meta = jsonGetText(db, fields, mpath);
      sqlite3_free(mpath);
      char *kind = meta ? jsonGetText(db, meta, "$.kind") : 0;
      if(meta && kind && !strcmp(kind, "base")){
        *out_push = pvtBuildFieldCondition(db, norm, meta, v0, t0, "o.");
        *out_resid = 0;
        if(!*out_push) *out_resid = sqlite3_mprintf("%s", filter);  /* fallback */
      }else{
        *out_push = 0; *out_resid = sqlite3_mprintf("%s", filter);
      }
      sqlite3_free(peek); sqlite3_free(norm); sqlite3_free(meta); sqlite3_free(kind);
    }
  }
  sqlite3_free(k0); sqlite3_free(v0); sqlite3_free(t0);
}

static int pvtFilterHasBaseRefs(sqlite3 *db, const char *filter, const char *fields){
  if(!filter) return 0;
  char *ft = jsonTypeAt(db, filter, "$");
  int isobj = ft && !strcmp(ft, "object");
  sqlite3_free(ft);
  if(!isobj) return 1;
  char *probe = jsonGetText(db, filter, "$");
  int empty = probe && !strcmp(probe, "{}");
  sqlite3_free(probe);
  if(empty) return 0;

  int base = 0;
  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT key, type, value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it, 1, filter, -1, SQLITE_TRANSIENT);
  while(!base && sqlite3_step(it) == SQLITE_ROW){
    const char *k = (const char*)sqlite3_column_text(it, 0);
    const char *ty = (const char*)sqlite3_column_text(it, 1);
    char *v = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 2));
    char lk[16]; int i = 0; for(; k && k[i] && i < 15; i++){ char ch = k[i]; lk[i] = (ch>='A'&&ch<='Z')?ch+32:ch; } lk[i] = 0;
    if(!strcmp(lk,"$hasancestor")||!strcmp(lk,"$hasdescendant")||!strcmp(lk,"$level")||!strcmp(lk,"$isroot")||!strcmp(lk,"$isleaf")){ base = 1; }
    else if(!strcmp(lk,"$and")||!strcmp(lk,"$or")){
      if(ty && !strcmp(ty,"array")){
        sqlite3_stmt *ai = 0; sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
        sqlite3_bind_text(ai, 1, v, -1, SQLITE_TRANSIENT);
        while(!base && sqlite3_step(ai) == SQLITE_ROW){ char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0)); if(pvtFilterHasBaseRefs(db, e, fields)) base = 1; sqlite3_free(e); }
        sqlite3_finalize(ai);
      }
    }else if(!strcmp(lk,"$not")){ if(pvtFilterHasBaseRefs(db, v, fields)) base = 1; }
    else if(k && k[0] == '$'){ base = 1; }   /* $expr / expr-form: conservative */
    else{
      char *peek = pvtPeekContainsKeyValue(db, v, ty);
      char *norm = pvtNormalizeFieldName(k, peek);
      char *mpath = sqlite3_mprintf("$.\"%w\"", norm);
      char *meta = jsonGetText(db, fields, mpath);
      sqlite3_free(mpath);
      if(!meta) base = 1;
      else{ char *kind = jsonGetText(db, meta, "$.kind"); if(kind && !strcmp(kind,"base")) base = 1; sqlite3_free(kind); }
      sqlite3_free(peek); sqlite3_free(norm); sqlite3_free(meta);
    }
    sqlite3_free(v);
  }
  sqlite3_finalize(it);
  return base;
}

/* ------------------------------------------------------------------------- */
/* 20_pvt_build_query_sql.sql — orchestrator                                 */
/* ------------------------------------------------------------------------- */

/* any field with kind != base ? */
int pvtHasPropFields(sqlite3 *db, const char *fields){
  int has = 0;
  sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it, 1, fields, -1, SQLITE_TRANSIENT);
  while(!has && sqlite3_step(it) == SQLITE_ROW){
    char *m = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(it, 0));
    char *kind = jsonGetText(db, m, "$.kind");
    if(!kind || strcmp(kind, "base") != 0) has = 1;
    sqlite3_free(kind); sqlite3_free(m);
  }
  sqlite3_finalize(it);
  return has;
}

/* DISTINCT ON <field> has no SQLite equivalent; we emulate it via ROW_NUMBER() OVER (PARTITION BY
** <field> ORDER BY o._id) in a chained _ranked CTE, keeping rn=1 (mirrors redb.SQLite.Pro). Returns
** the PARTITION BY expression for the first distinct_on entry, or NULL. distinct_on is the C# shape
** [{"field":"Name"}] (prop) / [{"field":"0$:Name"}] (base); the field is collected into `fields`. */
static char *pvtDistinctPartitionExpr(sqlite3 *db, const char *distinct_on, const char *fields){
  if(!distinct_on || !*distinct_on || !strcmp(distinct_on, "null")) return 0;
  char *fld = jsonGetText(db, distinct_on, "$[0].field");
  if(!fld) return 0;
  char *mp = sqlite3_mprintf("$.\"%w\"", fld);
  char *meta = jsonGetText(db, fields, mp); sqlite3_free(mp);
  char *expr = 0;
  if(meta){
    char *kind = jsonGetText(db, meta, "$.kind");
    char *col  = jsonGetText(db, meta, "$.column");
    if(kind && !strcmp(kind, "base")) expr = sqlite3_mprintf("o.%s", col ? col : "_id");
    else                              expr = sqlite3_mprintf("_pvt_cte.\"%w\"", fld);
    sqlite3_free(kind); sqlite3_free(col);
  }
  sqlite3_free(meta); sqlite3_free(fld);
  return expr;
}

static char *pvtBuildQuerySql(sqlite3 *db, sqlite3_int64 scheme, const char *filter,
                              int has_limit, int limit, int offset, const char *order,
                              int has_max_depth, int max_depth, int distinct,
                              const char *source_mode, const char *tree_ids_json,
                              int include_seed, int polymorphic, const char *distinct_on){
  if(!source_mode) source_mode = "flat";
  int is_flat = !strcmp(source_mode, "flat");
  int no_tree = !tree_ids_json || !strcmp(tree_ids_json, "null");

  /* Collect from filter+order AND the distinct_on field, so it is pivoted into the CTE and the
  ** PARTITION BY expression can reference it. distinct_on shares the order array shape. */
  int have_dion = distinct_on && *distinct_on && strcmp(distinct_on, "null") && strcmp(distinct_on, "[]");
  char *merged_order = 0;
  const char *collect_src = order;
  if(have_dion){
    if(order && *order && strcmp(order, "null") && strcmp(order, "[]"))
      merged_order = sqlite3_mprintf("%.*s,%s", (int)(strlen(order) - 1), order, distinct_on + 1);
    else
      merged_order = sqlite3_mprintf("%s", distinct_on);
    collect_src = merged_order;
  }

  char *fields = pvtCollectFields(db, scheme, filter, collect_src, 0);
  sqlite3_free(merged_order);
  if(!fields) return 0;

  char *part_expr = have_dion ? pvtDistinctPartitionExpr(db, distinct_on, fields) : 0;

  char *push = 0, *resid = 0;
  pvtSplitFilter(db, filter, fields, &push, &resid);
  const char *outer = resid;   /* may be NULL */

  int has_prop = pvtHasPropFields(db, fields);

  /* paging fragment */
  char *paging;
  {
    sqlite3_str *p = sqlite3_str_new(db);
    if(has_limit && limit >= 0) sqlite3_str_appendf(p, "\nLIMIT %d", limit);
    if(offset > 0) sqlite3_str_appendf(p, "\nOFFSET %d", offset);
    paging = sqlite3_str_finish(p);
  }

  char *result = 0;

  /* Shape A: pure-base flat. */
  if(!outer && is_flat && no_tree && !has_prop){
    if(part_expr){
      /* DistinctBy on a base field: ROW_NUMBER() partition (distinct_on field is base here). */
      result = sqlite3_mprintf(
        "WITH _ranked AS (\n  SELECT o._id AS _id, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY o._id) AS _rn"
        "\n  FROM _objects o\n  WHERE o._id_scheme = %lld%s%s\n)"
        "\nSELECT _id FROM _ranked WHERE _rn = 1%s",
        part_expr, (long long)scheme,
        (push && *push) ? " AND " : "", (push && *push) ? push : "",
        paging);
    }else{
      char *order_sql = pvtBuildOrderConditions(db, order, fields, "");
      result = sqlite3_mprintf(
        "SELECT %s_id FROM _objects o\nWHERE o._id_scheme = %lld%s%s%s%s",
        distinct ? "DISTINCT " : "", (long long)scheme,
        (push && *push) ? " AND " : "", (push && *push) ? push : "",
        order_sql, paging);
      sqlite3_free(order_sql);
    }
    goto done;
  }

  {
    int force_outer = pvtHasCheck(db, outer, 1) || !strcmp(fields, "{}") || !has_prop;
    int narrow = !force_outer;   /* has_nested&&has_scalar -> nested stubbed anyway */

    int can_push = narrow && outer && strcmp(outer, "{}") != 0 && !pvtFilterHasBaseRefs(db, outer, fields);
    char *residual_sql = can_push ? pvtBuildWhereFromJson(db, outer, fields, "") : 0;
    if(can_push && !residual_sql){ goto done; }   /* unsupported predicate */

    char *cte = pvtBuildCteSql(db, scheme, fields, source_mode, no_tree ? 0 : tree_ids_json,
                               has_max_depth, max_depth, force_outer, push, narrow,
                               include_seed, polymorphic, residual_sql);
    sqlite3_free(residual_sql);
    if(!cte){ goto done; }

    char *where_sql;
    if(can_push) where_sql = sqlite3_mprintf("TRUE");
    else         where_sql = pvtBuildWhereFromJson(db, outer, fields, narrow ? "o." : "");
    if(!where_sql){ sqlite3_free(cte); goto done; }

    char *order_sql = pvtBuildOrderConditions(db, order, fields, narrow ? "o." : "");

    if(narrow && part_expr){
      /* DistinctBy: chain a _ranked CTE with ROW_NUMBER() and keep rn=1. The distinct_on field is
      ** pivoted into _pvt_cte (prop) or read from o (base). Explicit ORDER BY is not applied on the
      ** id-only outer projection here; the representative per group is the lowest o._id. */
      result = sqlite3_mprintf(
        "%s,\n_ranked AS (\n  SELECT o._id AS _id, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY o._id) AS _rn"
        "\n  FROM _pvt_cte\n  JOIN _objects o ON o._id = _pvt_cte._id_object%s%s\n)"
        "\nSELECT _id FROM _ranked WHERE _rn = 1%s",
        cte, part_expr,
        strcmp(where_sql,"TRUE")==0 ? "" : "\n  WHERE ",
        strcmp(where_sql,"TRUE")==0 ? "" : where_sql,
        paging);
    }else if(narrow){
      result = sqlite3_mprintf(
        "%s\nSELECT %so._id FROM _pvt_cte\nJOIN _objects o ON o._id = _pvt_cte._id_object%s%s%s",
        cte, distinct ? "DISTINCT " : "",
        strcmp(where_sql,"TRUE")==0 ? "" : "\nWHERE ",
        strcmp(where_sql,"TRUE")==0 ? "" : where_sql,
        order_sql);
      char *r2 = sqlite3_mprintf("%s%s", result, paging); sqlite3_free(result); result = r2;
    }else{
      result = sqlite3_mprintf(
        "%s\nSELECT %s_id FROM _pvt_cte%s%s%s%s",
        cte, distinct ? "DISTINCT " : "",
        strcmp(where_sql,"TRUE")==0 ? "" : "\nWHERE ",
        strcmp(where_sql,"TRUE")==0 ? "" : where_sql,
        order_sql, paging);
    }
    sqlite3_free(cte); sqlite3_free(where_sql); sqlite3_free(order_sql);
  }

done:
  sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid); sqlite3_free(paging);
  sqlite3_free(part_expr);
  return result;
}

/* ------------------------------------------------------------------------- */
/* 17_pvt_expr.sql — expression engine                                       */
/* ------------------------------------------------------------------------- */

void pvtLc(char *dst, const char *src, int n){
  int i = 0; for(; src && src[i] && i < n-1; i++){ char c = src[i]; dst[i] = (c>='A'&&c<='Z')?c+32:c; } dst[i] = 0;
}
int pvtJsonArrayLen(sqlite3 *db, const char *container, const char *path){
  sqlite3_stmt *st = 0; int r = -1;
  sqlite3_prepare_v2(db, "SELECT json_array_length(?1,?2)", -1, &st, 0);
  if(st){ sqlite3_bind_text(st,1,container,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,path,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW && sqlite3_column_type(st,0)!=SQLITE_NULL) r = sqlite3_column_int(st,0);
    sqlite3_finalize(st); }
  return r;
}
static char *pvtNodeOpKey(sqlite3 *db, const char *node){
  sqlite3_stmt *st = 0; char *r = 0;
  sqlite3_prepare_v2(db, "SELECT key FROM json_each(?1) WHERE key LIKE '$%' LIMIT 1", -1, &st, 0);
  if(st){ sqlite3_bind_text(st,1,node,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW) r = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st); }
  return r;
}
static const char *pvtCastType(const char *t){
  if(!t) return 0;
  if(!strcmp(t,"text")||!strcmp(t,"varchar")||!strcmp(t,"uuid")||!strcmp(t,"timestamptz")||!strcmp(t,"timestamp")||!strcmp(t,"date")) return "TEXT";
  if(!strcmp(t,"bigint")||!strcmp(t,"integer")||!strcmp(t,"int")||!strcmp(t,"smallint")||!strcmp(t,"boolean")) return "INTEGER";
  if(!strcmp(t,"numeric")||!strcmp(t,"double precision")||!strcmp(t,"real")) return "REAL";
  if(!strcmp(t,"bytea")) return "BLOB";
  return 0;
}
static const char *pvtStrftimeFmt(const char *opl){
  if(!strcmp(opl,"$year"))return "%Y"; if(!strcmp(opl,"$month"))return "%m";
  if(!strcmp(opl,"$day"))return "%d";  if(!strcmp(opl,"$hour"))return "%H";
  if(!strcmp(opl,"$minute"))return "%M"; if(!strcmp(opl,"$second"))return "%S";
  if(!strcmp(opl,"$dayofweek"))return "%w"; if(!strcmp(opl,"$dayofyear"))return "%j";
  return 0;
}

/* Compile the node value at container+path into a SQL scalar fragment. */
char *pvtScalarNode(sqlite3 *db, const char *container, const char *path,
                    const char *fields, const char *prefix){
  char *ty = jsonTypeAt(db, container, path);
  if(!ty) return 0;
  char *res = 0;
  if(!strcmp(ty,"object") || !strcmp(ty,"array")){
    char *sub = jsonGetText(db, container, path);
    if(sub){ res = pvtBuildScalarExpr(db, sub, fields, prefix); sqlite3_free(sub); }
  }else if(!strcmp(ty,"text")){
    char *t = jsonGetText(db, container, path); res = sqlite3_mprintf("%Q", t?t:""); sqlite3_free(t);
  }else if(!strcmp(ty,"integer") || !strcmp(ty,"real")){
    char *t = jsonGetText(db, container, path); res = sqlite3_mprintf("%s", t?t:"0"); sqlite3_free(t);
  }else if(!strcmp(ty,"true"))  res = sqlite3_mprintf("1");
  else if(!strcmp(ty,"false")) res = sqlite3_mprintf("0");
  else res = sqlite3_mprintf("NULL");
  sqlite3_free(ty);
  return res;
}
/* operand i of an op (array form) or the whole operand (non-array form). */
static char *pvtArg(sqlite3 *db, const char *node, const char *argpath, int is_arr,
                    int i, const char *fields, const char *prefix){
  if(is_arr){ char *p = sqlite3_mprintf("%s[%d]", argpath, i); char *r = pvtScalarNode(db, node, p, fields, prefix); sqlite3_free(p); return r; }
  return pvtScalarNode(db, node, argpath, fields, prefix);
}

char *pvtBuildScalarExpr(sqlite3 *db, const char *node, const char *fields, const char *prefix){
  if(!prefix) prefix = "";
  char *ty = jsonTypeAt(db, node, "$");
  if(!ty) return 0;
  if(strcmp(ty,"object") != 0){ sqlite3_free(ty); return pvtScalarNode(db, node, "$", fields, prefix); }
  sqlite3_free(ty);

  /* $field */
  char *ft = jsonTypeAt(db, node, "$.\"$field\"");
  if(ft){ sqlite3_free(ft);
    char *name = jsonGetText(db, node, "$.\"$field\"");
    char *mp = sqlite3_mprintf("$.\"%w\"", name);
    char *meta = jsonGetText(db, fields, mp); sqlite3_free(mp);
    char *res = 0;
    if(meta){ char *kind = jsonGetText(db, meta, "$.kind"); char *col = jsonGetText(db, meta, "$.column");
      if(kind && !strcmp(kind,"base")) res = sqlite3_mprintf("%s%s", prefix, col?col:"_id");
      else res = sqlite3_mprintf("\"%w\"", name ? name : "");
      sqlite3_free(kind); sqlite3_free(col); }
    sqlite3_free(name); sqlite3_free(meta);
    return res;
  }
  /* $const */
  char *cnst = jsonTypeAt(db, node, "$.\"$const\"");
  if(cnst){ sqlite3_free(cnst); return pvtScalarNode(db, node, "$.\"$const\"", fields, prefix); }

  char *op = pvtNodeOpKey(db, node);
  if(!op) return 0;
  char opl[24]; pvtLc(opl, op, sizeof opl);
  char *argpath = sqlite3_mprintf("$.\"%w\"", op);
  char *argty = jsonTypeAt(db, node, argpath);
  int is_arr = argty && !strcmp(argty, "array");
  int alen = is_arr ? pvtJsonArrayLen(db, node, argpath) : -1;
  sqlite3_free(argty);
  char *res = 0;

  /* Aggregate passthrough: $count/$sum/$avg/$string_agg/$bool_and/$bool_or, and
     $min/$max with a non-array operand, delegate to the aggregate compiler
     (HAVING / ORDER BY / nested-scalar agg contexts). */
  if(!strcmp(opl,"$count")||!strcmp(opl,"$sum")||!strcmp(opl,"$avg")||
     !strcmp(opl,"$string_agg")||!strcmp(opl,"$bool_and")||!strcmp(opl,"$bool_or")||
     ((!strcmp(opl,"$min")||!strcmp(opl,"$max")) && !is_arr)){
    res = pvtBuildAggExpr(db, node, fields, prefix);
    sqlite3_free(op); sqlite3_free(argpath);
    return res;
  }

  /* Window passthrough: {"$over": <window-node>} -> pvtBuildWindowExpr. */
  if(!strcmp(opl,"$over")){
    char *win = jsonGetText(db, node, argpath);
    res = win ? pvtBuildWindowExpr(db, win, fields, prefix) : 0;
    sqlite3_free(win); sqlite3_free(op); sqlite3_free(argpath);
    return res;
  }

  /* n-ary infix / functions */
  if(!strcmp(opl,"$add")||!strcmp(opl,"$mul")||!strcmp(opl,"$concat")||
     !strcmp(opl,"$min")||!strcmp(opl,"$max")||!strcmp(opl,"$coalesce")){
    if(is_arr && alen >= 1){
      sqlite3_str *b = sqlite3_str_new(db); int ok = 1;
      const char *fn = !strcmp(opl,"$min")?"min(":!strcmp(opl,"$max")?"max(":!strcmp(opl,"$coalesce")?"COALESCE(":"(";
      const char *sep = !strcmp(opl,"$add")?" + ":!strcmp(opl,"$mul")?" * ":!strcmp(opl,"$concat")?" || ":", ";
      sqlite3_str_appendf(b, "%s", fn);
      for(int i = 0; i < alen; i++){ char *a = pvtArg(db,node,argpath,1,i,fields,prefix); if(!a){ok=0;break;} if(i) sqlite3_str_appendf(b,"%s",sep); sqlite3_str_appendf(b,"%s",a); sqlite3_free(a); }
      sqlite3_str_append(b, ")", 1);
      if(ok) res = sqlite3_str_finish(b); else sqlite3_free(sqlite3_str_finish(b));
    }
  }
  else if(!strcmp(opl,"$sub")||!strcmp(opl,"$div")||!strcmp(opl,"$mod")||!strcmp(opl,"$power")||!strcmp(opl,"$log")||!strcmp(opl,"$indexof")){
    if(is_arr && alen == 2){
      char *a = pvtArg(db,node,argpath,1,0,fields,prefix), *bb = pvtArg(db,node,argpath,1,1,fields,prefix);
      if(a && bb){
        if(!strcmp(opl,"$sub")) res = sqlite3_mprintf("(%s - %s)", a, bb);
        else if(!strcmp(opl,"$div")) res = sqlite3_mprintf("(%s / %s)", a, bb);
        else if(!strcmp(opl,"$mod")) res = sqlite3_mprintf("(%s %% %s)", a, bb);
        else if(!strcmp(opl,"$power")) res = sqlite3_mprintf("POWER(%s, %s)", a, bb);
        else if(!strcmp(opl,"$log")) res = sqlite3_mprintf("LOG(%s, %s)", a, bb);
        else /* $indexof: POSITION(needle IN str) -> INSTR(str,needle) */ res = sqlite3_mprintf("INSTR(%s, %s)", a, bb);
      }
      sqlite3_free(a); sqlite3_free(bb);
    }
  }
  else if(!strcmp(opl,"$neg")||!strcmp(opl,"$abs")||!strcmp(opl,"$floor")||!strcmp(opl,"$ceil")||
          !strcmp(opl,"$upper")||!strcmp(opl,"$lower")||!strcmp(opl,"$trim")||!strcmp(opl,"$trimstart")||
          !strcmp(opl,"$trimend")||!strcmp(opl,"$length")||!strcmp(opl,"$sqrt")||!strcmp(opl,"$sign")||
          !strcmp(opl,"$exp")||!strcmp(opl,"$ln")||!strcmp(opl,"$sin")||!strcmp(opl,"$cos")||!strcmp(opl,"$tan")||
          !strcmp(opl,"$asin")||!strcmp(opl,"$acos")||!strcmp(opl,"$atan")||!strcmp(opl,"$log10")){
    char *a = is_arr ? pvtArg(db,node,argpath,1,0,fields,prefix) : pvtArg(db,node,argpath,0,0,fields,prefix);
    if(a){
      if(!strcmp(opl,"$neg")) res = sqlite3_mprintf("(-%s)", a);
      else if(!strcmp(opl,"$trimstart")) res = sqlite3_mprintf("LTRIM(%s)", a);
      else if(!strcmp(opl,"$trimend")) res = sqlite3_mprintf("RTRIM(%s)", a);
      else if(!strcmp(opl,"$log10")) res = sqlite3_mprintf("LOG(%s)", a);
      else{ const char *fn = opl+1; res = sqlite3_mprintf("%s(%s)", /* abs/floor/ceil/upper/lower/trim/length/sqrt/sign/exp/ln/sin/... */ fn, a); }
    }
    sqlite3_free(a);
  }
  else if(pvtStrftimeFmt(opl)){
    char *a = is_arr ? pvtArg(db,node,argpath,1,0,fields,prefix) : pvtArg(db,node,argpath,0,0,fields,prefix);
    if(a) res = sqlite3_mprintf("CAST(strftime('%s', %s) AS INTEGER)", pvtStrftimeFmt(opl), a);
    sqlite3_free(a);
  }
  else if(!strcmp(opl,"$round")){
    if(is_arr && alen == 1){ char *a = pvtArg(db,node,argpath,1,0,fields,prefix); if(a) res = sqlite3_mprintf("ROUND(%s)", a); sqlite3_free(a); }
    else if(is_arr && alen == 2){ char *a = pvtArg(db,node,argpath,1,0,fields,prefix), *d = pvtArg(db,node,argpath,1,1,fields,prefix); if(a&&d) res = sqlite3_mprintf("ROUND(%s, %s)", a, d); sqlite3_free(a); sqlite3_free(d); }
  }
  else if(!strcmp(opl,"$cast")){
    if(is_arr && alen == 2){
      char *tname = jsonGetText(db, node, /* argpath[0] */ 0);
      char *tp = sqlite3_mprintf("%s[0]", argpath); sqlite3_free(tname); tname = jsonGetText(db, node, tp); sqlite3_free(tp);
      const char *st = pvtCastType(tname);
      char *a = pvtArg(db,node,argpath,1,1,fields,prefix);
      if(st && a) res = sqlite3_mprintf("CAST(%s AS %s)", a, st);
      sqlite3_free(tname); sqlite3_free(a);
    }
  }
  else if(!strcmp(opl,"$substring")){
    if(is_arr && (alen==2||alen==3)){
      char *a = pvtArg(db,node,argpath,1,0,fields,prefix), *s = pvtArg(db,node,argpath,1,1,fields,prefix);
      if(a && s){ if(alen==2) res = sqlite3_mprintf("SUBSTR(%s, %s)", a, s);
        else{ char *l = pvtArg(db,node,argpath,1,2,fields,prefix); if(l) res = sqlite3_mprintf("SUBSTR(%s, %s, %s)", a, s, l); sqlite3_free(l); } }
      sqlite3_free(a); sqlite3_free(s);
    }
  }
  else if(!strcmp(opl,"$replace")){
    if(is_arr && alen==3){ char *a=pvtArg(db,node,argpath,1,0,fields,prefix),*f=pvtArg(db,node,argpath,1,1,fields,prefix),*r=pvtArg(db,node,argpath,1,2,fields,prefix);
      if(a&&f&&r) res = sqlite3_mprintf("REPLACE(%s, %s, %s)", a, f, r); sqlite3_free(a);sqlite3_free(f);sqlite3_free(r); }
  }
  else if(!strcmp(opl,"$now")||!strcmp(opl,"$utcnow")){ res = sqlite3_mprintf("datetime('now')"); }
  else if(!strcmp(opl,"$today")){ res = sqlite3_mprintf("date('now')"); }
  else if(!strcmp(opl,"$if")){
    if(is_arr && alen==3){
      char *cp = sqlite3_mprintf("%s[0]", argpath); char *condsub = jsonGetText(db,node,cp); sqlite3_free(cp);
      char *cond = condsub ? pvtBuildBoolExpr(db, condsub, fields, prefix) : 0; sqlite3_free(condsub);
      char *t = pvtArg(db,node,argpath,1,1,fields,prefix), *e = pvtArg(db,node,argpath,1,2,fields,prefix);
      if(cond && t && e) res = sqlite3_mprintf("(CASE WHEN %s THEN %s ELSE %s END)", cond, t, e);
      sqlite3_free(cond); sqlite3_free(t); sqlite3_free(e);
    }
  }
  else if(!strcmp(opl,"$case")){
    /* [{when,then},...,{else}] -> (CASE WHEN .. THEN .. ELSE .. END) */
    if(is_arr && alen >= 1){
      sqlite3_str *cb = sqlite3_str_new(db); sqlite3_str_append(cb, "(CASE", 5);
      char *elseval = 0; int ok2 = 1;
      for(int i = 0; i < alen && ok2; i++){
        char *ep = sqlite3_mprintf("%s[%d]", argpath, i); char *e = jsonGetText(db, node, ep); sqlite3_free(ep);
        if(!e){ ok2 = 0; break; }
        char *elt = jsonTypeAt(db, e, "$.else");
        if(elt){ sqlite3_free(elt); char *ev = jsonGetText(db, e, "$.else"); sqlite3_free(elseval); elseval = ev ? pvtBuildScalarExpr(db, ev, fields, prefix) : 0; sqlite3_free(ev); if(!elseval) ok2 = 0; }
        else{ char *w = jsonGetText(db, e, "$.when"); char *t = jsonGetText(db, e, "$.then");
          char *wb2 = w ? pvtBuildBoolExpr(db, w, fields, prefix) : 0; char *tb = t ? pvtBuildScalarExpr(db, t, fields, prefix) : 0;
          if(wb2 && tb) sqlite3_str_appendf(cb, " WHEN %s THEN %s", wb2, tb); else ok2 = 0;
          sqlite3_free(w); sqlite3_free(t); sqlite3_free(wb2); sqlite3_free(tb); }
        sqlite3_free(e);
      }
      sqlite3_str_appendf(cb, " ELSE %s END)", elseval ? elseval : "NULL");
      sqlite3_free(elseval);
      if(ok2) res = sqlite3_str_finish(cb); else sqlite3_free(sqlite3_str_finish(cb));
    }
  }
  else if(!strcmp(opl,"$datetrunc")){
    /* ["unit", expr] -> strftime('<fmt>', expr) (SQLite has no DATE_TRUNC). */
    if(is_arr && alen == 2){
      char *up = sqlite3_mprintf("%s[0]", argpath); char *unit = jsonGetText(db, node, up); sqlite3_free(up);
      char low[16]; pvtLc(low, unit ? unit : "", sizeof low);
      char *a = pvtArg(db, node, argpath, 1, 1, fields, prefix);
      const char *fmt = !strcmp(low,"year")?"%Y-01-01":!strcmp(low,"month")?"%Y-%m-01":!strcmp(low,"day")?"%Y-%m-%d":
                        !strcmp(low,"hour")?"%Y-%m-%d %H:00:00":!strcmp(low,"minute")?"%Y-%m-%d %H:%M:00":
                        !strcmp(low,"second")?"%Y-%m-%d %H:%M:%S":0;
      if(a && fmt) res = sqlite3_mprintf("strftime('%s', %s)", fmt, a);
      sqlite3_free(unit); sqlite3_free(a);
    }
  }
  else if(!strcmp(opl,"$padleft")||!strcmp(opl,"$padright")){
    /* [str, len, padChar?] -> printf('%*s'/'%-*s', len, str) (space pad). */
    if(is_arr && (alen == 2 || alen == 3)){
      char *a = pvtArg(db, node, argpath, 1, 0, fields, prefix);
      char *l = pvtArg(db, node, argpath, 1, 1, fields, prefix);
      if(a && l) res = sqlite3_mprintf("printf('%%%s*s', %s, %s)", !strcmp(opl,"$padleft") ? "" : "-", l, a);
      sqlite3_free(a); sqlite3_free(l);
    }
  }
  else if(!strcmp(opl,"$dateadd")||!strcmp(opl,"$datesub")){
    /* ["unit", date_expr, amount] -> datetime(date, (±amount)||' <unit>s')
       (SQLite analog of MSSql DATEADD / PG INTERVAL). */
    if(is_arr && alen == 3){
      char *up = sqlite3_mprintf("%s[0]", argpath); char *unit = jsonGetText(db, node, up); sqlite3_free(up);
      char low[16]; pvtLc(low, unit ? unit : "", sizeof low);
      char *dexpr = pvtArg(db, node, argpath, 1, 1, fields, prefix);
      char *amt   = pvtArg(db, node, argpath, 1, 2, fields, prefix);
      /* SQLite modifier units (no 'weeks' -> map to days*7). */
      const char *u = !strcmp(low,"year")?"years":!strcmp(low,"month")?"months":!strcmp(low,"day")?"days":
                      !strcmp(low,"hour")?"hours":!strcmp(low,"minute")?"minutes":!strcmp(low,"second")?"seconds":
                      !strcmp(low,"week")?"days":!strcmp(low,"millisecond")?"seconds":0;
      if(dexpr && amt && u){
        int sub = !strcmp(opl,"$datesub");
        const char *mul = !strcmp(low,"week") ? "*7" : "";
        res = sqlite3_mprintf("datetime(%s, (%s(%s)%s) || ' %s')", dexpr, sub?"-":"", amt, mul, u);
      }
      sqlite3_free(unit); sqlite3_free(dexpr); sqlite3_free(amt);
    }
  }
  else if(!strcmp(opl,"$datediff")){
    /* ["unit", a, b] -> count of <unit> from b to a (julianday / strftime). */
    if(is_arr && alen == 3){
      char *up = sqlite3_mprintf("%s[0]", argpath); char *unit = jsonGetText(db, node, up); sqlite3_free(up);
      char low[16]; pvtLc(low, unit ? unit : "", sizeof low);
      char *a = pvtArg(db, node, argpath, 1, 1, fields, prefix);
      char *b = pvtArg(db, node, argpath, 1, 2, fields, prefix);
      if(a && b){
        if(!strcmp(low,"day"))         res = sqlite3_mprintf("CAST(julianday(%s) - julianday(%s) AS INTEGER)", a, b);
        else if(!strcmp(low,"week"))   res = sqlite3_mprintf("CAST((julianday(%s) - julianday(%s)) / 7 AS INTEGER)", a, b);
        else if(!strcmp(low,"hour"))   res = sqlite3_mprintf("CAST((julianday(%s) - julianday(%s)) * 24 AS INTEGER)", a, b);
        else if(!strcmp(low,"minute")) res = sqlite3_mprintf("CAST((julianday(%s) - julianday(%s)) * 1440 AS INTEGER)", a, b);
        else if(!strcmp(low,"second")) res = sqlite3_mprintf("CAST((julianday(%s) - julianday(%s)) * 86400 AS INTEGER)", a, b);
        else if(!strcmp(low,"millisecond")) res = sqlite3_mprintf("CAST((julianday(%s) - julianday(%s)) * 86400000 AS INTEGER)", a, b);
        else if(!strcmp(low,"month"))  res = sqlite3_mprintf("((CAST(strftime('%%Y',%s) AS INTEGER)*12 + CAST(strftime('%%m',%s) AS INTEGER)) - (CAST(strftime('%%Y',%s) AS INTEGER)*12 + CAST(strftime('%%m',%s) AS INTEGER)))", a, a, b, b);
        else if(!strcmp(low,"year"))   res = sqlite3_mprintf("(CAST(strftime('%%Y',%s) AS INTEGER) - CAST(strftime('%%Y',%s) AS INTEGER))", a, b);
      }
      sqlite3_free(unit); sqlite3_free(a); sqlite3_free(b);
    }
  }
  /* $regexReplace / $regex / $iregex / $fts: PG-only (regex engine SQLite lacks);
     MSSql v2-pvt skips these too -> legitimately unsupported on this backend. */

  sqlite3_free(op); sqlite3_free(argpath);
  return res;
}

static char *pvtBuildExprPredicate(sqlite3 *db, const char *op, const char *args,
                                   const char *fields, const char *prefix){
  if(!prefix) prefix = "";
  char opl[24]; pvtLc(opl, op, sizeof opl);

  if(!strcmp(opl,"$null")||!strcmp(opl,"$isnull")||!strcmp(opl,"$notnull")||!strcmp(opl,"$exists")){
    /* Operand is the array form [field] (FacetFilterBuilder emits {"$null":[field]}, like the other
    ** operator-form predicates). Unwrap element 0 the way $between/$in do; tolerate a bare node too. */
    char *aty = jsonTypeAt(db, args, "$");
    char *l = (aty && !strcmp(aty, "array")) ? pvtScalarNode(db, args, "$[0]", fields, prefix)
                                             : pvtBuildScalarExpr(db, args, fields, prefix);
    sqlite3_free(aty);
    if(!l) return 0;
    char *r = sqlite3_mprintf("%s IS %sNULL", l, (!strcmp(opl,"$null")||!strcmp(opl,"$isnull")) ? "" : "NOT ");
    sqlite3_free(l); return r;
  }
  if(!strcmp(opl,"$between")){
    if(pvtJsonArrayLen(db,args,"$")!=3) return 0;
    char *l=pvtScalarNode(db,args,"$[0]",fields,prefix),*lo=pvtScalarNode(db,args,"$[1]",fields,prefix),*hi=pvtScalarNode(db,args,"$[2]",fields,prefix);
    char *r=0; if(l&&lo&&hi) r=sqlite3_mprintf("(%s BETWEEN %s AND %s)",l,lo,hi); sqlite3_free(l);sqlite3_free(lo);sqlite3_free(hi); return r;
  }
  if(!strcmp(opl,"$in")||!strcmp(opl,"$nin")){
    if(pvtJsonArrayLen(db,args,"$")!=2) return 0;
    char *l=pvtScalarNode(db,args,"$[0]",fields,prefix);
    char *listpath="$[1]";
    int n=pvtJsonArrayLen(db,args,listpath);
    if(!l){ return 0; }
    if(n==0){ sqlite3_free(l); return sqlite3_mprintf("%s", !strcmp(opl,"$in")?"FALSE":"TRUE"); }
    sqlite3_str *b=sqlite3_str_new(db); sqlite3_str_appendf(b,"(%s %s (", l, !strcmp(opl,"$in")?"IN":"NOT IN");
    for(int i=0;i<n;i++){ char *p=sqlite3_mprintf("$[1][%d]",i); char *v=pvtScalarNode(db,args,p,fields,prefix); sqlite3_free(p); if(i)sqlite3_str_append(b,", ",2); sqlite3_str_appendf(b,"%s",v?v:"NULL"); sqlite3_free(v); }
    sqlite3_str_append(b,"))",2); sqlite3_free(l); return sqlite3_str_finish(b);
  }
  /* contains/startsWith/endsWith[IgnoreCase] sugar over LIKE */
  if(!strcmp(opl,"$contains")||!strcmp(opl,"$startswith")||!strcmp(opl,"$endswith")||
     !strcmp(opl,"$containsignorecase")||!strcmp(opl,"$startswithignorecase")||!strcmp(opl,"$endswithignorecase")){
    char *l=pvtScalarNode(db,args,"$[0]",fields,prefix);
    /* RHS literal: $const-wrapped or bare string */
    char *pat=jsonGetText(db,args,"$[1].\"$const\""); if(!pat) pat=jsonGetText(db,args,"$[1]");
    char *r=0;
    if(l && pat){
      int lead = !strcmp(opl,"$contains")||!strcmp(opl,"$containsignorecase")||!strcmp(opl,"$endswith")||!strcmp(opl,"$endswithignorecase");
      int trail= !strcmp(opl,"$contains")||!strcmp(opl,"$containsignorecase")||!strcmp(opl,"$startswith")||!strcmp(opl,"$startswithignorecase");
      char *p=sqlite3_mprintf("%s%s%s", lead?"%":"", pat, trail?"%":"");
      r=sqlite3_mprintf("(%s LIKE %Q)", l, p); sqlite3_free(p);
    }
    sqlite3_free(l); sqlite3_free(pat); return r;
  }
  if(!strcmp(opl,"$regex")||!strcmp(opl,"$iregex")||!strcmp(opl,"$notregex")||!strcmp(opl,"$inotregex")||!strcmp(opl,"$fts")) return 0; /* TODO */

  /* binary infix */
  if(pvtJsonArrayLen(db,args,"$")!=2) return 0;
  char *l=pvtScalarNode(db,args,"$[0]",fields,prefix),*r=pvtScalarNode(db,args,"$[1]",fields,prefix);
  char *res=0;
  if(l&&r){
    const char *sym = !strcmp(opl,"$eq")?"=":!strcmp(opl,"$ne")?"<>":!strcmp(opl,"$lt")?"<":!strcmp(opl,"$lte")?"<=":
                      !strcmp(opl,"$gt")?">":!strcmp(opl,"$gte")?">=":!strcmp(opl,"$like")?"LIKE":!strcmp(opl,"$ilike")?"LIKE":0;
    if(sym) res=sqlite3_mprintf("(%s %s %s)", l, sym, r);
  }
  sqlite3_free(l); sqlite3_free(r); return res;
}

char *pvtBuildBoolExpr(sqlite3 *db, const char *node, const char *fields, const char *prefix){
  if(!prefix) prefix = "";
  char *ty = jsonTypeAt(db, node, "$");
  if(!ty) return sqlite3_mprintf("TRUE");
  if(!strcmp(ty,"true")){ sqlite3_free(ty); return sqlite3_mprintf("TRUE"); }
  if(!strcmp(ty,"false")){ sqlite3_free(ty); return sqlite3_mprintf("FALSE"); }
  if(strcmp(ty,"object")!=0){ sqlite3_free(ty); return 0; }
  sqlite3_free(ty);
  char *op = pvtNodeOpKey(db, node);
  if(!op) return 0;
  char opl[16]; pvtLc(opl, op, sizeof opl);
  char *argpath = sqlite3_mprintf("$.\"%w\"", op);
  char *res = 0;
  if(!strcmp(opl,"$and")||!strcmp(opl,"$or")){
    char *arr = jsonGetText(db, node, argpath);
    int n = arr ? pvtJsonArrayLen(db, arr, "$") : -1;
    if(n >= 1){
      sqlite3_str *b = sqlite3_str_new(db); int ok = 1; sqlite3_str_append(b,"(",1);
      for(int i = 0; i < n; i++){ char *p=sqlite3_mprintf("$[%d]",i); char *e=jsonGetText(db,arr,p); sqlite3_free(p);
        char *c = e ? pvtBuildBoolExpr(db,e,fields,prefix) : 0; sqlite3_free(e);
        if(!c){ ok=0; break; } if(i) sqlite3_str_appendf(b," %s ", !strcmp(opl,"$and")?"AND":"OR"); sqlite3_str_appendf(b,"%s",c); sqlite3_free(c); }
      sqlite3_str_append(b,")",1);
      if(ok) res = sqlite3_str_finish(b); else sqlite3_free(sqlite3_str_finish(b));
    }
    sqlite3_free(arr);
  }else if(!strcmp(opl,"$not")){
    char *sub = jsonGetText(db, node, argpath);
    char *c = sub ? pvtBuildBoolExpr(db, sub, fields, prefix) : 0; sqlite3_free(sub);
    if(c){ res = sqlite3_mprintf("NOT (%s)", c); sqlite3_free(c); }
  }else{
    char *args = jsonGetText(db, node, argpath);
    if(args){ res = pvtBuildExprPredicate(db, op, args, fields, prefix); sqlite3_free(args); }
  }
  sqlite3_free(op); sqlite3_free(argpath);
  return res;
}

/* every $field inside resolves to kind=base ? */
static int pvtExprIsBaseOnly(sqlite3 *db, const char *node, const char *fields){
  if(!node) return 1;
  char *ty = jsonTypeAt(db, node, "$");
  if(!ty) return 1;
  int base_only = 1;
  if(!strcmp(ty,"array")){
    int n = pvtJsonArrayLen(db, node, "$");
    for(int i = 0; base_only && i < n; i++){ char *p=sqlite3_mprintf("$[%d]",i); char *e=jsonGetText(db,node,p); sqlite3_free(p); if(e && !pvtExprIsBaseOnly(db,e,fields)) base_only=0; sqlite3_free(e); }
  }else if(!strcmp(ty,"object")){
    char *ft = jsonTypeAt(db, node, "$.\"$field\"");
    if(ft){ sqlite3_free(ft);
      char *name = jsonGetText(db, node, "$.\"$field\"");
      char *mp = sqlite3_mprintf("$.\"%w\"", name); char *meta = jsonGetText(db, fields, mp); sqlite3_free(mp);
      char *kind = meta ? jsonGetText(db, meta, "$.kind") : 0;
      if(!meta || !kind || strcmp(kind,"base")!=0) base_only = 0;
      sqlite3_free(name); sqlite3_free(meta); sqlite3_free(kind);
    }else{
      char *cnst = jsonTypeAt(db, node, "$.\"$const\"");
      if(cnst){ sqlite3_free(cnst); }
      else{
        sqlite3_stmt *it = 0; sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &it, 0);
        sqlite3_bind_text(it,1,node,-1,SQLITE_TRANSIENT);
        while(base_only && sqlite3_step(it)==SQLITE_ROW){ char *v=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0)); if(!pvtExprIsBaseOnly(db,v,fields)) base_only=0; sqlite3_free(v); }
        sqlite3_finalize(it);
      }
    }
  }
  sqlite3_free(ty);
  return base_only;
}

/* ------------------------------------------------------------------------- */
/* 19/21/22 — aggregate, group-by                                            */
/* ------------------------------------------------------------------------- */

/* fields[name] kind == base ? (helper) */
static int pvtFieldIsBase(sqlite3 *db, const char *fields, const char *name){
  char *mp = sqlite3_mprintf("$.\"%w\"", name);
  char *meta = jsonGetText(db, fields, mp); sqlite3_free(mp);
  int base = 0; if(meta){ char *k = jsonGetText(db, meta, "$.kind"); base = k && !strcmp(k,"base"); sqlite3_free(k); }
  sqlite3_free(meta); return base;
}
int pvtHasNestedDict(sqlite3 *db, const char *fields){
  int has = 0; sqlite3_stmt *it = 0;
  sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it,1,fields,-1,SQLITE_TRANSIENT);
  while(!has && sqlite3_step(it)==SQLITE_ROW){ char *m=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0));
    char *dk=jsonGetText(db,m,"$.dict_key"); char *ps=jsonGetText(db,m,"$.parent_sid"); if(dk&&ps) has=1; sqlite3_free(dk);sqlite3_free(ps);sqlite3_free(m); }
  sqlite3_finalize(it); return has;
}

static void pvtWalkWindow(PvtCollect *c, const char *win);

/* harvest order/group entries (field|field_path|$expr) + aggs + having into ctx. */
static void pvtWalkOrderEntries(PvtCollect *c, const char *arr){
  if(!arr) return;
  char *t = jsonTypeAt(c->db, arr, "$"); int isarr = t && !strcmp(t,"array"); sqlite3_free(t);
  if(!isarr) return;
  sqlite3_stmt *it = 0; sqlite3_prepare_v2(c->db, "SELECT value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it,1,arr,-1,SQLITE_TRANSIENT);
  while(!c->err && sqlite3_step(it)==SQLITE_ROW){
    char *e = sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0));
    char *f = jsonGetText(c->db,e,"$.field"); if(!f) f = jsonGetText(c->db,e,"$.field_path");
    if(f){ if(!pvtSeen(c,f)) pvtEmitEntry(c,f,pvtResolveFieldPath(c->db,c->scheme,f)); }
    else { char *ex=jsonGetText(c->db,e,"$.\"$expr\"");
      if(ex){ char *ov=jsonTypeAt(c->db,ex,"$.\"$over\"");
        if(ov){ sqlite3_free(ov); char *win=jsonGetText(c->db,ex,"$.\"$over\""); if(win) pvtWalkWindow(c,win); sqlite3_free(win); }
        else pvtWalkFilter(c,ex); }
      sqlite3_free(ex); }
    sqlite3_free(f); sqlite3_free(e);
  }
  sqlite3_finalize(it);
}

/* harvest field refs from a window node (args + partition_by + order_by). */
static void pvtWalkWindow(PvtCollect *c, const char *win){
  if(c->err || !win) return;
  char *args = jsonGetText(c->db, win, "$.args");
  if(args){
    sqlite3_stmt *ai = 0; sqlite3_prepare_v2(c->db,"SELECT value FROM json_each(?1)",-1,&ai,0);
    sqlite3_bind_text(ai,1,args,-1,SQLITE_TRANSIENT);
    while(!c->err && sqlite3_step(ai)==SQLITE_ROW){ char *el=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(ai,0)); pvtWalkFilter(c,el); sqlite3_free(el); }
    sqlite3_finalize(ai); sqlite3_free(args);
  }
  char *pb = jsonGetText(c->db, win, "$.partition_by"); if(pb){ pvtWalkOrderEntries(c,pb); sqlite3_free(pb); }
  char *ob = jsonGetText(c->db, win, "$.order_by");     if(ob){ pvtWalkOrderEntries(c,ob); sqlite3_free(ob); }
}

static void pvtWalkAggs(PvtCollect *c, const char *aggs){
  if(!aggs) return;
  char *t = jsonTypeAt(c->db, aggs, "$"); int isarr = t && !strcmp(t,"array"); sqlite3_free(t);
  if(!isarr) return;
  sqlite3_stmt *it = 0; sqlite3_prepare_v2(c->db, "SELECT value FROM json_each(?1)", -1, &it, 0);
  sqlite3_bind_text(it,1,aggs,-1,SQLITE_TRANSIENT);
  while(!c->err && sqlite3_step(it)==SQLITE_ROW){
    char *e = sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0));
    sqlite3_stmt *ki = 0; sqlite3_prepare_v2(c->db,"SELECT key, value, type FROM json_each(?1)",-1,&ki,0);
    sqlite3_bind_text(ki,1,e,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(ki)==SQLITE_ROW){
      const char *k=(const char*)sqlite3_column_text(ki,0); const char *ty=(const char*)sqlite3_column_text(ki,2);
      char *v=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(ki,1));
      if((k && k[0]=='$') || (k && !strcmp(k,"filter"))){
        if(ty && !strcmp(ty,"array")){
          /* array operand (e.g. $string_agg [value, sep]): harvest each element. */
          sqlite3_stmt *ei2 = 0; sqlite3_prepare_v2(c->db,"SELECT value FROM json_each(?1)",-1,&ei2,0);
          sqlite3_bind_text(ei2,1,v,-1,SQLITE_TRANSIENT);
          while(sqlite3_step(ei2)==SQLITE_ROW){ char *el=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(ei2,0)); pvtWalkFilter(c,el); sqlite3_free(el); }
          sqlite3_finalize(ei2);
        }
        else if(!(ty && !strcmp(ty,"text"))) pvtWalkFilter(c, v);   /* skip "*" string operand */
      }
      sqlite3_free(v);
    }
    sqlite3_finalize(ki); sqlite3_free(e);
  }
  sqlite3_finalize(it);
}

/* general field collection across filter+order+group+aggs+having. */
char *pvtCollectAll(sqlite3 *db, sqlite3_int64 scheme, const char *filter,
                    const char *order, const char *group_by, const char *aggs,
                    const char *having){
  PvtCollect c; memset(&c,0,sizeof(c)); c.db=db; c.scheme=scheme; c.body=sqlite3_str_new(db);
  if(filter) pvtWalkFilter(&c, filter);
  pvtWalkOrderEntries(&c, group_by);
  pvtWalkOrderEntries(&c, order);
  pvtWalkAggs(&c, aggs);
  if(having) pvtWalkFilter(&c, having);
  char *res=0; char *body=sqlite3_str_finish(c.body);
  if(!c.err) res=sqlite3_mprintf("{%s}", body?body:"");
  sqlite3_free(body);
  for(int i=0;i<c.nseen;i++) sqlite3_free(c.seen[i]); sqlite3_free(c.seen);
  return res;
}

/* compile one aggregate entry -> SQL fragment (no alias). NULL on unsupported. */
static char *pvtBuildAggExpr(sqlite3 *db, const char *entry, const char *fields, const char *prefix){
  if(!prefix) prefix = "";
  /* find single $-func key + operand */
  char *func = 0, *operand = 0, *optype = 0;
  sqlite3_stmt *it = 0; sqlite3_prepare_v2(db,"SELECT key, value, type FROM json_each(?1) WHERE key LIKE '$%' LIMIT 1",-1,&it,0);
  sqlite3_bind_text(it,1,entry,-1,SQLITE_TRANSIENT);
  if(sqlite3_step(it)==SQLITE_ROW){ func=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0)); operand=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,1)); optype=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,2)); }
  sqlite3_finalize(it);
  if(!func) return 0;
  char fl[20]; pvtLc(fl, func, sizeof fl);
  int distinct = jsonGetBool(db, entry, "$.distinct");
  /* FILTER */
  char *filt = sqlite3_mprintf("");
  { char *ft = jsonTypeAt(db, entry, "$.filter");
    if(ft){ sqlite3_free(ft); char *fn=jsonGetText(db,entry,"$.filter"); char *be=fn?pvtBuildBoolExpr(db,fn,fields,prefix):0; sqlite3_free(fn);
      if(be){ sqlite3_free(filt); filt=sqlite3_mprintf(" FILTER (WHERE %s)", be); sqlite3_free(be); } } }

  /* array-field operand detection */
  int is_arr_field = 0;
  if(optype && !strcmp(optype,"object")){
    char *ff = jsonTypeAt(db, operand, "$.\"$field\"");
    if(ff){ sqlite3_free(ff); char *nm=jsonGetText(db,operand,"$.\"$field\"");
      char *mp=sqlite3_mprintf("$.\"%w\"",nm); char *meta=jsonGetText(db,fields,mp); sqlite3_free(mp);
      if(meta){ int ia=jsonGetBool(db,meta,"$.is_array"); char *k=jsonGetText(db,meta,"$.kind"); if(ia && !(k&&!strcmp(k,"base"))) is_arr_field=1; sqlite3_free(k); }
      sqlite3_free(nm); sqlite3_free(meta); } }

  char *res = 0;
  if(!strcmp(fl,"$count")){
    if(optype && !strcmp(optype,"text")){ /* "*" */
      res = sqlite3_mprintf("COUNT(*)%s", filt);
    }else if(is_arr_field){
      char *col = pvtBuildScalarExpr(db, operand, fields, prefix);
      if(col) res = sqlite3_mprintf("SUM(COALESCE(json_array_length(%s),0))%s", col, filt);
      sqlite3_free(col);
    }else{
      char *inner = pvtBuildScalarExpr(db, operand, fields, prefix);
      if(inner) res = sqlite3_mprintf("COUNT(%s%s)%s", distinct?"DISTINCT ":"", inner, filt);
      sqlite3_free(inner);
    }
  }else if(!strcmp(fl,"$sum")||!strcmp(fl,"$avg")||!strcmp(fl,"$min")||!strcmp(fl,"$max")){
    const char *fn = !strcmp(fl,"$sum")?"SUM":!strcmp(fl,"$avg")?"AVG":!strcmp(fl,"$min")?"MIN":"MAX";
    if(is_arr_field){
      char *col = pvtBuildScalarExpr(db, operand, fields, prefix);
      if(col){
        if(!strcmp(fl,"$avg"))
          res = sqlite3_mprintf("(SUM((SELECT SUM(value) FROM json_each(%s))) / NULLIF(SUM(COALESCE(json_array_length(%s),0)),0))%s", col, col, filt);
        else
          res = sqlite3_mprintf("%s((SELECT %s(value) FROM json_each(%s)))%s", fn, fn, col, filt);
      }
      sqlite3_free(col);
    }else{
      char *inner = pvtBuildScalarExpr(db, operand, fields, prefix);
      if(inner) res = sqlite3_mprintf("%s(%s%s)%s", fn, distinct?"DISTINCT ":"", inner, filt);
      sqlite3_free(inner);
    }
  }else if(!strcmp(fl,"$string_agg")){
    if(optype && !strcmp(optype,"array") && pvtJsonArrayLen(db,operand,"$")==2){
      char *v=pvtScalarNode(db,operand,"$[0]",fields,prefix), *s=pvtScalarNode(db,operand,"$[1]",fields,prefix);
      if(v&&s) res=sqlite3_mprintf("group_concat(%s%s, %s)%s", distinct?"DISTINCT ":"", v, s, filt);
      sqlite3_free(v); sqlite3_free(s);
    }
  }else if(!strcmp(fl,"$bool_and")||!strcmp(fl,"$bool_or")){
    char *inner = pvtBuildScalarExpr(db, operand, fields, prefix);
    if(inner) res = sqlite3_mprintf("%s(%s)%s", !strcmp(fl,"$bool_and")?"MIN":"MAX", inner, filt);
    sqlite3_free(inner);
  }
  sqlite3_free(func); sqlite3_free(operand); sqlite3_free(optype); sqlite3_free(filt);
  return res;
}

/* compile aggregate array -> "expr AS alias, ..." (NULL on error). */
static char *pvtBuildAggProjection(sqlite3 *db, const char *aggs, const char *fields, const char *prefix){
  sqlite3_str *out = sqlite3_str_new(db);
  int idx = 0, err = 0;
  sqlite3_stmt *it = 0; sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1)",-1,&it,0);
  sqlite3_bind_text(it,1,aggs,-1,SQLITE_TRANSIENT);
  while(!err && sqlite3_step(it)==SQLITE_ROW){
    idx++;
    char *e = sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0));
    char *alias = jsonGetText(db,e,"$.alias");
    char *expr = pvtBuildAggExpr(db, e, fields, prefix);
    if(!expr){ err = 1; }
    else{
      if(idx>1) sqlite3_str_append(out, ", ", 2);
      if(alias) sqlite3_str_appendf(out, "%s AS \"%w\"", expr, alias);
      else      sqlite3_str_appendf(out, "%s AS \"_agg_%d\"", expr, idx);
    }
    sqlite3_free(alias); sqlite3_free(expr); sqlite3_free(e);
  }
  sqlite3_finalize(it);
  char *body = sqlite3_str_finish(out);
  if(err){ sqlite3_free(body); return 0; }
  return body;
}

static char *pvtBuildAggregateSql(sqlite3 *db, sqlite3_int64 scheme, const char *filter,
                                  const char *aggs, const char *source_mode, const char *tree_ids,
                                  int has_md, int md, int include_seed, int polymorphic){
  if(!source_mode) source_mode = "flat";
  int is_flat = !strcmp(source_mode,"flat");
  int no_tree = !tree_ids || !strcmp(tree_ids,"null");
  char *fields = pvtCollectAll(db, scheme, filter, 0, 0, aggs, 0);
  if(!fields) return 0;
  char *push=0,*resid=0; pvtSplitFilter(db, filter, fields, &push, &resid);
  const char *outer = resid;
  int has_prop = pvtHasPropFields(db, fields);
  char *result = 0;

  if(!outer && is_flat && no_tree && !has_prop){
    char *proj = pvtBuildAggProjection(db, aggs, fields, "o.");
    if(proj) result = sqlite3_mprintf("SELECT %s FROM _objects o\nWHERE o._id_scheme = %lld%s%s",
      proj, (long long)scheme, (push&&*push)?" AND ":"", (push&&*push)?push:"");
    sqlite3_free(proj); goto done;
  }
  {
    int force_outer = pvtHasCheck(db, outer, 1) || !strcmp(fields,"{}");
    int narrow = is_flat && !force_outer && !pvtHasNestedDict(db, fields);
    char *cte = pvtBuildCteSql(db, scheme, fields, source_mode, no_tree?0:tree_ids, has_md, md, force_outer, push, narrow, include_seed, polymorphic, 0);
    if(!cte) goto done;
    char *where_sql = pvtBuildWhereFromJson(db, outer, fields, narrow?"o.":"");
    char *proj = pvtBuildAggProjection(db, aggs, fields, narrow?"o.":"");
    if(where_sql && proj){
      if(narrow) result = sqlite3_mprintf("%s\nSELECT %s FROM _pvt_cte\nJOIN _objects o ON o._id = _pvt_cte._id_object\nWHERE %s", cte, proj, where_sql);
      else       result = sqlite3_mprintf("%s\nSELECT %s FROM _pvt_cte\nWHERE %s", cte, proj, where_sql);
    }
    sqlite3_free(cte); sqlite3_free(where_sql); sqlite3_free(proj);
  }
done:
  sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid);
  return result;
}

static char *pvtBuildGroupBySql(sqlite3 *db, sqlite3_int64 scheme, const char *filter,
                                const char *group_by, const char *aggs, const char *having,
                                const char *order, int has_limit, int limit, int offset,
                                const char *source_mode, const char *tree_ids, int has_md, int md,
                                int include_seed, int polymorphic){
  if(!source_mode) source_mode = "flat";
  int is_flat = !strcmp(source_mode,"flat");
  int no_tree = !tree_ids || !strcmp(tree_ids,"null");
  if(!group_by) return 0;
  char *fields = pvtCollectAll(db, scheme, filter, order, group_by, aggs, having);
  if(!fields) return 0;
  char *push=0,*resid=0; pvtSplitFilter(db, filter, fields, &push, &resid);
  const char *outer = resid;
  int has_prop = pvtHasPropFields(db, fields);

  int shapeA = (!outer && is_flat && no_tree && !has_prop);
  int force_outer = pvtHasCheck(db, outer, 1) || !strcmp(fields,"{}");
  int narrow = !shapeA && is_flat && !force_outer && !pvtHasNestedDict(db, fields);
  const char *prefix = shapeA ? "o." : (narrow ? "o." : "");

  char *cte = 0;
  if(!shapeA){
    cte = pvtBuildCteSql(db, scheme, fields, source_mode, no_tree?0:tree_ids, has_md, md, force_outer, push, narrow, include_seed, polymorphic, 0);
    if(!cte){ sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid); return 0; }
  }

  /* group-key projection + GROUP BY list */
  sqlite3_str *sel = sqlite3_str_new(db); sqlite3_str *grp = sqlite3_str_new(db);
  int gidx = 0, err = 0;
  sqlite3_stmt *gi = 0; sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1)",-1,&gi,0);
  sqlite3_bind_text(gi,1,group_by,-1,SQLITE_TRANSIENT);
  while(!err && sqlite3_step(gi)==SQLITE_ROW){
    gidx++;
    char *e = sqlite3_mprintf("%s",(const char*)sqlite3_column_text(gi,0));
    char *col = pvtCompileOrderCol(db, e, fields, prefix);
    char *alias = jsonGetText(db,e,"$.alias"); if(!alias) alias=jsonGetText(db,e,"$.field"); if(!alias) alias=jsonGetText(db,e,"$.field_path");
    if(!col) err = 1;
    else{
      if(gidx>1){ sqlite3_str_append(sel,", ",2); sqlite3_str_append(grp,", ",2); }
      if(alias) sqlite3_str_appendf(sel, "%s AS \"%w\"", col, alias);
      else      sqlite3_str_appendf(sel, "%s AS \"_grp_%d\"", col, gidx);
      sqlite3_str_appendf(grp, "%s", col);
    }
    sqlite3_free(col); sqlite3_free(alias); sqlite3_free(e);
  }
  sqlite3_finalize(gi);
  char *sel_sql = sqlite3_str_finish(sel);
  char *grp_sql = sqlite3_str_finish(grp);

  char *agg_proj = 0;
  if(!err && aggs){ char *t=jsonTypeAt(db,aggs,"$"); int n = t&&!strcmp(t,"array")?pvtJsonArrayLen(db,aggs,"$"):0; sqlite3_free(t);
    if(n>0){ agg_proj = pvtBuildAggProjection(db, aggs, fields, prefix); if(!agg_proj) err=1; } }

  char *where_sql = err?0:pvtBuildWhereFromJson(db, outer, fields, prefix);
  char *order_sql = err?0:pvtBuildOrderConditions(db, order, fields, prefix);
  char *having_sql = sqlite3_mprintf("");
  if(!err && having){ char *t=jsonTypeAt(db,having,"$"); int isobj=t&&!strcmp(t,"object"); sqlite3_free(t);
    if(isobj){ char *probe=jsonGetText(db,having,"$"); int empty=probe&&!strcmp(probe,"{}"); sqlite3_free(probe);
      if(!empty){ char *hb=pvtBuildBoolExpr(db,having,fields,prefix); if(hb){ sqlite3_free(having_sql); having_sql=sqlite3_mprintf("\nHAVING %s", hb); sqlite3_free(hb);} else err=1; } } }

  char *result = 0;
  if(!err && where_sql && order_sql){
    char *full_sel = agg_proj ? sqlite3_mprintf("%s, %s", sel_sql, agg_proj) : sqlite3_mprintf("%s", sel_sql);
    sqlite3_str *pg = sqlite3_str_new(db);
    if(has_limit && limit>=0) sqlite3_str_appendf(pg,"\nLIMIT %d",limit);
    if(offset>0) sqlite3_str_appendf(pg,"\nOFFSET %d",offset);
    char *paging = sqlite3_str_finish(pg);
    const char *wclause = strcmp(where_sql,"TRUE")==0 ? "" : where_sql;
    if(shapeA){
      result = sqlite3_mprintf("SELECT %s FROM _objects o\nWHERE o._id_scheme = %lld%s%s\nGROUP BY %s%s%s%s",
        full_sel, (long long)scheme, (push&&*push)?" AND ":"", (push&&*push)?push:"",
        grp_sql, having_sql, order_sql, paging);
    }else if(narrow){
      result = sqlite3_mprintf("%s\nSELECT %s FROM _pvt_cte\nJOIN _objects o ON o._id = _pvt_cte._id_object%s%s\nGROUP BY %s%s%s%s",
        cte, full_sel, *wclause?"\nWHERE ":"", wclause, grp_sql, having_sql, order_sql, paging);
    }else{
      result = sqlite3_mprintf("%s\nSELECT %s FROM _pvt_cte%s%s\nGROUP BY %s%s%s%s",
        cte, full_sel, *wclause?"\nWHERE ":"", wclause, grp_sql, having_sql, order_sql, paging);
    }
    sqlite3_free(full_sel); sqlite3_free(paging);
  }
  sqlite3_free(cte); sqlite3_free(sel_sql); sqlite3_free(grp_sql); sqlite3_free(agg_proj);
  sqlite3_free(where_sql); sqlite3_free(order_sql); sqlite3_free(having_sql);
  sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid);
  return result;
}

/* ------------------------------------------------------------------------- */
/* 26_pvt_array_groupby.sql — GROUP BY over array elements                   */
/* ------------------------------------------------------------------------- */
/* Element fields are projected into an inner subquery via LEFT JOINs on
   _values keyed by _array_parent_id; the outer query groups by inner aliases
   and applies HAVING through pvt_build_bool_expr. Port of 26_pvt_array_groupby.sql. */

/* Add one element-field LEFT JOIN + inner projection. Resolves <array_path>[].<field>
   to its element structure id and typed column. Returns 0 on resolve/type failure. */
static int pvtArrGbAddJoin(sqlite3 *db, sqlite3_int64 scheme, sqlite3_int64 arr_sid,
                           const char *field, const char *join_prefix, int join_idx,
                           sqlite3_str *join_parts, sqlite3_str *inner_select,
                           char **fields_map){
  sqlite3_int64 fsid = 0; char *fdbt = 0; int fisarr = 0;
  if(!pvtLookupStruct(db, scheme, field, 1, arr_sid, &fsid, &fdbt, &fisarr)){
    sqlite3_free(fdbt); return 0;
  }
  const char *col = pvtDbTypeToValueColumn(fdbt ? fdbt : "");
  sqlite3_free(fdbt);
  if(!col) return 0;
  char a[24]; sqlite3_snprintf(sizeof a, a, "%s%d", join_prefix, join_idx);
  sqlite3_str_appendf(join_parts,
    "%sLEFT JOIN _values \"%w\" ON \"%w\"._id_object = arr._id_object"
    " AND \"%w\"._id_structure = %lld AND \"%w\"._array_parent_id = arr._id",
    sqlite3_str_length(join_parts) ? "\n" : "",
    a, a, a, (long long)fsid, a);
  sqlite3_str_appendf(inner_select, "%s\"%w\".%s AS \"%w\"",
    sqlite3_str_length(inner_select) ? ", " : "", a, col, field);
  char *patch = sqlite3_mprintf("{\"%w\":{\"kind\":\"props\"}}", field);
  *fields_map = pvtPatch(db, *fields_map, patch);
  sqlite3_free(patch);
  return 1;
}

static int pvtArrGbHas(sqlite3 *db, const char *fields_map, const char *field){
  char *mp = sqlite3_mprintf("$.\"%w\"", field);
  char *t = jsonTypeAt(db, fields_map, mp);
  sqlite3_free(mp);
  int has = t != 0; sqlite3_free(t);
  return has;
}

static char *pvtBuildArrayGroupBySql(sqlite3 *db, sqlite3_int64 scheme,
    const char *array_path, const char *filter, const char *group_by,
    const char *aggs, const char *having, const char *order,
    int has_limit, int limit, int offset){
  if(!array_path || !*array_path) return 0;
  /* group_by must be a non-empty array */
  { char *t = jsonTypeAt(db, group_by, "$"); int ok = t && !strcmp(t,"array"); sqlite3_free(t);
    if(!ok || pvtJsonArrayLen(db, group_by, "$") <= 0) return 0; }

  /* resolve array structure id */
  sqlite3_int64 arr_sid = 0; char *arr_dbt = 0; int arr_isarr = 0;
  if(!pvtLookupStruct(db, scheme, array_path, 0, 0, &arr_sid, &arr_dbt, &arr_isarr)){
    sqlite3_free(arr_dbt); return 0;
  }
  sqlite3_free(arr_dbt);

  /* outer object filter -> "arr._id_object IN (SELECT _id FROM (<inner>) _filt)" */
  char *filter_clause = sqlite3_mprintf("");
  if(filter){
    char *t = jsonTypeAt(db, filter, "$"); int isobj = t && !strcmp(t,"object"); sqlite3_free(t);
    char *probe = isobj ? jsonGetText(db, filter, "$") : 0;
    int empty = probe && !strcmp(probe, "{}"); sqlite3_free(probe);
    if(isobj && !empty){
      char *fi = pvtBuildQuerySql(db, scheme, filter, 0, 0, 0, 0, 0, 0, 0, "flat", 0, 0, 1, 0);
      if(!fi){ sqlite3_free(filter_clause); return 0; }
      sqlite3_free(filter_clause);
      filter_clause = sqlite3_mprintf(
        "\n  AND arr._id_object IN (SELECT _id FROM (%s) _filt)", fi);
      sqlite3_free(fi);
    }
  }

  sqlite3_str *join_parts   = sqlite3_str_new(db);
  sqlite3_str *inner_select = sqlite3_str_new(db);
  sqlite3_str *select_parts = sqlite3_str_new(db);
  sqlite3_str *group_parts  = sqlite3_str_new(db);
  char *fields_map = sqlite3_mprintf("{}");
  int join_idx = 0, err = 0;

  /* ---- group_by entries */
  sqlite3_stmt *gi = 0;
  sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &gi, 0);
  sqlite3_bind_text(gi, 1, group_by, -1, SQLITE_TRANSIENT);
  while(!err && sqlite3_step(gi) == SQLITE_ROW){
    char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(gi, 0));
    char *field = jsonGetText(db, e, "$.field");
    char *alias = jsonGetText(db, e, "$.alias"); if(!alias && field) alias = sqlite3_mprintf("%s", field);
    if(!field){ err = 1; }
    else{
      join_idx++;
      if(!pvtArrGbAddJoin(db, scheme, arr_sid, field, "g", join_idx,
                          join_parts, inner_select, &fields_map)) err = 1;
      else{
        sqlite3_str_appendf(select_parts, "%s\"%w\" AS \"%w\"",
          sqlite3_str_length(select_parts) ? ", " : "", field, alias ? alias : field);
        sqlite3_str_appendf(group_parts, "%s\"%w\"",
          sqlite3_str_length(group_parts) ? ", " : "", field);
      }
    }
    sqlite3_free(field); sqlite3_free(alias); sqlite3_free(e);
  }
  sqlite3_finalize(gi);

  /* ---- aggregations */
  if(!err && aggs){
    char *t = jsonTypeAt(db, aggs, "$"); int isarr = t && !strcmp(t,"array"); sqlite3_free(t);
    if(isarr){
      sqlite3_stmt *ai = 0;
      sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &ai, 0);
      sqlite3_bind_text(ai, 1, aggs, -1, SQLITE_TRANSIENT);
      while(!err && sqlite3_step(ai) == SQLITE_ROW){
        char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(ai, 0));
        char *field = jsonGetText(db, e, "$.field");
        char *func = jsonGetText(db, e, "$.func");
        char *alias = jsonGetText(db, e, "$.alias"); if(!alias && func) alias = sqlite3_mprintf("%s", func);
        char ufunc[16]; pvtLc(ufunc, func ? func : "", sizeof ufunc);
        for(char *p = ufunc; *p; p++) if(*p >= 'a' && *p <= 'z') *p -= 32;  /* upper */
        if(!alias || !*alias){ err = 1; }
        else if(!strcmp(ufunc,"COUNT") && (!field || !strcmp(field,"*"))){
          sqlite3_str_appendf(select_parts, "%sCOUNT(*) AS \"%w\"",
            sqlite3_str_length(select_parts) ? ", " : "", alias);
        }else if(!field){ err = 1; }
        else{
          if(!pvtArrGbHas(db, fields_map, field)){
            join_idx++;
            if(!pvtArrGbAddJoin(db, scheme, arr_sid, field, "a", join_idx,
                                join_parts, inner_select, &fields_map)) err = 1;
          }
          if(!err)
            sqlite3_str_appendf(select_parts, "%s%s(\"%w\") AS \"%w\"",
              sqlite3_str_length(select_parts) ? ", " : "", ufunc, field, alias);
        }
        sqlite3_free(field); sqlite3_free(func); sqlite3_free(alias); sqlite3_free(e);
      }
      sqlite3_finalize(ai);
    }
  }

  /* ---- HAVING: register joins for any $field not yet projected, then translate */
  char *having_sql = sqlite3_mprintf("");
  if(!err && having){
    char *t = jsonTypeAt(db, having, "$"); int isobj = t && !strcmp(t,"object"); sqlite3_free(t);
    char *probe = isobj ? jsonGetText(db, having, "$") : 0;
    int empty = probe && !strcmp(probe, "{}"); sqlite3_free(probe);
    if(isobj && !empty){
      sqlite3_stmt *hi = 0;
      sqlite3_prepare_v2(db, "SELECT DISTINCT value FROM json_tree(?1) WHERE key = '$field'", -1, &hi, 0);
      sqlite3_bind_text(hi, 1, having, -1, SQLITE_TRANSIENT);
      while(!err && sqlite3_step(hi) == SQLITE_ROW){
        const char *hf = (const char*)sqlite3_column_text(hi, 0);
        if(hf && !pvtArrGbHas(db, fields_map, hf)){
          join_idx++;
          if(!pvtArrGbAddJoin(db, scheme, arr_sid, hf, "h", join_idx,
                              join_parts, inner_select, &fields_map)) err = 1;
        }
      }
      sqlite3_finalize(hi);
      if(!err){
        char *hb = pvtBuildBoolExpr(db, having, fields_map, "");
        if(hb){ sqlite3_free(having_sql); having_sql = sqlite3_mprintf("\nHAVING %s", hb); sqlite3_free(hb); }
        else err = 1;
      }
    }
  }

  /* ---- ORDER BY (over outer aliases) */
  char *paging = sqlite3_mprintf("");
  if(!err && order){
    char *t = jsonTypeAt(db, order, "$"); int isarr = t && !strcmp(t,"array"); sqlite3_free(t);
    if(isarr && pvtJsonArrayLen(db, order, "$") > 0){
      sqlite3_str *ord = sqlite3_str_new(db);
      sqlite3_stmt *oi = 0;
      sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1)", -1, &oi, 0);
      sqlite3_bind_text(oi, 1, order, -1, SQLITE_TRANSIENT);
      while(!err && sqlite3_step(oi) == SQLITE_ROW){
        char *e = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(oi, 0));
        char *of = jsonGetText(db, e, "$.field");
        char *asc = jsonGetText(db, e, "$.asc");
        if(!of){ err = 1; }
        else sqlite3_str_appendf(ord, "%s\"%w\"%s",
          sqlite3_str_length(ord) ? ", " : "", of,
          (asc && !strcmp(asc,"false")) ? " DESC" : "");
        sqlite3_free(of); sqlite3_free(asc); sqlite3_free(e);
      }
      sqlite3_finalize(oi);
      char *ords = sqlite3_str_finish(ord);
      if(!err && ords && *ords){ sqlite3_free(paging); paging = sqlite3_mprintf("\nORDER BY %s", ords); }
      sqlite3_free(ords);
    }
  }
  if(!err && has_limit && limit >= 0){ char *p = sqlite3_mprintf("%s\nLIMIT %d", paging, limit); sqlite3_free(paging); paging = p; }
  if(!err && offset > 0){ char *p = sqlite3_mprintf("%s\nOFFSET %d", paging, offset); sqlite3_free(paging); paging = p; }

  char *result = 0;
  char *insel = sqlite3_str_finish(inner_select);
  char *joins = sqlite3_str_finish(join_parts);
  char *sels  = sqlite3_str_finish(select_parts);
  char *grps  = sqlite3_str_finish(group_parts);
  if(!err && insel && *insel && sels && *sels && grps && *grps){
    char *inner_sql = sqlite3_mprintf(
      "SELECT %s\nFROM _values arr\nJOIN _objects o ON o._id = arr._id_object\n%s\n"
      "WHERE o._id_scheme = %lld AND arr._id_structure = %lld AND arr._array_index IS NOT NULL%s",
      insel, joins ? joins : "", (long long)scheme, (long long)arr_sid, filter_clause);
    result = sqlite3_mprintf(
      "SELECT %s\nFROM (\n%s\n) elements\nGROUP BY %s%s%s",
      sels, inner_sql, grps, having_sql, paging);
    sqlite3_free(inner_sql);
  }
  sqlite3_free(insel); sqlite3_free(joins); sqlite3_free(sels); sqlite3_free(grps);
  sqlite3_free(fields_map); sqlite3_free(having_sql); sqlite3_free(paging); sqlite3_free(filter_clause);
  return result;
}

/* ------------------------------------------------------------------------- */
/* SQL function bindings                                                     */
/* ------------------------------------------------------------------------- */

static void pvtModuleVersionFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, PVT_MODULE_VERSION, -1, SQLITE_STATIC);
}

static void pvtDbTypeToValueColumnFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc;
  const char *t = (const char*)sqlite3_value_text(argv[0]);
  const char *col = pvtDbTypeToValueColumn(t);
  if(col) sqlite3_result_text(ctx, col, -1, SQLITE_STATIC);
  else    sqlite3_result_null(ctx);
}

static void pvtBuildColumnExprFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *name = (const char*)sqlite3_value_text(argv[0]);
  const char *meta = (const char*)sqlite3_value_text(argv[1]);
  int aio = argc > 2 ? sqlite3_value_int(argv[2]) : 0;
  if(!name || !meta){ sqlite3_result_null(ctx); return; }
  char *s = pvtBuildColumnExpr(db, name, meta, aio);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_null(ctx);
}

static void pvtResolveFieldPathFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc;
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *path = (const char*)sqlite3_value_text(argv[1]);
  char *m = pvtResolveFieldPath(db, scheme, path);
  if(m) sqlite3_result_text(ctx, m, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_resolve_field_path: field not found", -1);
}

static void pvtCollectFieldsFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *filter = sqlite3_value_type(argv[1]) == SQLITE_NULL ? 0 : (const char*)sqlite3_value_text(argv[1]);
  const char *order  = (argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL) ? (const char*)sqlite3_value_text(argv[2]) : 0;
  int include_all = argc > 3 ? sqlite3_value_int(argv[3]) : 0;
  char *m = pvtCollectFields(db, scheme, filter, order, include_all);
  if(m) sqlite3_result_text(ctx, m, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_collect_fields: unresolved field", -1);
}

static void pvtHasNullCheckFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc;
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *f = sqlite3_value_type(argv[0]) == SQLITE_NULL ? 0 : (const char*)sqlite3_value_text(argv[0]);
  sqlite3_result_int(ctx, pvtHasCheck(db, f, 0));
}
static void pvtHasAbsenceCheckFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc;
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *f = sqlite3_value_type(argv[0]) == SQLITE_NULL ? 0 : (const char*)sqlite3_value_text(argv[0]);
  sqlite3_result_int(ctx, pvtHasCheck(db, f, 1));
}

/* pvt_build_where_from_json(filter_json, fields_json [, base_prefix]) -> TEXT */
static void pvtBuildWhereFromJsonFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *filter = (const char*)sqlite3_value_text(argv[0]);
  const char *fields = (const char*)sqlite3_value_text(argv[1]);
  const char *prefix = argc > 2 ? (const char*)sqlite3_value_text(argv[2]) : "";
  char *s = pvtBuildWhereFromJson(db, filter, fields, prefix ? prefix : "");
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_build_where_from_json: unsupported filter node", -1);
}

/* pvt_build_field_condition(field_name, meta_json, op_json [, base_prefix]) -> TEXT */
static void pvtBuildFieldConditionFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *name = (const char*)sqlite3_value_text(argv[0]);
  const char *meta = (const char*)sqlite3_value_text(argv[1]);
  const char *opj  = (const char*)sqlite3_value_text(argv[2]);
  const char *opty = 0;
  { char *t = jsonTypeAt(db, opj, "$"); /* derive operand type */
    char *s = pvtBuildFieldCondition(db, name, meta, opj, t ? t : "text",
                                     argc > 3 ? (const char*)sqlite3_value_text(argv[3]) : "");
    sqlite3_free(t);
    if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
    else  sqlite3_result_error(ctx, "pvt_build_field_condition: unsupported operator", -1);
    (void)opty;
  }
}

/* pvt_build_aggregate_sql(scheme, filter, aggs[, mode, tree_ids, max_depth, include_seed, polymorphic]) */
static void pvtBuildAggregateSqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  #define AA(i) (argc > (i) && sqlite3_value_type(argv[i]) != SQLITE_NULL)
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *filter = AA(1) ? (const char*)sqlite3_value_text(argv[1]) : 0;
  const char *aggs = AA(2) ? (const char*)sqlite3_value_text(argv[2]) : 0;
  const char *mode = AA(3) ? (const char*)sqlite3_value_text(argv[3]) : "flat";
  const char *tree = AA(4) ? (const char*)sqlite3_value_text(argv[4]) : 0;
  int has_md = AA(5); int md = has_md ? sqlite3_value_int(argv[5]) : 0;
  int seed = AA(6) ? sqlite3_value_int(argv[6]) : 1;
  int poly = AA(7) ? sqlite3_value_int(argv[7]) : 1;
  #undef AA
  char *s = pvtBuildAggregateSql(db, scheme, filter, aggs, mode, tree, has_md, md, seed, poly);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_build_aggregate_sql: unsupported", -1);
}

/* pvt_build_groupby_sql(scheme, filter, group_by[, aggs, having, order, limit, offset, mode, tree_ids, max_depth, include_seed, polymorphic]) */
static void pvtBuildGroupBySqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  #define GA(i) (argc > (i) && sqlite3_value_type(argv[i]) != SQLITE_NULL)
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *filter = GA(1) ? (const char*)sqlite3_value_text(argv[1]) : 0;
  const char *group_by = GA(2) ? (const char*)sqlite3_value_text(argv[2]) : 0;
  const char *aggs = GA(3) ? (const char*)sqlite3_value_text(argv[3]) : 0;
  const char *having = GA(4) ? (const char*)sqlite3_value_text(argv[4]) : 0;
  const char *order = GA(5) ? (const char*)sqlite3_value_text(argv[5]) : 0;
  int has_limit = GA(6); int limit = has_limit ? sqlite3_value_int(argv[6]) : 0;
  int offset = GA(7) ? sqlite3_value_int(argv[7]) : 0;
  const char *mode = GA(8) ? (const char*)sqlite3_value_text(argv[8]) : "flat";
  const char *tree = GA(9) ? (const char*)sqlite3_value_text(argv[9]) : 0;
  int has_md = GA(10); int md = has_md ? sqlite3_value_int(argv[10]) : 0;
  int seed = GA(11) ? sqlite3_value_int(argv[11]) : 1;
  int poly = GA(12) ? sqlite3_value_int(argv[12]) : 1;
  #undef GA
  char *s = pvtBuildGroupBySql(db, scheme, filter, group_by, aggs, having, order, has_limit, limit, offset, mode, tree, has_md, md, seed, poly);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_build_groupby_sql: unsupported", -1);
}

/* pvt_build_array_groupby_sql(scheme, array_path, filter, group_by[, aggs, having, order, limit, offset]) */
static void pvtBuildArrayGroupBySqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  #define AG(i) (argc > (i) && sqlite3_value_type(argv[i]) != SQLITE_NULL)
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *array_path = AG(1) ? (const char*)sqlite3_value_text(argv[1]) : 0;
  const char *filter   = AG(2) ? (const char*)sqlite3_value_text(argv[2]) : 0;
  const char *group_by = AG(3) ? (const char*)sqlite3_value_text(argv[3]) : 0;
  const char *aggs     = AG(4) ? (const char*)sqlite3_value_text(argv[4]) : 0;
  const char *having   = AG(5) ? (const char*)sqlite3_value_text(argv[5]) : 0;
  const char *order    = AG(6) ? (const char*)sqlite3_value_text(argv[6]) : 0;
  int has_limit = AG(7); int limit = has_limit ? sqlite3_value_int(argv[7]) : 0;
  int offset = AG(8) ? sqlite3_value_int(argv[8]) : 0;
  #undef AG
  char *s = pvtBuildArrayGroupBySql(db, scheme, array_path, filter, group_by, aggs, having, order, has_limit, limit, offset);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_build_array_groupby_sql: unsupported", -1);
}

/* pvt_build_query_sql(scheme, filter[, limit, offset, order, max_depth, distinct,
**                     source_mode, tree_ids_json, include_seed, polymorphic]) */
static void pvtBuildQuerySqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  #define PA(i) (argc > (i) && sqlite3_value_type(argv[i]) != SQLITE_NULL)
  const char *filter = PA(1) ? (const char*)sqlite3_value_text(argv[1]) : 0;
  int has_limit = PA(2); int limit = has_limit ? sqlite3_value_int(argv[2]) : 0;
  int offset = PA(3) ? sqlite3_value_int(argv[3]) : 0;
  const char *order = PA(4) ? (const char*)sqlite3_value_text(argv[4]) : 0;
  int has_md = PA(5); int md = has_md ? sqlite3_value_int(argv[5]) : 0;
  int distinct = PA(6) ? sqlite3_value_int(argv[6]) : 0;
  const char *mode = PA(7) ? (const char*)sqlite3_value_text(argv[7]) : "flat";
  const char *tree = PA(8) ? (const char*)sqlite3_value_text(argv[8]) : 0;
  int seed = PA(9) ? sqlite3_value_int(argv[9]) : 1;
  int poly = PA(10) ? sqlite3_value_int(argv[10]) : 1;
  const char *dion = PA(11) ? (const char*)sqlite3_value_text(argv[11]) : 0;  /* distinct_on (DistinctBy) */
  #undef PA
  char *s = pvtBuildQuerySql(db, scheme, filter, has_limit, limit, offset, order,
                             has_md, md, distinct, mode, tree, seed, poly, dion);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_build_query_sql: unsupported (stubbed op / nested-dict / unresolved field)", -1);
}

/* pvt_build_cte_sql(scheme, fields[, mode[, narrow[, force_outer[, extra_where[, residual]]]]]) */
static void pvtBuildCteSqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *fields = (const char*)sqlite3_value_text(argv[1]);
  const char *mode = (argc > 2 && sqlite3_value_type(argv[2]) != SQLITE_NULL) ? (const char*)sqlite3_value_text(argv[2]) : "flat";
  int narrow = argc > 3 ? sqlite3_value_int(argv[3]) : 0;
  int force_outer = argc > 4 ? sqlite3_value_int(argv[4]) : 1;
  const char *extra = (argc > 5 && sqlite3_value_type(argv[5]) != SQLITE_NULL) ? (const char*)sqlite3_value_text(argv[5]) : 0;
  const char *resid = (argc > 6 && sqlite3_value_type(argv[6]) != SQLITE_NULL) ? (const char*)sqlite3_value_text(argv[6]) : 0;
  char *s = pvtBuildCteSql(db, scheme, fields, mode, 0, 0, 0, force_outer, extra, narrow, 1, 1, resid);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free);
  else  sqlite3_result_error(ctx, "pvt_build_cte_sql: unsupported (nested-dict not yet ported)", -1);
}

int redbRegisterPvt(sqlite3 *db){
  int rc;
  rc = sqlite3_create_function(db, "pvt_module_version", 0,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0,
                               pvtModuleVersionFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "pvt_db_type_to_value_column", 1,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0,
                               pvtDbTypeToValueColumnFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "pvt_build_column_expr", 2, SQLITE_UTF8, 0,
                               pvtBuildColumnExprFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;
  rc = sqlite3_create_function(db, "pvt_build_column_expr", 3, SQLITE_UTF8, 0,
                               pvtBuildColumnExprFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "pvt_build_field_condition", 3, SQLITE_UTF8, 0,
                               pvtBuildFieldConditionFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;
  rc = sqlite3_create_function(db, "pvt_build_field_condition", 4, SQLITE_UTF8, 0,
                               pvtBuildFieldConditionFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "pvt_build_where_from_json", 2, SQLITE_UTF8, 0,
                               pvtBuildWhereFromJsonFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;
  rc = sqlite3_create_function(db, "pvt_build_where_from_json", 3, SQLITE_UTF8, 0,
                               pvtBuildWhereFromJsonFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "pvt_resolve_field_path", 2, SQLITE_UTF8, 0,
                               pvtResolveFieldPathFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;
  for(int a = 2; a <= 4; a++){
    rc = sqlite3_create_function(db, "pvt_collect_fields", a, SQLITE_UTF8, 0,
                                 pvtCollectFieldsFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }
  rc = sqlite3_create_function(db, "pvt_has_null_check", 1, SQLITE_UTF8, 0,
                               pvtHasNullCheckFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;
  rc = sqlite3_create_function(db, "pvt_has_absence_check", 1, SQLITE_UTF8, 0,
                               pvtHasAbsenceCheckFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  for(int a = 2; a <= 7; a++){
    rc = sqlite3_create_function(db, "pvt_build_cte_sql", a, SQLITE_UTF8, 0,
                                 pvtBuildCteSqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }

  for(int a = 1; a <= 12; a++){   /* 12th = distinct_on (currently ignored by the engine) */
    rc = sqlite3_create_function(db, "pvt_build_query_sql", a, SQLITE_UTF8, 0,
                                 pvtBuildQuerySqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }
  for(int a = 3; a <= 8; a++){
    rc = sqlite3_create_function(db, "pvt_build_aggregate_sql", a, SQLITE_UTF8, 0,
                                 pvtBuildAggregateSqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }
  for(int a = 3; a <= 13; a++){
    rc = sqlite3_create_function(db, "pvt_build_groupby_sql", a, SQLITE_UTF8, 0,
                                 pvtBuildGroupBySqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }
  for(int a = 4; a <= 9; a++){
    rc = sqlite3_create_function(db, "pvt_build_array_groupby_sql", a, SQLITE_UTF8, 0,
                                 pvtBuildArrayGroupBySqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }

  /* window + projection (separate TU). */
  rc = redbRegisterPvtAnalytics(db);
  if(rc != SQLITE_OK) return rc;

  return SQLITE_OK;
}
