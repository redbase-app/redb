/*
** REDB SQLite Free — v2-pvt analytics orchestrators (window + projection).
** Port of 23_pvt_window.sql + 24_pvt_projection.sql. Reuses the core engine
** declared in redb_pvt.h (expr/order/cte/where/split/collect). SQLite window
** functions / PARTITION BY / ORDER BY / frames / FILTER map ~1:1 from PG.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "redb_ext.h"
#include "redb_pvt.h"
#include <string.h>

/* ------------------------------------------------------------------------- */
/* 23 — window                                                               */
/* ------------------------------------------------------------------------- */

/* frame bound at container+path -> SQL text (sqlite3_malloc'd) or NULL. */
static char *pvtFrameBound(sqlite3 *db, const char *container, const char *path){
  char *ty = jsonTypeAt(db, container, path);
  if(!ty) return 0;
  char *res = 0;
  if(!strcmp(ty,"text")){
    char *s = jsonGetText(db, container, path);
    char l[32]; pvtLc(l, s ? s : "", sizeof l);
    if(!strcmp(l,"unbounded_preceding")) res = sqlite3_mprintf("UNBOUNDED PRECEDING");
    else if(!strcmp(l,"current_row"))    res = sqlite3_mprintf("CURRENT ROW");
    else if(!strcmp(l,"unbounded_following")) res = sqlite3_mprintf("UNBOUNDED FOLLOWING");
    sqlite3_free(s);
  }else if(!strcmp(ty,"object")){
    char *pp = sqlite3_mprintf("%s.preceding", path);
    char *fp = sqlite3_mprintf("%s.following", path);
    int f = 0; sqlite3_int64 n = jsonGetInt(db, container, pp, &f);
    if(f) res = sqlite3_mprintf("%lld PRECEDING", (long long)n);
    else { f = 0; n = jsonGetInt(db, container, fp, &f); if(f) res = sqlite3_mprintf("%lld FOLLOWING", (long long)n); }
    sqlite3_free(pp); sqlite3_free(fp);
  }
  sqlite3_free(ty);
  return res;
}

/* OVER (...) clause from a window node. */
static char *pvtWindowOver(sqlite3 *db, const char *node, const char *fields, const char *prefix){
  sqlite3_str *parts = sqlite3_str_new(db);
  int n = 0;

  /* PARTITION BY */
  char *pb = jsonGetText(db, node, "$.partition_by");
  if(pb){
    int len = pvtJsonArrayLen(db, pb, "$");
    if(len > 0){
      sqlite3_str_append(parts, "PARTITION BY ", 13); n++;
      sqlite3_stmt *it = 0; sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1)",-1,&it,0);
      sqlite3_bind_text(it,1,pb,-1,SQLITE_TRANSIENT);
      int i = 0;
      while(sqlite3_step(it)==SQLITE_ROW){ char *e=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0));
        char *col=pvtCompileOrderCol(db,e,fields,prefix); if(col){ if(i) sqlite3_str_append(parts,", ",2); sqlite3_str_appendf(parts,"%s",col); i++; } sqlite3_free(col); sqlite3_free(e); }
      sqlite3_finalize(it);
    }
    sqlite3_free(pb);
  }

  /* ORDER BY (reuse builder, strip leading newline+keyword) */
  char *ob = jsonGetText(db, node, "$.order_by");
  if(ob){
    int len = pvtJsonArrayLen(db, ob, "$");
    if(len > 0){
      char *os = pvtBuildOrderConditions(db, ob, fields, prefix);   /* "\nORDER BY ..." */
      if(os && *os){ const char *p = os; while(*p=='\n') p++; if(n) sqlite3_str_append(parts," ",1); sqlite3_str_appendf(parts,"%s",p); n++; }
      sqlite3_free(os);
    }
    sqlite3_free(ob);
  }

  /* FRAME */
  char *frt = jsonTypeAt(db, node, "$.frame");
  if(frt && !strcmp(frt,"object")){
    char *fr = jsonGetText(db, node, "$.frame");
    char *tp = jsonGetText(db, fr, "$.type"); char low[8]; pvtLc(low, tp?tp:"rows", sizeof low);
    const char *kw = !strcmp(low,"range") ? "RANGE" : !strcmp(low,"groups") ? "GROUPS" : "ROWS";
    char *start = pvtFrameBound(db, fr, "$.start");
    char *endty = jsonTypeAt(db, fr, "$.end");
    char *frag = 0;
    if(endty && strcmp(endty,"null")!=0){ char *e = pvtFrameBound(db, fr, "$.end"); if(start&&e) frag=sqlite3_mprintf("%s BETWEEN %s AND %s", kw, start, e); sqlite3_free(e); }
    else if(start) frag = sqlite3_mprintf("%s %s", kw, start);
    if(frag){
      char *excl = jsonGetText(db, fr, "$.exclude");
      if(excl){ char l[16]; pvtLc(l, excl, sizeof l);
        const char *x = !strcmp(l,"current_row")?"CURRENT ROW":!strcmp(l,"group")?"GROUP":!strcmp(l,"ties")?"TIES":!strcmp(l,"no_others")?"NO OTHERS":0;
        if(x){ char *f2=sqlite3_mprintf("%s EXCLUDE %s", frag, x); sqlite3_free(frag); frag=f2; } }
      sqlite3_free(excl);
      if(n) sqlite3_str_append(parts," ",1); sqlite3_str_appendf(parts,"%s",frag); n++;
    }
    sqlite3_free(frag); sqlite3_free(start); sqlite3_free(endty); sqlite3_free(tp); sqlite3_free(fr);
  }
  sqlite3_free(frt);

  char *body = sqlite3_str_finish(parts);
  char *res = n ? sqlite3_mprintf("OVER (%s)", body) : sqlite3_mprintf("OVER ()");
  sqlite3_free(body);
  return res;
}

char *pvtBuildWindowExpr(sqlite3 *db, const char *node, const char *fields, const char *prefix){
  if(!prefix) prefix = "";
  char *func = jsonGetText(db, node, "$.func");
  if(!func) return 0;
  char fl[20]; pvtLc(fl, func, sizeof fl);
  char fu[20]; for(int i=0; fl[i]; i++) fu[i]=(fl[i]>='a'&&fl[i]<='z')?fl[i]-32:fl[i], fu[i+1]=0; if(!fl[0]) fu[0]=0;

  int is_agg = !strcmp(fl,"sum")||!strcmp(fl,"avg")||!strcmp(fl,"min")||!strcmp(fl,"max")||!strcmp(fl,"count");
  char *call = 0;
  char *argst = jsonTypeAt(db, node, "$.args");
  if(argst){
    char *args = jsonGetText(db, node, "$.args");
    int len = pvtJsonArrayLen(db, args, "$");
    /* count(*) shorthand */
    char *a0t = jsonTypeAt(db, args, "$[0]");
    if(!strcmp(fl,"count") && len==1 && a0t && !strcmp(a0t,"text")){
      char *a0 = jsonGetText(db,args,"$[0]"); if(a0 && !strcmp(a0,"*")) call = sqlite3_mprintf("COUNT(*)"); sqlite3_free(a0);
    }
    if(!call){
      sqlite3_str *ab = sqlite3_str_new(db); sqlite3_str_appendf(ab,"%s(", fu); int ok=1;
      for(int i=0;i<len;i++){ char *p=sqlite3_mprintf("$[%d]",i); char *as=pvtScalarNode(db,args,p,fields,prefix); sqlite3_free(p); if(!as){ok=0;break;} if(i) sqlite3_str_append(ab,", ",2); sqlite3_str_appendf(ab,"%s",as); sqlite3_free(as); }
      sqlite3_str_append(ab,")",1);
      if(ok) call=sqlite3_str_finish(ab); else sqlite3_free(sqlite3_str_finish(ab));
    }
    sqlite3_free(a0t); sqlite3_free(args);
  }else{
    call = sqlite3_mprintf("%s()", fu);
  }
  sqlite3_free(argst);
  if(!call){ sqlite3_free(func); return 0; }

  /* FILTER for aggregate windows */
  char *filt = sqlite3_mprintf("");
  if(is_agg){ char *ft=jsonTypeAt(db,node,"$.filter");
    if(ft){ sqlite3_free(ft); char *fn=jsonGetText(db,node,"$.filter"); char *be=fn?pvtBuildBoolExpr(db,fn,fields,prefix):0; sqlite3_free(fn);
      if(be){ sqlite3_free(filt); filt=sqlite3_mprintf(" FILTER (WHERE %s)", be); sqlite3_free(be); } } }

  char *over = pvtWindowOver(db, node, fields, prefix);
  char *res = sqlite3_mprintf("%s%s %s", call, filt, over);
  sqlite3_free(call); sqlite3_free(filt); sqlite3_free(over); sqlite3_free(func);
  return res;
}

/* compile one window-select entry -> "col" (window or plain). NULL on error. */
static char *pvtWindowSelectCol(sqlite3 *db, const char *entry, const char *fields, const char *prefix){
  char *xt = jsonTypeAt(db, entry, "$.\"$expr\"");
  if(xt && !strcmp(xt,"object")){
    char *ovt = jsonTypeAt(db, entry, "$.\"$expr\".\"$over\"");
    if(ovt){ sqlite3_free(ovt); sqlite3_free(xt);
      char *win = jsonGetText(db, entry, "$.\"$expr\".\"$over\"");
      char *r = win ? pvtBuildWindowExpr(db, win, fields, prefix) : 0; sqlite3_free(win); return r; }
  }
  sqlite3_free(xt);
  return pvtCompileOrderCol(db, entry, fields, prefix);
}

static char *pvtBuildWindowSql(sqlite3 *db, sqlite3_int64 scheme, const char *filter,
                               const char *select, const char *order, int has_limit, int limit,
                               int offset, const char *source_mode, const char *tree_ids,
                               int has_md, int md, int include_seed, int polymorphic){
  if(!source_mode) source_mode = "flat";
  int is_flat = !strcmp(source_mode,"flat");
  int no_tree = !tree_ids || !strcmp(tree_ids,"null");
  if(!select) return 0;
  char *fields = pvtCollectAll(db, scheme, filter, select, order, 0, 0);
  if(!fields) return 0;
  char *push=0,*resid=0; pvtSplitFilter(db, filter, fields, &push, &resid);
  const char *outer = resid;
  int has_prop = pvtHasPropFields(db, fields);
  int shapeA = (!outer && is_flat && no_tree && !has_prop);
  int force_outer = pvtHasCheck(db, outer, 1) || !strcmp(fields,"{}");
  int narrow = !shapeA && is_flat && !force_outer && !pvtHasNestedDict(db, fields);
  const char *prefix = shapeA ? "o." : (narrow ? "o." : "");

  char *cte = 0;
  if(!shapeA){ cte = pvtBuildCteSql(db, scheme, fields, source_mode, no_tree?0:tree_ids, has_md, md, force_outer, push, narrow, include_seed, polymorphic, 0);
    if(!cte){ sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid); return 0; } }

  /* select list */
  sqlite3_str *sel = sqlite3_str_new(db); int sidx=0, err=0;
  sqlite3_stmt *si = 0; sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1)",-1,&si,0);
  sqlite3_bind_text(si,1,select,-1,SQLITE_TRANSIENT);
  while(!err && sqlite3_step(si)==SQLITE_ROW){
    sidx++;
    char *e=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(si,0));
    char *col=pvtWindowSelectCol(db,e,fields,prefix);
    char *alias=jsonGetText(db,e,"$.alias"); if(!alias) alias=jsonGetText(db,e,"$.field"); if(!alias) alias=jsonGetText(db,e,"$.field_path");
    if(!col) err=1;
    else{ if(sidx>1) sqlite3_str_append(sel,", ",2);
      if(alias) sqlite3_str_appendf(sel,"%s AS \"%w\"", col, alias); else sqlite3_str_appendf(sel,"%s AS \"_sel_%d\"", col, sidx); }
    sqlite3_free(col); sqlite3_free(alias); sqlite3_free(e);
  }
  sqlite3_finalize(si);
  char *sel_sql = sqlite3_str_finish(sel);
  char *where_sql = err?0:pvtBuildWhereFromJson(db, outer, fields, prefix);
  char *order_sql = err?0:pvtBuildOrderConditions(db, order, fields, prefix);
  char *result = 0;
  if(!err && where_sql && order_sql){
    sqlite3_str *pg=sqlite3_str_new(db); if(has_limit&&limit>=0) sqlite3_str_appendf(pg,"\nLIMIT %d",limit); if(offset>0) sqlite3_str_appendf(pg,"\nOFFSET %d",offset);
    char *paging=sqlite3_str_finish(pg);
    const char *wc = strcmp(where_sql,"TRUE")==0 ? "" : where_sql;
    if(shapeA)
      result=sqlite3_mprintf("SELECT %s FROM _objects o\nWHERE o._id_scheme = %lld%s%s%s%s",
        sel_sql,(long long)scheme,(push&&*push)?" AND ":"",(push&&*push)?push:"",order_sql,paging);
    else if(narrow)
      result=sqlite3_mprintf("%s\nSELECT %s FROM _pvt_cte\nJOIN _objects o ON o._id = _pvt_cte._id_object%s%s%s%s",
        cte,sel_sql,*wc?"\nWHERE ":"",wc,order_sql,paging);
    else
      result=sqlite3_mprintf("%s\nSELECT %s FROM _pvt_cte%s%s%s%s",
        cte,sel_sql,*wc?"\nWHERE ":"",wc,order_sql,paging);
    sqlite3_free(paging);
  }
  sqlite3_free(cte); sqlite3_free(sel_sql); sqlite3_free(where_sql); sqlite3_free(order_sql);
  sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid);
  return result;
}

/* ------------------------------------------------------------------------- */
/* 24 — projection                                                           */
/* ------------------------------------------------------------------------- */

/* projection array -> "expr AS alias, ..." (NULL on error). */
static char *pvtBuildProjection(sqlite3 *db, const char *projection, const char *fields, const char *prefix){
  sqlite3_str *out = sqlite3_str_new(db); int idx=0, err=0;
  sqlite3_stmt *it=0; sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1)",-1,&it,0);
  sqlite3_bind_text(it,1,projection,-1,SQLITE_TRANSIENT);
  while(!err && sqlite3_step(it)==SQLITE_ROW){
    idx++;
    char *e=sqlite3_mprintf("%s",(const char*)sqlite3_column_text(it,0));
    /* normalize source to a scalar-expr node */
    char *expr_sql=0;
    char *xt=jsonTypeAt(db,e,"$.\"$expr\"");
    if(xt){ sqlite3_free(xt); char *ex=jsonGetText(db,e,"$.\"$expr\""); expr_sql=ex?pvtBuildScalarExpr(db,ex,fields,prefix):0; sqlite3_free(ex); }
    else{ char *f=jsonGetText(db,e,"$.field"); if(!f) f=jsonGetText(db,e,"$.field_path");
      if(f){ char *node=sqlite3_mprintf("{\"$field\":%Q}", f); expr_sql=pvtBuildScalarExpr(db,node,fields,prefix); sqlite3_free(node); } sqlite3_free(f); }
    char *alias=jsonGetText(db,e,"$.alias"); if(!alias) alias=jsonGetText(db,e,"$.field"); if(!alias) alias=jsonGetText(db,e,"$.field_path");
    if(!expr_sql) err=1;
    else{ if(idx>1) sqlite3_str_append(out,", ",2);
      if(alias) sqlite3_str_appendf(out,"%s AS \"%w\"",expr_sql,alias); else sqlite3_str_appendf(out,"%s AS \"_proj_%d\"",expr_sql,idx); }
    sqlite3_free(expr_sql); sqlite3_free(alias); sqlite3_free(e);
  }
  sqlite3_finalize(it);
  char *body=sqlite3_str_finish(out);
  if(err){ sqlite3_free(body); return 0; }
  return body;
}

static char *pvtBuildProjectionSql(sqlite3 *db, sqlite3_int64 scheme, const char *projection,
                                   const char *filter, int has_limit, int limit, int offset,
                                   const char *order, int has_md, int md, int distinct,
                                   const char *source_mode, const char *tree_ids,
                                   int include_seed, int polymorphic){
  if(!source_mode) source_mode = "flat";
  int is_flat = !strcmp(source_mode,"flat");
  int no_tree = !tree_ids || !strcmp(tree_ids,"null");
  if(!projection) return 0;
  char *fields = pvtCollectAll(db, scheme, filter, order, projection, 0, 0);
  if(!fields) return 0;
  char *push=0,*resid=0; pvtSplitFilter(db, filter, fields, &push, &resid);
  const char *outer = resid;
  int has_prop = pvtHasPropFields(db, fields);
  char *paging; { sqlite3_str *p=sqlite3_str_new(db); if(has_limit&&limit>=0) sqlite3_str_appendf(p,"\nLIMIT %d",limit); if(offset>0) sqlite3_str_appendf(p,"\nOFFSET %d",offset); paging=sqlite3_str_finish(p); }
  char *result = 0;

  if(!outer && is_flat && no_tree && !has_prop){
    char *order_sql=pvtBuildOrderConditions(db,order,fields,"o.");
    char *proj=pvtBuildProjection(db,projection,fields,"o.");
    if(proj) result=sqlite3_mprintf("SELECT %s%s FROM _objects o\nWHERE o._id_scheme = %lld%s%s%s%s",
      distinct?"DISTINCT ":"", proj, (long long)scheme, (push&&*push)?" AND ":"", (push&&*push)?push:"", order_sql, paging);
    sqlite3_free(order_sql); sqlite3_free(proj); goto done;
  }
  {
    int force_outer = pvtHasCheck(db, outer, 1) || !strcmp(fields,"{}");
    int narrow = is_flat && !force_outer && !pvtHasNestedDict(db, fields);
    const char *prefix = narrow ? "o." : "";
    char *cte=pvtBuildCteSql(db,scheme,fields,source_mode,no_tree?0:tree_ids,has_md,md,force_outer,push,narrow,include_seed,polymorphic,0);
    if(!cte) goto done;
    char *where_sql=pvtBuildWhereFromJson(db,outer,fields,prefix);
    char *order_sql=pvtBuildOrderConditions(db,order,fields,prefix);
    char *proj=pvtBuildProjection(db,projection,fields,prefix);
    if(where_sql && order_sql && proj){
      const char *wc = strcmp(where_sql,"TRUE")==0?"":where_sql;
      if(narrow) result=sqlite3_mprintf("%s\nSELECT %s%s FROM _pvt_cte\nJOIN _objects o ON o._id = _pvt_cte._id_object%s%s%s%s",
        cte, distinct?"DISTINCT ":"", proj, *wc?"\nWHERE ":"", wc, order_sql, paging);
      else result=sqlite3_mprintf("%s\nSELECT %s%s FROM _pvt_cte%s%s%s%s",
        cte, distinct?"DISTINCT ":"", proj, *wc?"\nWHERE ":"", wc, order_sql, paging);
    }
    sqlite3_free(cte); sqlite3_free(where_sql); sqlite3_free(order_sql); sqlite3_free(proj);
  }
done:
  sqlite3_free(fields); sqlite3_free(push); sqlite3_free(resid); sqlite3_free(paging);
  return result;
}

/* ------------------------------------------------------------------------- */
/* bindings                                                                  */
/* ------------------------------------------------------------------------- */

static void pvtBuildWindowSqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  #define WA(i) (argc > (i) && sqlite3_value_type(argv[i]) != SQLITE_NULL)
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *filter = WA(1) ? (const char*)sqlite3_value_text(argv[1]) : 0;
  const char *select = WA(2) ? (const char*)sqlite3_value_text(argv[2]) : 0;
  const char *order = WA(3) ? (const char*)sqlite3_value_text(argv[3]) : 0;
  int has_limit = WA(4); int limit = has_limit ? sqlite3_value_int(argv[4]) : 0;
  int offset = WA(5) ? sqlite3_value_int(argv[5]) : 0;
  const char *mode = WA(6) ? (const char*)sqlite3_value_text(argv[6]) : "flat";
  const char *tree = WA(7) ? (const char*)sqlite3_value_text(argv[7]) : 0;
  int has_md = WA(8); int md = has_md ? sqlite3_value_int(argv[8]) : 0;
  int seed = WA(9) ? sqlite3_value_int(argv[9]) : 1;
  int poly = WA(10) ? sqlite3_value_int(argv[10]) : 1;
  #undef WA
  char *s = pvtBuildWindowSql(db, scheme, filter, select, order, has_limit, limit, offset, mode, tree, has_md, md, seed, poly);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free); else sqlite3_result_error(ctx, "pvt_build_window_sql: unsupported", -1);
}

static void pvtBuildProjectionSqlFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  #define PA(i) (argc > (i) && sqlite3_value_type(argv[i]) != SQLITE_NULL)
  sqlite3_int64 scheme = sqlite3_value_int64(argv[0]);
  const char *projection = PA(1) ? (const char*)sqlite3_value_text(argv[1]) : 0;
  const char *filter = PA(2) ? (const char*)sqlite3_value_text(argv[2]) : 0;
  int has_limit = PA(3); int limit = has_limit ? sqlite3_value_int(argv[3]) : 0;
  int offset = PA(4) ? sqlite3_value_int(argv[4]) : 0;
  const char *order = PA(5) ? (const char*)sqlite3_value_text(argv[5]) : 0;
  int has_md = PA(6); int md = has_md ? sqlite3_value_int(argv[6]) : 0;
  int distinct = PA(7) ? sqlite3_value_int(argv[7]) : 0;
  const char *mode = PA(8) ? (const char*)sqlite3_value_text(argv[8]) : "flat";
  const char *tree = PA(9) ? (const char*)sqlite3_value_text(argv[9]) : 0;
  int seed = PA(10) ? sqlite3_value_int(argv[10]) : 1;
  int poly = PA(11) ? sqlite3_value_int(argv[11]) : 1;
  #undef PA
  char *s = pvtBuildProjectionSql(db, scheme, projection, filter, has_limit, limit, offset, order, has_md, md, distinct, mode, tree, seed, poly);
  if(s) sqlite3_result_text(ctx, s, -1, sqlite3_free); else sqlite3_result_error(ctx, "pvt_build_projection_sql: unsupported", -1);
}

int redbRegisterPvtAnalytics(sqlite3 *db){
  int rc;
  for(int a = 3; a <= 11; a++){
    rc = sqlite3_create_function(db, "pvt_build_window_sql", a, SQLITE_UTF8, 0, pvtBuildWindowSqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }
  for(int a = 2; a <= 12; a++){
    rc = sqlite3_create_function(db, "pvt_build_projection_sql", a, SQLITE_UTF8, 0, pvtBuildProjectionSqlFunc, 0, 0);
    if(rc != SQLITE_OK) return rc;
  }
  return SQLITE_OK;
}
