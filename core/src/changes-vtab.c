#include "changes-vtab.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "changes-vtab-common.h"
#include "changes-vtab-read.h"
#include "changes-vtab-write.h"
#include "consts.h"
#include "crsqlite.h"
#include "ext-data.h"
#include "util.h"

/**
 * Created when the virtual table is initialized.
 * This happens when the vtab is first used in a given connection.
 * The method allocated the crsql_Changes_vtab for use for the duration
 * of the connection.
 */
static int changesConnect(sqlite3 *db, void *pAux, int argc,
                          const char *const *argv, sqlite3_vtab **ppVtab,
                          char **pzErr) {
  crsql_Changes_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(
      db,
      "CREATE TABLE x([table] TEXT NOT NULL, [pk] TEXT NOT NULL, [cid] TEXT "
      "NOT NULL, [val] ANY, [col_version] INTEGER NOT NULL, [db_version] "
      "INTEGER "
      "NOT NULL, [site_id] BLOB, [seq] HIDDEN INTEGER NOT NULL)");
  if (rc != SQLITE_OK) {
    *pzErr = sqlite3_mprintf("Could not define the table");
    return rc;
  }
  pNew = sqlite3_malloc(sizeof(*pNew));
  *ppVtab = (sqlite3_vtab *)pNew;
  if (pNew == 0) {
    *pzErr = sqlite3_mprintf("Out of memory");
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew));
  pNew->db = db;
  pNew->pExtData = (crsql_ExtData *)pAux;

  rc = crsql_ensureTableInfosAreUpToDate(db, pNew->pExtData,
                                         &(*ppVtab)->zErrMsg);
  if (rc != SQLITE_OK) {
    *pzErr = sqlite3_mprintf("Could not update table infos");
    sqlite3_free(pNew);
    return rc;
  }

  return rc;
}

/**
 * Called when the connection closes to free
 * all resources allocated by `changesConnect`
 *
 * I.e., free everything in `crsql_Changes_vtab` / `pVtab`
 */
static int changesDisconnect(sqlite3_vtab *pVtab) {
  crsql_Changes_vtab *p = (crsql_Changes_vtab *)pVtab;
  // ext data is free by other registered extensions
  sqlite3_free(p);
  return SQLITE_OK;
}

/**
 * Called to allocate a cursor for use in executing a query against
 * the virtual table.
 */
static int changesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
  crsql_Changes_cursor *pCur;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if (pCur == 0) {
    return SQLITE_NOMEM;
  }
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  pCur->pTab = (crsql_Changes_vtab *)p;
  return SQLITE_OK;
}

static int changesCrsrFinalize(crsql_Changes_cursor *crsr) {
  // Assign pointers to null after freeing
  // since we can get into this twice for the same cursor object.
  int rc = SQLITE_OK;
  rc += sqlite3_finalize(crsr->pChangesStmt);
  crsr->pChangesStmt = 0;
  rc += sqlite3_finalize(crsr->pRowStmt);
  crsr->pRowStmt = 0;

  crsr->dbVersion = MIN_POSSIBLE_DB_VERSION;

  return rc;
}

/**
 * Called to reclaim all of the resources allocated in `changesOpen`
 * once a query against the virtual table has completed.
 *
 * We, of course, do not de-allocated the `pTab` reference
 * given `pTab` must persist for the life of the connection.
 *
 * `pChangesStmt` and `pRowStmt` must be finalized.
 *
 * `colVrsns` does not need to be freed as it comes from
 * `pChangesStmt` thus finalizing `pChangesStmt` will
 * release `colVrsnsr`
 */
static int changesClose(sqlite3_vtab_cursor *cur) {
  crsql_Changes_cursor *pCur = (crsql_Changes_cursor *)cur;
  changesCrsrFinalize(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/**
 * Update to invoke slab algorithm to generate rowids
 */
static int changesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid) {
  crsql_Changes_cursor *pCur = (crsql_Changes_cursor *)cur;
  *pRowid = crsql_slabRowid(pCur->tblInfoIdx, pCur->changesRowid);
  return SQLITE_OK;
}

/**
 * Returns true if the cursor has been moved off the last row.
 * `pChangesStmt` is finalized and set to null when this is the case as we
 * finalize `pChangeStmt` in `changesNext` when it returns `SQLITE_DONE`
 */
static int changesEof(sqlite3_vtab_cursor *cur) {
  crsql_Changes_cursor *pCur = (crsql_Changes_cursor *)cur;
  return pCur->pChangesStmt == 0;
}

// char **crsql_extractValList() {

// }

/**
 * Advances our Changes_cursor to its next row of output.
 */
static int changesNext(sqlite3_vtab_cursor *cur) {
  crsql_Changes_cursor *pCur = (crsql_Changes_cursor *)cur;
  sqlite3_vtab *pTabBase = (sqlite3_vtab *)(pCur->pTab);
  int rc = SQLITE_OK;

  if (pCur->pChangesStmt == 0) {
    pTabBase->zErrMsg = sqlite3_mprintf(
        "crsql internal error: in an unexpected state. pChangesStmt is "
        "null.");
    return SQLITE_ERROR;
  }

  if (pCur->pRowStmt != 0) {
    // Finalize the prior row result
    // before getting the next row.
    // Do not re-use the statement since we could be entering
    // a new table.
    // An optimization would be to keep (rewind) it if we're processing the
    // same table for many rows.
    sqlite3_finalize(pCur->pRowStmt);
    pCur->pRowStmt = 0;
  }

  // step to next
  // if no row, tear down (finalize) statements
  // set statements to null
  rc = sqlite3_step(pCur->pChangesStmt);
  if (rc != SQLITE_ROW) {
    // tear down since we're done
    return changesCrsrFinalize(pCur);
  }

  // todo: pChangesStmt should also pull rowid from the underlying clock tbls
  const char *tbl = (const char *)sqlite3_column_text(pCur->pChangesStmt, TBL);
  const char *pks = (const char *)sqlite3_column_text(pCur->pChangesStmt, PKS);
  const char *cid = (const char *)sqlite3_column_text(pCur->pChangesStmt, CID);
  sqlite3_int64 dbVersion = sqlite3_column_int64(pCur->pChangesStmt, DB_VRSN);
  sqlite3_int64 changesRowid =
      sqlite3_column_int64(pCur->pChangesStmt, CHANGES_ROWID);
  pCur->dbVersion = dbVersion;

  // get information required to calculate rowid slabs.
  int tblInfoIndex =
      crsql_indexofTableInfo(pCur->pTab->pExtData->zpTableInfos,
                             pCur->pTab->pExtData->tableInfosLen, tbl);
  if (tblInfoIndex < 0) {
    pTabBase->zErrMsg = sqlite3_mprintf(
        "crsql internal error. Could not find schema for table %s", tbl);
    changesCrsrFinalize(pCur);
    return SQLITE_ERROR;
  }
  crsql_TableInfo *tblInfo = pCur->pTab->pExtData->zpTableInfos[tblInfoIndex];
  pCur->changesRowid = changesRowid;
  pCur->tblInfoIdx = tblInfoIndex;

  if (tblInfo->pksLen == 0) {
    crsql_freeTableInfo(tblInfo);
    pTabBase->zErrMsg = sqlite3_mprintf(
        "crr table %s is missing primary key columns", tblInfo->tblName);
    return SQLITE_ERROR;
  }

  if (strcmp(DELETE_CID_SENTINEL, cid) == 0) {
    pCur->rowType = ROW_TYPE_DELETE;
    return SQLITE_OK;
  } else if (strcmp(PKS_ONLY_CID_SENTINEL, cid) == 0) {
    pCur->rowType = ROW_TYPE_PKONLY;
    return SQLITE_OK;
  } else {
    pCur->rowType = ROW_TYPE_UPDATE;
  }

  char *zSql = crsql_rowPatchDataQuery(pCur->pTab->db, tblInfo, cid, pks);
  if (zSql == 0) {
    pTabBase->zErrMsg = sqlite3_mprintf(
        "crsql internal error generationg raw data fetch query for table "
        "%s",
        tbl);
    return SQLITE_ERROR;
  }

  sqlite3_stmt *pRowStmt;
  rc = sqlite3_prepare_v2(pCur->pTab->db, zSql, -1, &pRowStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    pTabBase->zErrMsg = sqlite3_mprintf(
        "crsql internal error preparing row data fetch statement");
    sqlite3_finalize(pRowStmt);
    return rc;
  }

  rc = sqlite3_step(pRowStmt);
  if (rc != SQLITE_ROW) {
    sqlite3_finalize(pRowStmt);
    // getting 0 rows for something we have clock entries for is not an
    // error it could just be the case that the thing was deleted so we have
    // nothing to retrieve to fill in values for do we re-write cids in this
    // case?
    if (rc == SQLITE_DONE) {
      return SQLITE_OK;
    }
    pTabBase->zErrMsg =
        sqlite3_mprintf("crsql internal error fetching row data");
    return SQLITE_ERROR;
  } else {
    rc = SQLITE_OK;
  }

  pCur->pRowStmt = pRowStmt;

  return rc;
}

/**
 * Returns volums for the row at which
 * the cursor currently resides.
 */
static int changesColumn(
    sqlite3_vtab_cursor *cur, /* The cursor */
    sqlite3_context *ctx,     /* First argument to sqlite3_result_...() */
    int i                     /* Which column to return */
) {
  crsql_Changes_cursor *pCur = (crsql_Changes_cursor *)cur;
  switch (i) {
      // we clean up the cursor on moving to the next result
      // so no need to tell sqlite to free these values.
    case CHANGES_SINCE_VTAB_TBL:
      sqlite3_result_value(ctx, sqlite3_column_value(pCur->pChangesStmt, TBL));
      break;
    case CHANGES_SINCE_VTAB_PK:
      sqlite3_result_value(ctx, sqlite3_column_value(pCur->pChangesStmt, PKS));
      break;
    case CHANGES_SINCE_VTAB_CVAL:
      // pRowStmt is null if the event was a delete. i.e., there is no row
      // data.
      // TODO: there's an edge case here where we can end up replicating a
      // bunch of nulls for a row that is deleted but has prior events
      // proceeding the delete. So on row delete we should, in our delete
      // trigger, go drop all state records for the row except the delete
      // event. "all" is actually quite small given we only keep max 1
      // record per col in a row. so this drop is feasible on delete.
      if (pCur->pRowStmt == 0) {
        sqlite3_result_null(ctx);
      } else {
        sqlite3_result_value(ctx, sqlite3_column_value(pCur->pRowStmt, 0));
      }
      break;
    case CHANGES_SINCE_VTAB_CID:
      if (pCur->rowType == ROW_TYPE_PKONLY) {
        sqlite3_result_text(ctx, PKS_ONLY_CID_SENTINEL, -1, SQLITE_STATIC);
      } else if (pCur->rowType == ROW_TYPE_DELETE || pCur->pRowStmt == 0) {
        sqlite3_result_text(ctx, DELETE_CID_SENTINEL, -1, SQLITE_STATIC);
      } else {
        sqlite3_result_value(ctx,
                             sqlite3_column_value(pCur->pChangesStmt, CID));
      }
      break;
    case CHANGES_SINCE_VTAB_COL_VRSN:
      sqlite3_result_value(ctx,
                           sqlite3_column_value(pCur->pChangesStmt, COL_VRSN));
      break;
    case CHANGES_SINCE_VTAB_DB_VRSN:
      sqlite3_result_int64(ctx, pCur->dbVersion);
      break;
    case CHANGES_SINCE_VTAB_SITE_ID:
      if (sqlite3_column_type(pCur->pChangesStmt, SITE_ID) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        // sqlite3_result_blob(ctx, pCur->pTab->pExtData->siteId, SITE_ID_LEN,
        //                     SQLITE_STATIC);
      } else {
        sqlite3_result_value(ctx,
                             sqlite3_column_value(pCur->pChangesStmt, SITE_ID));
      }
      break;
    case CHANGES_SINCE_VTAB_SEQ:
      sqlite3_result_value(ctx, sqlite3_column_value(pCur->pChangesStmt, SEQ));
      break;
    default:
      return SQLITE_ERROR;
  }
  // sqlite3_result_value(ctx, sqlite3_column_value(pCur->pRowStmt, i));
  return SQLITE_OK;
}

/**
 * Invoked to kick off the pulling of rows from the virtual table.
 * Provides the constraints with which the vtab can work with
 * to compute what rows to pull.
 *
 * Provided constraints are filled in by the changesBestIndex method.
 */
static int changesFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                         const char *idxStr, int argc, sqlite3_value **argv) {
  int rc = SQLITE_OK;
  crsql_Changes_cursor *pCrsr = (crsql_Changes_cursor *)pVtabCursor;
  crsql_Changes_vtab *pTab = pCrsr->pTab;
  sqlite3_vtab *pTabBase = (sqlite3_vtab *)pTab;
  sqlite3 *db = pTab->db;

  // This should never happen. pChangesStmt should be finalized
  // before filter is ever invoked.
  if (pCrsr->pChangesStmt) {
    sqlite3_finalize(pCrsr->pChangesStmt);
    pCrsr->pChangesStmt = 0;
  }

  // construct and prepare our union for fetching changes
  rc = crsql_ensureTableInfosAreUpToDate(db, pTab->pExtData,
                                         &(pTabBase->zErrMsg));
  if (rc != SQLITE_OK) {
    return rc;
  }

  // nothing to fetch, no crrs exist.
  if (pTab->pExtData->tableInfosLen == 0) {
    return SQLITE_OK;
  }

  char *zSql = crsql_changesUnionQuery(pTab->pExtData->zpTableInfos,
                                       pTab->pExtData->tableInfosLen, idxStr);

  if (zSql == 0) {
    pTabBase->zErrMsg = sqlite3_mprintf(
        "crsql internal error generating the query to extract changes.");
    return SQLITE_ERROR;
  }

  sqlite3_stmt *pStmt = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if (rc != SQLITE_OK) {
    pTabBase->zErrMsg = sqlite3_mprintf(
        "error preparing stmt to extract changes %s", sqlite3_errmsg(db));
    sqlite3_finalize(pStmt);
    return rc;
  }

  // now bind the params.
  // for each arg, bind.
  for (int i = 0; i < argc; ++i) {
    rc = sqlite3_bind_value(pStmt, i + 1, argv[i]);
    if (rc != SQLITE_OK) {
      pTabBase->zErrMsg = sqlite3_mprintf(
          "error binding params to the statement to extract "
          "changes.");
      sqlite3_finalize(pStmt);
      return rc;
    }
  }

  pCrsr->pChangesStmt = pStmt;
  return changesNext((sqlite3_vtab_cursor *)pCrsr);
}

static const char *getOperatorString(unsigned char op) {
  // SQLITE_INDEX_CONSTRAINT_NE
  switch (op) {
    case SQLITE_INDEX_CONSTRAINT_EQ:
      return "=";
    case SQLITE_INDEX_CONSTRAINT_GT:
      return ">";
    case SQLITE_INDEX_CONSTRAINT_LE:
      return "<=";
    case SQLITE_INDEX_CONSTRAINT_LT:
      return "<";
    case SQLITE_INDEX_CONSTRAINT_GE:
      return ">=";
    case SQLITE_INDEX_CONSTRAINT_MATCH:
      return "MATCH";
    case SQLITE_INDEX_CONSTRAINT_LIKE:
      return "LIKE";
    case SQLITE_INDEX_CONSTRAINT_GLOB:
      return "GLOB";
    case SQLITE_INDEX_CONSTRAINT_REGEXP:
      return "REGEXP";
    case SQLITE_INDEX_CONSTRAINT_NE:
      return "!=";
    case SQLITE_INDEX_CONSTRAINT_ISNOT:
      return "IS NOT";
    case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
      return "IS NOT NULL";
    case SQLITE_INDEX_CONSTRAINT_ISNULL:
      return "IS NULL";
    case SQLITE_INDEX_CONSTRAINT_IS:
      return "IS";
    default:
      return 0;
  }
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
** TODO: should we support `where table` filters?
*/
static int changesBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo) {
  int idxNum = 0;

  crsql_Changes_vtab *crsqlTab = (crsql_Changes_vtab *)tab;
  sqlite3_str *pStr = sqlite3_str_new(crsqlTab->db);

  int firstConstaint = 1;
  char *colName = 0;
  int argvIndex = 1;
  for (int i = 0; i < pIdxInfo->nConstraint; i++) {
    const struct sqlite3_index_constraint *pConstraint =
        &pIdxInfo->aConstraint[i];
    if (pConstraint->usable == 0) {
      continue;
    }
    switch (pConstraint->iColumn) {
      case CHANGES_SINCE_VTAB_TBL:
        // TODO: stick tbl constraint into pTab?
        // to read out later?
        break;
      case CHANGES_SINCE_VTAB_PK:
        // TODO: bind param it? o wait, it would need splitting.
        // the clock table has pks split out.
        break;
      case CHANGES_SINCE_VTAB_CID:
        colName = "cid";
        break;
      case CHANGES_SINCE_VTAB_CVAL:
        break;
      case CHANGES_SINCE_VTAB_COL_VRSN:
        colName = "col_vrsn";
        break;
      case CHANGES_SINCE_VTAB_DB_VRSN:
        colName = "db_vrsn";
        break;
      case CHANGES_SINCE_VTAB_SITE_ID:
        colName = "site_id";
        break;
    }

    if (colName != 0) {
      const char *opString = getOperatorString(pConstraint->op);
      if (opString == 0) {
        continue;
      }
      if (firstConstaint) {
        firstConstaint = 0;
      } else {
        sqlite3_str_appendall(pStr, " AND ");
      }

      if (pConstraint->op == SQLITE_INDEX_CONSTRAINT_ISNOTNULL ||
          pConstraint->op == SQLITE_INDEX_CONSTRAINT_ISNULL) {
        sqlite3_str_appendf(pStr, "%s %s", colName, opString);
        pIdxInfo->aConstraintUsage[i].argvIndex = 0;
        pIdxInfo->aConstraintUsage[i].omit = 1;
      } else {
        sqlite3_str_appendf(pStr, "%s %s ?", colName, opString);
        pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex;
        pIdxInfo->aConstraintUsage[i].omit = 1;
        argvIndex += 1;
      }
      colName = 0;
    }

    switch (pConstraint->iColumn) {
      case CHANGES_SINCE_VTAB_DB_VRSN:
        idxNum |= 2;
        break;
      case CHANGES_SINCE_VTAB_SITE_ID:
        idxNum |= 4;
        break;
    }
  }

  // both constraints are present
  if ((idxNum & 6) == 6) {
    pIdxInfo->estimatedCost = (double)1;
    pIdxInfo->estimatedRows = 1;
  }
  // only the version constraint is present
  else if ((idxNum & 2) == 2) {
    pIdxInfo->estimatedCost = (double)10;
    pIdxInfo->estimatedRows = 10;
  }
  // only the requestor constraint is present
  else if ((idxNum & 4) == 4) {
    pIdxInfo->estimatedCost = (double)2147483647;
    pIdxInfo->estimatedRows = 2147483647;
  }
  // no constraints are present
  else {
    pIdxInfo->estimatedCost = (double)2147483647;
    pIdxInfo->estimatedRows = 2147483647;
  }

  pIdxInfo->idxNum = idxNum;
  pIdxInfo->idxStr = sqlite3_str_finish(pStr);
  pIdxInfo->needToFreeIdxStr = 1;
  return SQLITE_OK;
}

static int changesApply(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv,
                        sqlite3_int64 *pRowid) {
  int argv0Type = sqlite3_value_type(argv[0]);
  char *errmsg = 0;
  int rc = SQLITE_OK;
  // if (argc == 1 && argv[0] != 0)
  // {
  //   // delete statement
  //   return crsql_mergeDelete();
  // }
  if (argc > 1 && argv0Type == SQLITE_NULL) {
    // insert statement
    // argv[1] is the rowid.. but why would it ever be filled for us?
    rc = crsql_mergeInsert(pVTab, argc, argv, pRowid, &errmsg);
    if (rc != SQLITE_OK) {
      pVTab->zErrMsg = errmsg;
    }
    return rc;
  } else {
    pVTab->zErrMsg = sqlite3_mprintf(
        "Only INSERT and SELECT statements are allowed against the crsql "
        "changes table.");
    return SQLITE_MISUSE;
  }

  return SQLITE_OK;
}

// If xBegin is not defined xCommit is not called.
static int xBegin(sqlite3_vtab *pVTab) { return SQLITE_OK; }

static int xCommit(sqlite3_vtab *pVTab) {
  crsql_Changes_vtab *crsqlTab = (crsql_Changes_vtab *)pVTab;
  crsqlTab->pExtData->rowsImpacted = 0;
  return SQLITE_OK;
}

sqlite3_module crsql_changesModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ changesConnect,
    /* xBestIndex  */ changesBestIndex,
    /* xDisconnect */ changesDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ changesOpen,
    /* xClose      */ changesClose,
    /* xFilter     */ changesFilter,
    /* xNext       */ changesNext,
    /* xEof        */ changesEof,
    /* xColumn     */ changesColumn,
    /* xRowid      */ changesRowid,
    /* xUpdate     */ changesApply,
    /* xBegin      */ xBegin,
    /* xSync       */ 0,
    /* xCommit     */ xCommit,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0};
