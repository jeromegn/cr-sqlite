#ifndef CHANGES_VTAB_READ_H
#define CHANGES_VTAB_READ_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "changes-vtab-common.h"
#include "tableinfo.h"

char *crsql_changesQueryForTable(crsql_TableInfo *tableInfo,
                                 const char *idxStr);

#define TBL 0
#define PKS 1
#define CID 2
#define COL_VRSN 3
#define DB_VRSN 4
#define SITE_ID 5
#define CHANGES_ROWID 6
#define SEQ 7

char *crsql_changesUnionQuery(crsql_TableInfo **tableInfos, int tableInfosLen,
                              const char *idxStr);
char *crsql_rowPatchDataQuery(sqlite3 *db, crsql_TableInfo *tblInfo,
                              const char *colName, const char *pks);

#endif