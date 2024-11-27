extern crate alloc;
use alloc::format;
use sqlite::Connection;

use core::ffi::c_char;

use sqlite::{sqlite3, ResultCode};
use sqlite_nostd as sqlite;

use crate::tableinfo::TableInfo;

pub fn recreate_update_triggers(
    db: *mut sqlite3,
    table_info: &TableInfo,
    err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    db.exec_safe(&format!(
        "DROP TRIGGER IF EXISTS \"{table}__crsql_utrig\"",
        table = crate::util::escape_ident(&table_info.tbl_name)
    ))?;

    // get all columns of table
    // iterate pk cols
    // drop triggers against those pk cols
    let stmt = db.prepare_v2("SELECT name FROM pragma_table_info(?) WHERE pk > 0")?;
    stmt.bind_text(1, &table_info.tbl_name, sqlite::Destructor::STATIC)?;
    while stmt.step()? == ResultCode::ROW {
        let col_name = stmt.column_text(0)?;
        db.exec_safe(&format!(
            "DROP TRIGGER IF EXISTS \"{tbl_name}_{col_name}__crsql_utrig\"",
            tbl_name = crate::util::escape_ident(&table_info.tbl_name),
            col_name = crate::util::escape_ident(col_name),
        ))?;
    }

    create_update_trigger(db, table_info, err)
}

pub fn create_triggers(
    db: *mut sqlite3,
    table_info: &TableInfo,
    err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    create_insert_trigger(db, table_info, err)?;
    create_update_trigger(db, table_info, err)?;
    create_delete_trigger(db, table_info, err)
}

fn create_insert_trigger(
    db: *mut sqlite3,
    table_info: &TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let create_trigger_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table_name}__crsql_itrig\"
      AFTER INSERT ON \"{table_name}\" WHEN crsql_internal_sync_bit() = 0
      BEGIN
        VALUES (crsql_after_insert('{table_name}', {pk_new_list}));
      END;",
        table_name = crate::util::escape_ident_as_value(&table_info.tbl_name),
        pk_new_list = crate::util::as_identifier_list(&table_info.pks, Some("NEW."))?
    );

    db.exec_safe(&create_trigger_sql)
}

fn create_update_trigger(
    db: *mut sqlite3,
    table_info: &TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let table_name = &table_info.tbl_name;
    let pk_columns = &table_info.pks;
    let non_pk_columns = &table_info.non_pks;
    let pk_new_list = crate::util::as_identifier_list(pk_columns, Some("NEW."))?;
    let pk_old_list = crate::util::as_identifier_list(pk_columns, Some("OLD."))?;

    let trigger_body = if non_pk_columns.is_empty() {
        format!(
            "VALUES (crsql_after_update('{table_name}', {pk_new_list}, {pk_old_list}))",
            table_name = crate::util::escape_ident_as_value(table_name),
            pk_new_list = pk_new_list,
            pk_old_list = pk_old_list,
        )
    } else {
        format!(
        "VALUES (crsql_after_update('{table_name}', {pk_new_list}, {pk_old_list}, {non_pk_new_list}, {non_pk_old_list}))",
        table_name = crate::util::escape_ident_as_value(table_name),
        pk_new_list = pk_new_list,
        pk_old_list = pk_old_list,
        non_pk_new_list = crate::util::as_identifier_list(non_pk_columns, Some("NEW."))?,
        non_pk_old_list = crate::util::as_identifier_list(non_pk_columns, Some("OLD."))?
      )
    };
    db.exec_safe(&format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table_name}__crsql_utrig\"
      AFTER UPDATE ON \"{table_name}\" WHEN crsql_internal_sync_bit() = 0
      BEGIN
        {trigger_body};
      END;",
        table_name = crate::util::escape_ident(table_name),
    ))
}

fn create_delete_trigger(
    db: *mut sqlite3,
    table_info: &TableInfo,
    _err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    let table_name = &table_info.tbl_name;
    let pk_columns = &table_info.pks;
    let pk_old_list = crate::util::as_identifier_list(pk_columns, Some("OLD."))?;

    let create_trigger_sql = format!(
        "CREATE TRIGGER IF NOT EXISTS \"{table_name}__crsql_dtrig\"
    AFTER DELETE ON \"{table_name}\" WHEN crsql_internal_sync_bit() = 0
    BEGIN
      VALUES (crsql_after_delete('{table_name}', {pk_old_list}));
    END;",
        table_name = crate::util::escape_ident(table_name),
        pk_old_list = pk_old_list
    );

    db.exec_safe(&create_trigger_sql)
}
