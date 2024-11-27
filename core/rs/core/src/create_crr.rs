use alloc::vec::Vec;
use core::ffi::c_char;
use sqlite_nostd as sqlite;
use sqlite_nostd::ResultCode;

use crate::bootstrap::create_clock_table;
use crate::tableinfo::{is_table_compatible, pull_table_info, TableInfo};
use crate::triggers::create_triggers;
use crate::{backfill_table, is_crr, remove_crr_triggers_if_exist};

/**
 * Create a new crr --
 * all triggers, views, tables
 */
pub fn create_crr(
    db: *mut sqlite::sqlite3,
    table_infos: &Vec<TableInfo>,
    _schema: &str,
    table: &str,
    is_commit_alter: bool,
    no_tx: bool,
    err: *mut *mut c_char,
) -> Result<ResultCode, ResultCode> {
    if !is_table_compatible(db, table, err)? {
        return Err(ResultCode::ERROR);
    }
    if is_crr(db, table)? {
        return Ok(ResultCode::OK);
    }

    // We do not / can not pull this from the cached set of table infos
    // since nothing would exist in it for a table not yet made into a crr.
    // TODO: Note: we can optimize out our `ensureTableInfosAreUpToDate` by mutating our ext data
    // when upgrading stuff to CRRs
    let table_info = pull_table_info(db, table, err)?;

    let curr_table_info = table_infos.iter().find(|t| t.tbl_name == table);

    if curr_table_info.is_none() {
        libc_print::libc_println!("creating clock table");
        create_clock_table(db, &table_info, err)?;
        libc_print::libc_println!("DONE creating clock table");
    } else {
        libc_print::libc_println!("removing triggers if exist");
        remove_crr_triggers_if_exist(db, table)?;
        libc_print::libc_println!("DONE removing triggers if exist");
    }

    let pks_changed = if let Some(curr_table_info) = curr_table_info {
        // PAINFUL, but cheap in comparison to what it prevents
        let curr_contains_all = curr_table_info.pks.iter().all(|curr_t| {
            table_info
                .pks
                .iter()
                .any(|t| curr_t.name == t.name && curr_t.cid == t.cid)
        });
        let new_contains_all = table_info.pks.iter().all(|new_t| {
            curr_table_info
                .pks
                .iter()
                .any(|t| new_t.name == t.name && new_t.cid == t.cid)
        });
        curr_contains_all && new_contains_all
    } else {
        true
    };

    if pks_changed {
        libc_print::libc_println!("creating triggers");
        create_triggers(db, &table_info, err)?;
        libc_print::libc_println!("DONE creating triggers");
    }

    libc_print::libc_println!("backfilling table");
    backfill_table(
        db,
        curr_table_info,
        table,
        &table_info.pks,
        &table_info.non_pks,
        is_commit_alter,
        pks_changed,
        no_tx,
    )?;
    libc_print::libc_println!("DONE backfilling table");

    Ok(ResultCode::OK)
}
