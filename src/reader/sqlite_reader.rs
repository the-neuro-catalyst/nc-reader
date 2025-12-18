use std::fs;
use std::path::Path;

use rusqlite::{Connection, Result};
use serde::{Deserialize, Serialize};

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct SqliteColumnInfo {
    pub name:   String,
    pub c_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct SqliteTableInfo {
    pub name:      String,
    pub schema:    Option<Vec<SqliteColumnInfo,>,>,
    pub row_count: Option<i64,>,
}

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct SqliteData {
    pub total_size: u64,
    pub tables:     Vec<SqliteTableInfo,>,
}

pub fn read_sqlite_data(file_path: &Path,) -> Result<SqliteData, DataReaderError,> {
    let total_size = fs::metadata(file_path,)
        .map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?
        .len();

    let conn = Connection::open(file_path,).map_err(|e| DataReaderError::ParseError {
        path:   file_path.to_path_buf(),
        source: Box::new(e,),
    },)?;

    let mut tables_info = Vec::new();

    // Get list of tables
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table'",)
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?;
    let table_names: Vec<String,> = stmt
        .query_map([], |row| row.get(0,),)
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?
        .filter_map(|r| r.ok(),)
        .collect();

    if !table_names.is_empty() {
        for table_name in table_names {
            let mut table_info = SqliteTableInfo {
                name:      table_name.clone(),
                schema:    None,
                row_count: None,
            };

            // Get table schema
            let mut schema_stmt = conn
                .prepare(&format!("PRAGMA table_info('{}')", table_name),)
                .map_err(|e| DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                },)?;
            let column_info: Vec<SqliteColumnInfo,> = schema_stmt
                .query_map([], |row| {
                    Ok(SqliteColumnInfo {
                        name:   row.get(1,)?,
                        c_type: row.get(2,)?,
                    },)
                },)
                .map_err(|e| DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                },)?
                .filter_map(|r| r.ok(),)
                .collect();
            table_info.schema = Some(column_info,);

            // Get row count
            let mut count_stmt = conn
                .prepare(&format!("SELECT COUNT(*) FROM '{}'", table_name),)
                .map_err(|e| DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                },)?;
            let row_count: i64 = count_stmt.query_row([], |row| row.get(0,),).map_err(|e| {
                DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                }
            },)?;
            table_info.row_count = Some(row_count,);

            tables_info.push(table_info,);
        }
    }

    Ok(SqliteData {
        total_size,
        tables: tables_info,
    },)
}
