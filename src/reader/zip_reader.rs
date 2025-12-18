use std::fs::File;
use std::path::Path;

use serde::{Deserialize, Serialize};
use zip::ZipArchive;

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct ZipEntryInfo {
    pub name:              String,
    pub uncompressed_size: u64,
    pub last_modified:     String, // Formatted datetime string
}

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct ZipData {
    pub total_size:  u64,
    pub entry_count: usize,
    pub entries:     Vec<ZipEntryInfo,>,
}

pub fn read_zip_data(file_path: &Path,) -> Result<ZipData, DataReaderError,> {
    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;
    let total_size = file
        .metadata()
        .map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?
        .len();

    let mut archive = ZipArchive::new(file,).map_err(|e| DataReaderError::ParseError {
        path:   file_path.to_path_buf(),
        source: Box::new(e,),
    },)?;
    let entry_count = archive.len();

    let mut entries_info = Vec::new();
    for i in 0..archive.len() {
        let file = archive
            .by_index(i,)
            .map_err(|e| DataReaderError::ParseError {
                path:   file_path.to_path_buf(),
                source: Box::new(e,),
            },)?;

        let outpath = match file.enclosed_name() {
            Some(path,) => path.to_owned(),
            None => continue,
        };

        let datetime = file.last_modified();
        entries_info.push(ZipEntryInfo {
            name:              outpath.display().to_string(),
            uncompressed_size: file.size(),
            last_modified:     format!(
                "{}-{}-{} {}:{}:{}",
                datetime.year(),
                datetime.month(),
                datetime.day(),
                datetime.hour(),
                datetime.minute(),
                datetime.second()
            ),
        },);
    }

    Ok(ZipData {
        total_size,
        entry_count,
        entries: entries_info,
    },)
}
