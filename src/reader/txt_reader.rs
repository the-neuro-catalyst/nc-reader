use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct TextData {
    pub content:     String,
    pub first_lines: Option<Vec<String,>,>,
    pub line_count:  usize,
    pub total_size:  u64, // In bytes
}

pub fn read_txt_content(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<TextData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,);

    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;

    let mut decoder = crate::reader::charset::get_decoded_reader(file,).map_err(|e| {
        DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        }
    },)?;
    let mut content = String::new();
    decoder
        .read_to_string(&mut content,)
        .map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;

    let total_size = std::fs::metadata(file_path,)
        .map(|m| m.len(),)
        .unwrap_or(content.len() as u64,);
    let line_count = content.lines().count();

    let first_lines: Option<Vec<String,>,> = if num_lines_to_extract > 0 {
        let lines: Vec<String,> = content
            .lines()
            .take(num_lines_to_extract,)
            .map(|s: &str| s.to_string(),)
            .collect();
        Some(lines,)
    } else {
        None
    };

    Ok(TextData {
        content,
        first_lines,
        line_count,
        total_size,
    },)
}
