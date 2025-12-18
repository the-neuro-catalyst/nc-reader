use std::io::Read;
use std::path::Path;
use std::fs::File;

use serde::{Deserialize, Serialize}; // Added
use toml::Value;
use encoding_rs_io::DecodeReaderBytes;

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)] // Added
pub struct TomlData {
    pub value:       toml::Value,
    pub first_lines: Option<Vec<String,>,>,
}

pub fn read_toml_value(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<TomlData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,);

    let file = File::open(file_path).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;
    
    let mut decoder = DecodeReaderBytes::new(file);
    let mut content = String::new();
    decoder.read_to_string(&mut content).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;

    let value: Value = toml::from_str(&content,).map_err(|e| DataReaderError::ParseError {
        path:   file_path.to_path_buf(),
        source: Box::new(e,),
    },)?;

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

    Ok(TomlData {
        value, first_lines,
    },)
}

pub fn get_toml_raw_content(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<String, DataReaderError,> {
    let toml_data = read_toml_value(file_path, head,)?; // Use the new read_toml_value that returns TomlData

    serde_json::to_string_pretty(&toml_data.value,).map_err(|e| {
        DataReaderError::InternalError(format!("Failed to serialize TOML to JSON: {}", e),)
    },)
}
