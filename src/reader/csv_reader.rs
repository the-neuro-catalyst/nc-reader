use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead}; // Added File and BufRead
use std::path::Path;

use nc_schema::{DataType, merge_nc_types};
use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::DataReaderError;
use crate::nc_reader_result::RecordStream;

#[derive(Debug, Serialize, Deserialize,)]
pub struct CsvData {
    pub file_size:       u64,
    pub num_rows:        u64,
    pub column_headers:  Vec<String,>,
    pub nc_rows:         Vec<serde_json::Value,>,
    pub total_size:      u64,
    pub first_lines:     Option<Vec<String,>,>,
    pub inferred_schema: Option<HashMap<String, DataType,>,>,
}

pub fn read_csv_stream(
    file_path: &Path,
) -> Result<(Vec<String,>, RecordStream,), DataReaderError,> {
    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;

    let decoder = crate::reader::charset::get_decoded_reader(file,).map_err(|e| {
        DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        }
    },)?;
    let mut rdr = csv::Reader::from_reader(decoder,);

    let headers = rdr
        .headers()
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?
        .iter()
        .map(|s| s.to_string(),)
        .collect::<Vec<String,>>();

    let headers_clone = headers.clone();
    let path_clone = file_path.to_path_buf();

    let stream = rdr.into_records().map(move |result| {
        let record = result.map_err(|e| DataReaderError::ParseError {
            path:   path_clone.clone(),
            source: Box::new(e,),
        },)?;

        let mut row_map = serde_json::Map::new();
        for (i, header,) in headers_clone.iter().enumerate() {
            let field_val = if let Some(field,) = record.get(i,) {
                if field.is_empty() {
                    serde_json::Value::Null
                } else if let Ok(i_val,) = field.parse::<i64>() {
                    serde_json::Value::Number(i_val.into(),)
                } else if let Ok(f_val,) = field.parse::<f64>() {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(f_val,)
                            .unwrap_or(serde_json::Number::from(0,),),
                    )
                } else if let Ok(b_val,) = field.parse::<bool>() {
                    serde_json::Value::Bool(b_val,)
                } else {
                    serde_json::Value::String(field.to_string(),)
                }
            } else {
                serde_json::Value::Null
            };
            row_map.insert(header.clone(), field_val,);
        }
        Ok(serde_json::Value::Object(row_map,),)
    },);

    Ok((headers, Box::new(stream,),),)
}

pub fn read_csv_data(file_path: &Path, head: Option<usize,>,) -> Result<CsvData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,); // Default to 0 if None

    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;
    let metadata = file
        .metadata()
        .map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
    let file_size = metadata.len();

    let first_lines: Option<Vec<String,>,> = if num_lines_to_extract > 0 {
        let cloned_file = file
            .try_clone()
            .map_err(|e| DataReaderError::FileReadError {
                path:   file_path.to_path_buf(),
                source: e,
            },)?;
        let decoder = crate::reader::charset::get_decoded_reader(cloned_file,).map_err(|e| {
            DataReaderError::FileReadError {
                path:   file_path.to_path_buf(),
                source: e,
            }
        },)?;
        let reader = io::BufReader::new(decoder,);
        Some(
            reader
                .lines()
                .take(num_lines_to_extract,)
                .filter_map(|l| l.ok(),)
                .collect(),
        )
    } else {
        None
    };

    let (headers, stream,) = read_csv_stream(file_path,)?;

    let mut records: Vec<serde_json::Value,> = Vec::new();
    let mut schema_map: HashMap<String, DataType,> = HashMap::new();

    for result in stream {
        let row = result?;

        if let serde_json::Value::Object(ref obj,) = row {
            for (header, value,) in obj {
                let current_type = match value {
                    serde_json::Value::Null => DataType::Null,
                    serde_json::Value::Bool(_,) => DataType::Boolean,
                    serde_json::Value::Number(n,) => {
                        if n.is_i64() {
                            DataType::Integer
                        } else {
                            DataType::Float
                        }
                    },
                    serde_json::Value::String(_,) => DataType::String,
                    _ => DataType::Unknown,
                };
                schema_map
                    .entry(header.clone(),)
                    .and_modify(|t| *t = merge_nc_types(t.clone(), current_type.clone(),),)
                    .or_insert(current_type,);
            }
        }

        records.push(row,);
    }

    let num_rows = records.len() as u64;

    Ok(CsvData {
        file_size,
        num_rows,
        column_headers: headers,
        nc_rows: records,
        total_size: file_size, // Now using actual file_size
        first_lines,
        inferred_schema: Some(schema_map,),
    },)
}

pub fn get_csv_raw_content(
    file_path: &Path,
    _head: Option<usize,>,
) -> Result<String, DataReaderError,> {
    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;

    let decoder = crate::reader::charset::get_decoded_reader(file,).map_err(|e| {
        DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        }
    },)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true,) // Assuming CSVs always have headers for this mode
        .from_reader(decoder,);

    let headers = rdr
        .headers()
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?
        .iter()
        .map(|s| s.to_string(),)
        .collect::<Vec<String,>>();

    let mut records: Vec<serde_json::Value,> = Vec::new();

    for result in rdr.into_records() {
        let record = result.map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?;
        let mut row_map = serde_json::Map::new();
        for (i, header,) in headers.iter().enumerate() {
            if let Some(value,) = record.get(i,) {
                row_map.insert(
                    header.clone(),
                    serde_json::Value::String(value.to_string(),),
                );
            } else {
                row_map.insert(header.clone(), serde_json::Value::Null,);
            }
        }
        records.push(serde_json::Value::Object(row_map,),);
    }

    serde_json::to_string_pretty(&records,).map_err(|e| {
        DataReaderError::InternalError(format!(
            "Failed to serialize CSV raw content to JSON: {}",
            e
        ),)
    },)
}
