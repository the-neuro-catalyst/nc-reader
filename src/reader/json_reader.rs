use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::DataReaderError;
use nc_schema::{DataType, merge_nc_types};
use crate::nc_reader_result::RecordStream;

#[derive(Debug, Serialize, Deserialize, PartialEq,)]
pub struct JsonSchema {
    pub nc_type: DataType,
    pub nullable:  bool,
}

#[derive(Debug, Serialize, Deserialize,)]
pub struct JsonData {
    pub value:           serde_json::Value,
    pub first_lines:     Option<Vec<String,>,>,
    pub inferred_schema: Option<JsonSchema,>,
    pub line_count:      Option<usize,>,
}

fn infer_json_nc_type(value: &serde_json::Value,) -> DataType {
    match value {
        serde_json::Value::Null => DataType::Null,
        serde_json::Value::Bool(_,) => DataType::Boolean,
        serde_json::Value::Number(n,) => {
            if n.is_i64() {
                DataType::Integer
            } else if n.is_f64() {
                DataType::Float
            } else {
                DataType::Number
            }
        },
        serde_json::Value::String(_,) => DataType::String,
        serde_json::Value::Array(arr,) => {
            if arr.is_empty() {
                DataType::Array(Box::new(DataType::Unknown,),)
            } else {
                let mut element_type = infer_json_nc_type(&arr[0],);
                for item in arr.iter().skip(1,) {
                    element_type = merge_nc_types(element_type, infer_json_nc_type(item,),);
                }
                DataType::Array(Box::new(element_type,),)
            }
        },
        serde_json::Value::Object(obj,) => {
            let mut properties = HashMap::new();
            for (key, val,) in obj {
                properties.insert(key.clone(), infer_json_nc_type(val,),);
            }
            DataType::Object(properties,)
        },
    }
}

fn infer_json_schema(value: &serde_json::Value,) -> JsonSchema {
    let nc_type = infer_json_nc_type(value,);
    let nullable = matches!(nc_type, DataType::Null)
        || if let DataType::Union(v,) = &nc_type {
            v.contains(&DataType::Null,)
        } else {
            false
        };

    JsonSchema {
        nc_type,
        nullable,
    }
}

fn merge_json_schemas(a: JsonSchema, b: JsonSchema,) -> JsonSchema {
    let merged_type = merge_nc_types(a.nc_type, b.nc_type,);
    let nullable = a.nullable
        || b.nullable
        || matches!(merged_type, DataType::Null)
        || if let DataType::Union(v,) = &merged_type {
            v.contains(&DataType::Null,)
        } else {
            false
        };

    JsonSchema {
        nc_type: merged_type,
        nullable,
    }
}

pub fn read_json_stream(
    file_path: &Path,
) -> Result<RecordStream, DataReaderError> {
    let is_jsonl = file_path.extension().is_some_and(|ext| ext == "jsonl");
    let file = File::open(file_path).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;
    let path_clone = file_path.to_path_buf();
    let decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;

    if is_jsonl {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(decoder);
        let stream = reader.lines().filter_map(move |line_res| {
             match line_res {
                 Ok(line) => {
                     let trimmed = line.trim();
                     if trimmed.is_empty() {
                         None
                     } else {
                         match serde_json::from_str::<Value>(trimmed) {
                             Ok(v) => Some(Ok(v)),
                             Err(e) => Some(Err(DataReaderError::ParseError {
                                 path: path_clone.clone(),
                                 source: Box::new(e),
                             })),
                         }
                     }
                 }
                 Err(e) => Some(Err(DataReaderError::FileReadError {
                     path: path_clone.clone(),
                     source: e,
                 })),
             }
        });
        Ok(Box::new(stream))
    } else {
        use std::io::BufReader;
        let reader = BufReader::new(decoder);
        let stream = serde_json::Deserializer::from_reader(reader)
            .into_iter::<Value>()
            .map(move |res| {
                res.map_err(|e| DataReaderError::ParseError {
                    path: path_clone.clone(),
                    source: Box::new(e),
                })
            });
        Ok(Box::new(stream))
    }
}

pub fn read_json_value(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<JsonData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,);
    let is_jsonl = file_path.extension().is_some_and(|ext| ext == "jsonl",);

    let stream = read_json_stream(file_path)?;
    let mut values = Vec::new();
    let mut inferred_schema: Option<JsonSchema,> = None;

    for value_result in stream {
        let value = value_result?;
        let current_schema = infer_json_schema(&value,);
        inferred_schema = match inferred_schema {
            Some(prev_schema,) => Some(merge_json_schemas(prev_schema, current_schema,),),
            None => Some(current_schema,),
        };
        values.push(value,);
    }

    let first_lines = if num_lines_to_extract > 0 {
        use std::io::{BufRead, BufReader};
        let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
        let decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
            path: file_path.to_path_buf(),
            source: e,
        })?;
        let reader = BufReader::new(decoder,);
        let lines: Vec<String,> = reader
            .lines()
            .take(num_lines_to_extract,)
            .filter_map(|l| l.ok(),)
            .collect();
        if !lines.is_empty() {
            Some(lines,)
        } else {
            None
        }
    } else {
        None
    };

    let final_value = if values.len() == 1 && !is_jsonl {
        values.into_iter().next().unwrap()
    } else {
        serde_json::Value::Array(values,)
    };

    let line_count = match &final_value {
        serde_json::Value::Array(arr,) => Some(arr.len(),),
        _ => None,
    };

    Ok(JsonData {
        value: final_value,
        first_lines,
        inferred_schema,
        line_count,
    },)
}

pub fn get_json_raw_content(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<String, DataReaderError,> {
    let json_data = read_json_value(file_path, head,)?;

    serde_json::to_string_pretty(&json_data.value,)
        .map_err(|e| DataReaderError::InternalError(format!("Failed to serialize JSON: {}", e),),)
}
