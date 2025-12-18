use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf; // New import for PathBuf

use serde::{Deserialize, Serialize};

use crate::error::DataReaderError;
use crate::output::OutputFormat;
use crate::reader::csv_reader::CsvData;
use crate::reader::gzip_reader::GzipData;
use crate::reader::image_reader::ImageData;
use crate::reader::json_reader::JsonData;
use crate::reader::md_reader::MarkdownData;
use crate::reader::parquet_reader::{ParquetData, ParquetDataForAnalysis};
use crate::reader::pdf_reader::PdfData;
use crate::reader::spreadsheet_reader::SpreadsheetData;
use crate::reader::sqlite_reader::SqliteData;
use crate::reader::toml_reader::TomlData;
use crate::reader::txt_reader::TextData;
use crate::reader::xml_reader::XmlData;
use crate::reader::yaml_reader::YamlData;
use crate::reader::zip_reader::ZipData;

pub type RecordStream =
    Box<dyn Iterator<Item = Result<serde_json::Value, DataReaderError,>,> + Send,>;

#[derive(Debug, Serialize, Deserialize,)]
pub struct FileMetadata {
    pub size:       u64,
    pub line_count: Option<usize,>,
}

#[derive(Serialize, Deserialize,)]
#[serde(untagged)] // Use untagged enum for flexible deserialization
pub enum DataReaderResult {
    Csv(CsvData, FileMetadata,),
    Gzip(GzipData, FileMetadata,),
    Image(ImageData, FileMetadata,),
    Json(JsonData, FileMetadata,),
    Markdown(MarkdownData, FileMetadata,),
    Parquet(ParquetData, FileMetadata,),
    ParquetAnalysis(ParquetDataForAnalysis, FileMetadata,), /* New variant for detailed
                                                             * analysis data */
    Pdf(PdfData, FileMetadata,),
    Spreadsheet(SpreadsheetData, FileMetadata,),
    Sqlite(SqliteData, FileMetadata,),
    Toml(TomlData, FileMetadata,),
    Text(TextData, FileMetadata,),
    Xml(XmlData, FileMetadata,),
    Yaml(YamlData, FileMetadata,),
    Zip(ZipData, FileMetadata,),
    RawContent(String, FileMetadata,), // New variant for raw content
    #[serde(skip_serializing)] // Skip serialization of this variant directly
    DirectoryResults(Vec<(PathBuf, DataReaderResult,),>, FileMetadata,), // New variant
    #[serde(skip)]
    Stream(RecordStream, FileMetadata,),
}

impl fmt::Debug for DataReaderResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_,>,) -> fmt::Result {
        match self {
            DataReaderResult::Csv(d, m,) => f.debug_tuple("Csv",).field(d,).field(m,).finish(),
            DataReaderResult::Gzip(d, m,) => f.debug_tuple("Gzip",).field(d,).field(m,).finish(),
            DataReaderResult::Image(d, m,) => f.debug_tuple("Image",).field(d,).field(m,).finish(),
            DataReaderResult::Json(d, m,) => f.debug_tuple("Json",).field(d,).field(m,).finish(),
            DataReaderResult::Markdown(d, m,) => {
                f.debug_tuple("Markdown",).field(d,).field(m,).finish()
            },
            DataReaderResult::Parquet(d, m,) => {
                f.debug_tuple("Parquet",).field(d,).field(m,).finish()
            },
            DataReaderResult::ParquetAnalysis(d, m,) => f
                .debug_tuple("ParquetAnalysis",)
                .field(d,)
                .field(m,)
                .finish(),
            DataReaderResult::Pdf(d, m,) => f.debug_tuple("Pdf",).field(d,).field(m,).finish(),
            DataReaderResult::Spreadsheet(d, m,) => {
                f.debug_tuple("Spreadsheet",).field(d,).field(m,).finish()
            },
            DataReaderResult::Sqlite(d, m,) => {
                f.debug_tuple("Sqlite",).field(d,).field(m,).finish()
            },
            DataReaderResult::Toml(d, m,) => f.debug_tuple("Toml",).field(d,).field(m,).finish(),
            DataReaderResult::Text(d, m,) => f.debug_tuple("Text",).field(d,).field(m,).finish(),
            DataReaderResult::Xml(d, m,) => f.debug_tuple("Xml",).field(d,).field(m,).finish(),
            DataReaderResult::Yaml(d, m,) => f.debug_tuple("Yaml",).field(d,).field(m,).finish(),
            DataReaderResult::Zip(d, m,) => f.debug_tuple("Zip",).field(d,).field(m,).finish(),
            DataReaderResult::RawContent(d, m,) => {
                f.debug_tuple("RawContent",).field(d,).field(m,).finish()
            },
            DataReaderResult::DirectoryResults(d, m,) => f
                .debug_tuple("DirectoryResults",)
                .field(d,)
                .field(m,)
                .finish(),
            DataReaderResult::Stream(_, m,) => f
                .debug_tuple("Stream",)
                .field(&"<RecordStream>",)
                .field(m,)
                .finish(),
        }
    }
}

impl DataReaderResult {
    // This method will now take an OutputFormat to determine serialization
    pub fn to_string_formatted(&self, format: OutputFormat,) -> String {
        match format {
            OutputFormat::Json => match self {
                DataReaderResult::DirectoryResults(results, _metadata,) => {
                    let serialized_results: Vec<serde_json::Value> = results.iter().map(|(path, nc_result)| {
                            let result_value = match nc_result {
                                DataReaderResult::Json(json_data, _meta) => serde_json::to_value(&json_data.value).unwrap_or_else(|_| serde_json::json!({"error": "Failed to serialize inner json value"})),
                                _ => serde_json::to_value(nc_result).unwrap_or_else(|_| serde_json::json!({"error": "Failed to serialize inner result"})),
                            };
                            serde_json::json!({
                                "path": path.to_string_lossy(),
                                "result": result_value,
                            })
                        }).collect();
                    serde_json::to_string_pretty(&serialized_results,).unwrap_or_else(|e| {
                        format!("Error serializing directory results to JSON: {}", e)
                    },)
                },
                _ => serde_json::to_string_pretty(self,)
                    .unwrap_or_else(|e| format!("Error serializing to JSON: {}", e),),
            },
            OutputFormat::Yaml => match self {
                DataReaderResult::DirectoryResults(results, _metadata,) => {
                    let serialized_results: Vec<serde_yaml::Value,> = results
                        .iter()
                        .map(|(path, nc_result,)| {
                            let result_value =
                                serde_yaml::to_value(nc_result,).unwrap_or_else(|_| {
                                    serde_yaml::Value::String(
                                        "Failed to serialize inner result".to_string(),
                                    )
                                },);
                            let mut map = serde_yaml::Mapping::new();
                            map.insert(
                                serde_yaml::Value::String("path".to_string(),),
                                serde_yaml::Value::String(path.to_string_lossy().into_owned(),),
                            );
                            map.insert(
                                serde_yaml::Value::String("result".to_string(),),
                                result_value,
                            );
                            serde_yaml::Value::Mapping(map,)
                        },)
                        .collect();
                    serde_yaml::to_string(&serialized_results,).unwrap_or_else(|e| {
                        format!("Error serializing directory results to YAML: {}", e)
                    },)
                },
                _ => serde_yaml::to_string(self,)
                    .unwrap_or_else(|e| format!("Error serializing to YAML: {}", e),),
            },
            OutputFormat::Text => {
                match self {
                    DataReaderResult::RawContent(s, _metadata,) => s.clone(),
                    DataReaderResult::Text(text_data, _metadata,) => text_data.content.clone(), /* Handle TextData specifically */
                    DataReaderResult::DirectoryResults(results, _metadata,) => {
                        // For text output, iterate and print each result with its path
                        results
                            .iter()
                            .map(|(path, nc_result,)| {
                                format!(
                                    "---\n File: {} ---\n{}",
                                    path.display(),
                                    nc_result.to_string_formatted(OutputFormat::Text)
                                )
                            },)
                            .collect::<Vec<String,>>()
                            .join("\n\n",)
                    },
                    DataReaderResult::Parquet(parquet_data, _metadata,) => {
                        let mut output = String::new();
                        output.push_str("--- Parquet Data ---\n",);
                        output
                            .push_str(&format!("File Size: {} bytes\n", parquet_data.file_size,),);
                        output.push_str(&format!("Number of Rows: {}\n", parquet_data.num_rows,),);

                        output.push_str("\nColumn Schemas:\n",);
                        for schema in &parquet_data.column_schemas {
                            output.push_str(&format!(
                                "  - {}: Physical={}, Logical={:?}, Nullable={}, Encodings={:?}, \
                                 Compression={}\n",
                                schema.name,
                                schema.physical_type,
                                schema.logical_type,
                                schema.nullable,
                                schema.encodings,
                                schema.compression,
                            ),);
                        }

                        if let Some(sample_rows,) = &parquet_data.sample_rows {
                            if !sample_rows.is_empty() {
                                output.push_str("\nSample Rows:\n",);
                                // Collect all unique column names for header
                                let mut column_names: Vec<String,> = Vec::new();
                                for row in sample_rows {
                                    for col_name in row.0.keys() {
                                        if !column_names.contains(col_name,) {
                                            column_names.push(col_name.clone(),);
                                        }
                                    }
                                }
                                column_names.sort(); // Sort to ensure consistent column order

                                // Calculate max column widths
                                let mut column_widths: HashMap<String, usize,> = HashMap::new();
                                for col_name in &column_names {
                                    column_widths.insert(col_name.clone(), col_name.len(),); // Initialize with header width
                                }

                                for row in sample_rows {
                                    for (col_name, value,) in &row.0 {
                                        let current_max =
                                            column_widths.entry(col_name.clone(),).or_insert(0,);
                                        *current_max = (*current_max).max(value.len(),);
                                    }
                                }

                                // Add some padding to each column
                                let padding = 4;
                                for width in column_widths.values_mut() {
                                    *width += padding;
                                }

                                // Print header
                                output.push_str("  ",);
                                for col_name in &column_names {
                                    let width = *column_widths.get(col_name,).unwrap_or(&20,); // Default to 20 if not found
                                    output.push_str(&format!("{: <width$}", col_name,),);
                                }
                                output.push('\n',);
                                output.push_str("  ",);
                                for col_name in &column_names {
                                    let width = *column_widths.get(col_name,).unwrap_or(&20,);
                                    for _ in 0..width {
                                        output.push('-',);
                                    }
                                }
                                output.push('\n',);

                                // Print rows
                                for row in sample_rows {
                                    output.push_str("  ",);
                                    for col_name in &column_names {
                                        let width = *column_widths.get(col_name,).unwrap_or(&20,);
                                        let null_str = "NULL".to_string();
                                        let value = row.0.get(col_name,).unwrap_or(&null_str,);
                                        // Truncate value if it's longer than the column width
                                        let display_value = if value.len() > width {
                                            format!("{}...", &value[0..(width - 3)],)
                                        } else {
                                            value.clone()
                                        };
                                        output.push_str(&format!("{: <width$}", display_value,),);
                                    }
                                    output.push('\n',);
                                }
                            } else {
                                output.push_str("\nSample Rows: (No samples read)\n",);
                            }
                        } else {
                            output.push_str("\nSample Rows: (Not requested)\n",);
                        }
                        output
                    },
                    DataReaderResult::Stream(_, _metadata,) => {
                        "Stream data (cannot be displayed)".to_string()
                    },
                    _ => format!("{:?}", self),
                } // This closes the match self block
            }, // This closes the OutputFormat::Text arm
        }
    }
}

// Implement Display trait for DataReaderResult to allow direct printing
impl fmt::Display for DataReaderResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_,>,) -> fmt::Result {
        // By default, print as text. This might be used if `to_string_formatted` is not called.
        // It's a fallback and should ideally be controlled by the CLI's output_format.
        write!(f, "{}", self.to_string_formatted(OutputFormat::Text))
    }
}
