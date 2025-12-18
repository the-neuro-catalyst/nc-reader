use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use tracing::error;
use walkdir::WalkDir;

use crate::error::DataReaderError;
use crate::nc_reader_result::{DataReaderResult, FileMetadata};
use crate::output::{OutputFormat, OutputMode};

#[derive(Debug, PartialEq, Clone,)]
pub enum FileFormat {
    Csv,
    Gzip,
    Image,
    Json,
    Markdown,
    Parquet,
    Pdf,
    Spreadsheet,
    Sqlite,
    Toml,
    Text,
    Xml,
    Yaml,
    Zip,
    Unknown,
}

fn detect_format_from_magic_bytes(file_path: &Path,) -> Option<FileFormat,> {
    let mut file = match File::open(file_path,) {
        Ok(file,) => file,
        Err(_,) => return None,
    };

    let mut buffer = [0; 8];
    if file.read(&mut buffer,).is_err() {
        return None;
    }

    if buffer.starts_with(b"PAR1",) {
        return Some(FileFormat::Parquet,);
    }
    if buffer.starts_with(b"PK\x03\x04",) {
        return Some(FileFormat::Zip,);
    }
    if buffer.starts_with(b"\x1f\x8b",) {
        return Some(FileFormat::Gzip,);
    }
    if buffer.starts_with(b"%PDF",) {
        return Some(FileFormat::Pdf,);
    }
    if buffer.starts_with(b"\x89PNG\r\n\x1a\n",) {
        return Some(FileFormat::Image,);
    }
    if buffer.starts_with(b"\xff\xd8\xff",) {
        return Some(FileFormat::Image,);
    }

    None
}

pub fn get_file_format(file_path: &Path,) -> FileFormat {
    match file_path.extension().and_then(|s| s.to_str(),) {
        Some("xlsx",) | Some("xls",) | Some("ods",) => return FileFormat::Spreadsheet,
        Some("csv",) => return FileFormat::Csv,
        Some("json",) | Some("jsonl",) => return FileFormat::Json,
        Some("md",) => return FileFormat::Markdown,
        Some("parquet",) => return FileFormat::Parquet,
        Some("pdf",) => return FileFormat::Pdf,
        Some("sqlite",) | Some("db",) => return FileFormat::Sqlite,
        Some("toml",) => return FileFormat::Toml,
        Some("txt",) => return FileFormat::Text,
        Some("xml",) => return FileFormat::Xml,
        Some("yaml",) | Some("yml",) => return FileFormat::Yaml,
        Some("zip",) => return FileFormat::Zip,
        Some("gz",) => return FileFormat::Gzip,
        Some("jpg",) | Some("jpeg",) | Some("png",) | Some("gif",) | Some("bmp",)
        | Some("webp",) | Some("svg",) => return FileFormat::Image,
        _ => {},
    }

    if let Some(format,) = detect_format_from_magic_bytes(file_path,) {
        return format;
    }

    FileFormat::Unknown
}

#[derive(Clone,)]
pub struct FileReaderOptions {
    pub head:               Option<usize,>,
    pub file_type_override: Option<String,>,
    pub output_mode:        OutputMode,
    pub output_format:      OutputFormat,
    pub recursive:          bool,
    pub filter_exts:        Option<Vec<String,>,>,
    pub output_path:        Option<PathBuf,>,
}

pub fn read_file_to_data(
    file_path: &Path,
    head: Option<usize,>,
    file_format: FileFormat,
) -> Result<DataReaderResult, DataReaderError,> {
    let base_metadata =
        std::fs::metadata(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
    let file_size = base_metadata.len();

    match file_format {
        FileFormat::Csv => crate::reader::csv_reader::read_csv_data(file_path, head,).map(|data| {
            DataReaderResult::Csv(
                data,
                FileMetadata {
                    size:       file_size,
                    line_count: None,
                },
            )
        },),
        FileFormat::Gzip => crate::reader::gzip_reader::read_gzip_data(file_path,).map(|data| {
            DataReaderResult::Gzip(
                data,
                FileMetadata {
                    size:       file_size,
                    line_count: None,
                },
            )
        },),
        FileFormat::Image => crate::reader::image_reader::read_image_data(file_path,).map(|data| {
            DataReaderResult::Image(
                data,
                FileMetadata {
                    size:       file_size,
                    line_count: None,
                },
            )
        },),
        FileFormat::Json => {
            crate::reader::json_reader::read_json_value(file_path, head,).map(|data| {
                let line_count = data.line_count;
                DataReaderResult::Json(
                    data,
                    FileMetadata {
                        size: file_size,
                        line_count,
                    },
                )
            },)
        },
        FileFormat::Markdown => {
            crate::reader::md_reader::read_md_content(file_path, head,).map(|data| {
                let line_count = data.content.lines().count();
                DataReaderResult::Markdown(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: Some(line_count,),
                    },
                )
            },)
        },
        FileFormat::Parquet => crate::reader::parquet_reader::read_parquet_data(file_path, head,)
            .map(|data| {
                let num_rows = data.num_rows;
                DataReaderResult::Parquet(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: Some(num_rows as usize,),
                    },
                )
            },),
        FileFormat::Pdf => crate::reader::pdf_reader::read_pdf_text(file_path, head,).map(|data| {
            let line_count = data.line_count;
            DataReaderResult::Pdf(
                data,
                FileMetadata {
                    size:       file_size,
                    line_count: Some(line_count,),
                },
            )
        },),
        FileFormat::Spreadsheet => {
            crate::reader::spreadsheet_reader::read_spreadsheet_data(file_path,).map(|data| {
                DataReaderResult::Spreadsheet(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: None,
                    },
                )
            },)
        },
        FileFormat::Sqlite => {
            crate::reader::sqlite_reader::read_sqlite_data(file_path,).map(|data| {
                DataReaderResult::Sqlite(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: None,
                    },
                )
            },)
        },
        FileFormat::Toml => {
            crate::reader::toml_reader::read_toml_value(file_path, head,).map(|data| {
                DataReaderResult::Toml(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: None,
                    },
                )
            },)
        },
        FileFormat::Text => {
            crate::reader::txt_reader::read_txt_content(file_path, head,).map(|data| {
                let line_count = data.line_count;
                let total_size = data.total_size;
                DataReaderResult::Text(
                    data,
                    FileMetadata {
                        size:       total_size,
                        line_count: Some(line_count,),
                    },
                )
            },)
        },
        FileFormat::Xml => {
            crate::reader::xml_reader::read_xml_content(file_path, head,).map(|data| {
                let line_count = data.content.lines().count();
                DataReaderResult::Xml(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: Some(line_count,),
                    },
                )
            },)
        },
        FileFormat::Yaml => {
            crate::reader::yaml_reader::read_yaml_value(file_path, head,).map(|data| {
                DataReaderResult::Yaml(
                    data,
                    FileMetadata {
                        size:       file_size,
                        line_count: None,
                    },
                )
            },)
        },
        FileFormat::Zip => crate::reader::zip_reader::read_zip_data(file_path,).map(|data| {
            DataReaderResult::Zip(
                data,
                FileMetadata {
                    size:       file_size,
                    line_count: None,
                },
            )
        },),
        FileFormat::Unknown => Err(DataReaderError::InternalError(format!(
            "Unsupported file format for data reading: {}",
            file_path.display()
        ),),),
    }
}

fn serialize_raw_content_to_string(
    content: String,
    output_format: OutputFormat,
    file_type: &str,
) -> Result<String, DataReaderError,> {
    match output_format {
        OutputFormat::Json => {
            let mut map = serde_json::Map::new();
            map.insert("content".to_string(), serde_json::Value::String(content,),);
            serde_json::to_string_pretty(&serde_json::Value::Object(map,),).map_err(|e| {
                DataReaderError::InternalError(format!(
                    "Failed to serialize {} raw content to JSON: {}",
                    file_type, e
                ),)
            },)
        },
        OutputFormat::Yaml => {
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                serde_yaml::Value::String("content".to_string(),),
                serde_yaml::Value::String(content,),
            );
            serde_yaml::to_string(&serde_yaml::Value::Mapping(map,),).map_err(|e| {
                DataReaderError::InternalError(format!(
                    "Failed to serialize {} raw content to YAML: {}",
                    file_type, e
                ),)
            },)
        },
        _ => Err(DataReaderError::InternalError(format!(
            "Unsupported output format for {} raw content: {:?}",
            file_type, output_format
        ),),),
    }
}

pub fn read_file_to_raw_content(
    file_path: &Path,
    head: Option<usize,>,
    output_format: OutputFormat,
) -> Result<String, DataReaderError,> {
    let format = get_file_format(file_path,);
    match format {
        FileFormat::Csv => crate::reader::csv_reader::get_csv_raw_content(file_path, head,),
        FileFormat::Json => crate::reader::json_reader::get_json_raw_content(file_path, head,),
        FileFormat::Toml => crate::reader::toml_reader::get_toml_raw_content(file_path, head,),
        FileFormat::Yaml => crate::reader::yaml_reader::get_yaml_raw_content(file_path, head,),
        FileFormat::Markdown => {
            let markdown_data = crate::reader::md_reader::read_md_content(file_path, head,)?;
            serialize_raw_content_to_string(markdown_data.content, output_format, "Markdown",)
        },
        FileFormat::Pdf => {
            let pdf_data = crate::reader::pdf_reader::read_pdf_text(file_path, head,)?;
            serialize_raw_content_to_string(pdf_data.content, output_format, "PDF",)
        },
        FileFormat::Text => {
            let text_data = crate::reader::txt_reader::read_txt_content(file_path, head,)?;
            serialize_raw_content_to_string(text_data.content, output_format, "Text",)
        },
        FileFormat::Xml => {
            let xml_data = crate::reader::xml_reader::read_xml_content(file_path, head,)?;
            serialize_raw_content_to_string(xml_data.content, output_format, "XML",)
        },
        FileFormat::Parquet => {
            let all_rows = crate::reader::parquet_reader::read_full_parquet_content(file_path,)?;
            match output_format {
                OutputFormat::Json => serde_json::to_string_pretty(&all_rows,).map_err(|e| {
                    DataReaderError::InternalError(format!(
                        "Failed to serialize Parquet raw content to JSON: {}",
                        e
                    ),)
                },),
                OutputFormat::Yaml => serde_yaml::to_string(&all_rows,).map_err(|e| {
                    DataReaderError::InternalError(format!(
                        "Failed to serialize Parquet raw content to YAML: {}",
                        e
                    ),)
                },),
                _ => Err(DataReaderError::InternalError(format!(
                    "Unsupported output format for Parquet raw content: {:?}",
                    output_format
                ),),),
            }
        },
        _ => Err(DataReaderError::InternalError(format!(
            "Unsupported file format for raw content output: {}",
            file_path.display()
        ),),),
    }
}

pub fn read_file_to_stream(
    file_path: &Path,
    file_format: FileFormat,
) -> Result<DataReaderResult, DataReaderError,> {
    let base_metadata =
        std::fs::metadata(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
    let file_size = base_metadata.len();
    let metadata = FileMetadata {
        size:       file_size,
        line_count: None,
    };

    match file_format {
        FileFormat::Csv => {
            let (_headers, stream,) = crate::reader::csv_reader::read_csv_stream(file_path,)?;
            Ok(DataReaderResult::Stream(stream, metadata,),)
        },
        FileFormat::Json => {
            let stream = crate::reader::json_reader::read_json_stream(file_path,)?;
            Ok(DataReaderResult::Stream(stream, metadata,),)
        },
        FileFormat::Xml => {
            let stream = crate::reader::xml_reader::create_xml_stream(file_path,)?;
            Ok(DataReaderResult::Stream(stream, metadata,),)
        },
        FileFormat::Parquet => {
            let stream = crate::reader::parquet_reader::read_parquet_stream(file_path,)?;
            Ok(DataReaderResult::Stream(stream, metadata,),)
        },
        // For other formats, we don't have a record-based stream yet, so fall back
        _ => read_file_to_data(file_path, None, file_format,),
    }
}

pub async fn read_file_content(
    file_path: &Path,
    options: FileReaderOptions,
) -> Result<DataReaderResult, DataReaderError,> {
    let determined_format = if let Some(file_type_str,) = &options.file_type_override {
        match file_type_str.to_lowercase().as_str() {
            "csv" => FileFormat::Csv,
            "gz" => FileFormat::Gzip,
            "image" => FileFormat::Image,
            "json" => FileFormat::Json,
            "md" => FileFormat::Markdown,
            "parquet" => FileFormat::Parquet,
            "pdf" => FileFormat::Pdf,
            "spreadsheet" => FileFormat::Spreadsheet,
            "sqlite" => FileFormat::Sqlite,
            "toml" => FileFormat::Toml,
            "txt" => FileFormat::Text,
            "xml" => FileFormat::Xml,
            "yaml" => FileFormat::Yaml,
            "zip" => FileFormat::Zip,
            _ => {
                return Err(DataReaderError::UnsupportedFileFormat(format!(
                    "Unsupported file type override: {}",
                    file_type_str
                ),),);
            },
        }
    } else {
        get_file_format(file_path,)
    };

    match options.output_mode {
        OutputMode::FullRaw => {
            let raw_content =
                read_file_to_raw_content(file_path, options.head, options.output_format,)?;
            let metadata =
                std::fs::metadata(file_path,).map_err(|e| DataReaderError::FileReadError {
                    path:   file_path.to_path_buf(),
                    source: e,
                },)?;
            Ok(DataReaderResult::RawContent(
                raw_content,
                FileMetadata {
                    size:       metadata.len(),
                    line_count: None,
                },
            ),)
        },
        OutputMode::SchemaOnly | OutputMode::Default => {
            read_file_to_data(file_path, options.head, determined_format,)
        },
        OutputMode::Stream => read_file_to_stream(file_path, determined_format,),
        OutputMode::Analyze => match determined_format {
            FileFormat::Parquet => {
                let data = crate::reader::parquet_reader::read_parquet_nc_for_analysis(file_path,)?;
                let metadata =
                    std::fs::metadata(file_path,).map_err(|e| DataReaderError::FileReadError {
                        path:   file_path.to_path_buf(),
                        source: e,
                    },)?;
                let num_rows = data.num_rows;
                Ok(DataReaderResult::ParquetAnalysis(
                    data,
                    FileMetadata {
                        size:       metadata.len(),
                        line_count: Some(num_rows as usize,),
                    },
                ),)
            },
            _ => read_file_to_data(file_path, options.head, determined_format,),
        },
    }
}

pub async fn read_directory_content(
    directory_path: &Path,
    options: FileReaderOptions,
) -> Result<DataReaderResult, DataReaderError,> {
    let mut results: Vec<(PathBuf, DataReaderResult,),> = Vec::new();

    let walker = if options.recursive {
        WalkDir::new(directory_path,)
    } else {
        WalkDir::new(directory_path,).max_depth(1,)
    };

    for entry in walker {
        let entry = entry.map_err(|e| {
            DataReaderError::InternalError(format!("Error walking directory: {}", e),)
        },)?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let canonical_path = std::fs::canonicalize(path,).map_err(|e| {
            DataReaderError::InternalError(format!(
                "Error canonicalizing path {}: {}",
                path.display(),
                e
            ),)
        },)?;

        if let Some(output_p,) = &options.output_path
            && canonical_path == *output_p
        {
            continue;
        }

        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with('.',),)
        {
            continue;
        }

        let skip_file = if let Some(ext_filters,) = &options.filter_exts {
            match path.extension().and_then(|s| s.to_str(),) {
                Some(ext,) => !ext_filters
                    .iter()
                    .any(|f| f.to_lowercase() == ext.to_lowercase(),),
                None => true,
            }
        } else {
            false
        };

        if skip_file {
            continue;
        }

        match read_file_content(path, options.clone(),).await {
            Ok(result,) => results.push((path.to_path_buf(), result,),),
            Err(e,) => {
                error!("Error reading file {}: {}", path.display(), e);
            },
        }
    }
    let dir_metadata =
        std::fs::metadata(directory_path,).map_err(|e| DataReaderError::FileReadError {
            path:   directory_path.to_path_buf(),
            source: e,
        },)?;
    Ok(DataReaderResult::DirectoryResults(
        results,
        FileMetadata {
            size:       dir_metadata.len(),
            line_count: None,
        },
    ),)
}
