use std::path::Path;

use serde::{Deserialize, Serialize}; // Added Serialize and Deserialize

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct PdfData {
    pub content:     String,
    pub first_lines: Option<Vec<String,>,>,
    pub page_count:  Option<usize,>, // Using Option because pdf_extract doesn't expose it directly
    pub line_count:  usize,          // From extracted text
    pub total_size:  u64,            // In bytes
}

pub fn read_pdf_text(file_path: &Path, head: Option<usize,>,) -> Result<PdfData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,);

    let content =
        pdf_extract::extract_text(file_path,).map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?;

    let file_metadata =
        std::fs::metadata(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
    let total_size = file_metadata.len();
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

    Ok(PdfData {
        content,
        first_lines,
        page_count: None, // pdf_extract does not provide page count directly
        line_count,
        total_size,
    },)
}
