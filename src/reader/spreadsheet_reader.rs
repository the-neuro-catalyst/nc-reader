use std::path::Path;

use calamine::{Data, Range, Reader, open_workbook_auto};
use serde::{Deserialize, Serialize}; // Add this import

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)] // Added Serialize and Deserialize
pub struct SpreadsheetSheetInfo {
    pub name:        String,
    pub row_count:   Option<usize,>,
    pub col_count:   Option<usize,>,
    pub range_start: Option<String,>,
    pub range_end:   Option<String,>,
}

#[derive(Debug, Serialize, Deserialize, Clone,)] // Added Serialize and Deserialize
pub struct SpreadsheetData {
    pub total_size:  u64,
    pub sheet_count: usize,
    pub sheets:      Vec<SpreadsheetSheetInfo,>,
}

pub fn read_spreadsheet_data(file_path: &Path,) -> Result<SpreadsheetData, DataReaderError,> {
    let total_size = std::fs::metadata(file_path,)
        .map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?
        .len();

    let mut workbook = open_workbook_auto(file_path,).map_err(|e| DataReaderError::ParseError {
        path:   file_path.to_path_buf(),
        source: Box::new(e,),
    },)?;

    let mut sheets_info = Vec::new();
    let sheet_names = workbook.sheet_names().to_owned();

    for sheet_name in sheet_names {
        let sheet_content: Option<Result<Range<Data,>, calamine::Error,>,> =
            Some(workbook.worksheet_range(&sheet_name,),);
        match sheet_content {
            Some(Ok(range,),) => {
                let mut sheet_info = SpreadsheetSheetInfo {
                    name:        sheet_name.clone(),
                    row_count:   None,
                    col_count:   None,
                    range_start: None,
                    range_end:   None,
                };

                if let (Some((start_row, start_col,),), Some((end_row, end_col,),),) =
                    (range.start(), range.end(),)
                {
                    sheet_info.row_count = Some((end_row - start_row + 1) as usize,);
                    sheet_info.col_count = Some((end_col - start_col + 1) as usize,);
                    sheet_info.range_start = Some(format!("{},{}", start_row, start_col),);
                    sheet_info.range_end = Some(format!("{},{}", end_row, end_col),);
                }
                sheets_info.push(sheet_info,);
            },
            Some(Err(e,),) => {
                return Err(DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                },);
            },
            None => {
                // Sheet not found or empty
            },
        }
    }

    Ok(SpreadsheetData {
        total_size,
        sheet_count: sheets_info.len(),
        sheets: sheets_info,
    },)
}
