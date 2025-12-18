use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize}; // Add this import

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct GzipData {
    pub compressed_size:      u64,
    pub decompressed_content: Vec<u8,>,
}

pub fn read_gzip_data(file_path: &Path,) -> Result<GzipData, DataReaderError,> {
    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;
    let compressed_size = file
        .metadata()
        .map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?
        .len();

    let decoder = GzDecoder::new(file,);
    let mut reader = io::BufReader::new(decoder,);

    let mut decompressed_data = Vec::new();
    reader
        .read_to_end(&mut decompressed_data,)
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?; // Changed to ParseError as it's an issue with decompression, not just reading

    Ok(GzipData {
        compressed_size,
        decompressed_content: decompressed_data,
    },)
}
