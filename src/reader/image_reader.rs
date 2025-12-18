use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use exif::{Reader, Tag};
use image::ImageFormat;
use image::io::Reader as ImageReader;
use serde::{Deserialize, Serialize}; // Add this import

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct ImageData {
    pub total_size: u64,
    pub format:     String,
    pub dimensions: String,
    pub exif_data:  Option<HashMap<String, String,>,>,
}

pub fn read_image_data(file_path: &Path,) -> Result<ImageData, DataReaderError,> {
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

    let img_reader = ImageReader::open(file_path,)
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?
        .with_guessed_format()
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?;
    let format = img_reader.format().ok_or_else(|| {
        DataReaderError::InternalError("Could not guess image format".to_string(),)
    },)?;

    let img = img_reader
        .decode()
        .map_err(|e| DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(e,),
        },)?;
    let dimensions = format!("{}x{}", img.width(), img.height());

    let mut exif_data: Option<HashMap<String, String,>,> = None;

    // Extract EXIF data if it's a JPEG
    if format == ImageFormat::Jpeg {
        let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
        let mut buf_reader = BufReader::new(&file,);
        let exif_reader = Reader::new();

        match exif_reader.read_from_container(&mut buf_reader,) {
            Ok(exif,) => {
                let mut exif_map = HashMap::new();
                for field in exif.fields() {
                    if matches!(
                        field.tag,
                        Tag::Make
                            | Tag::Model
                            | Tag::DateTimeOriginal
                            | Tag::FNumber
                            | Tag::ExposureTime
                            | Tag::ISOSpeed
                    ) {
                        exif_map.insert(
                            format!("{}", field.tag),
                            format!("{}", field.display_value().with_unit(&exif)),
                        );
                    }
                }
                exif_data = Some(exif_map,);
            },
            Err(_,) => {
                // Not returning an error here, but adding it to exif_data if not found/parsed
                // For now, keep exif_data as None, but if needed, can add a field in
                // ImageData for exif_parse_error.
            },
        }
    }

    Ok(ImageData {
        total_size,
        format: format!("{:?}", format),
        dimensions,
        exif_data,
    },)
}
