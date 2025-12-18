use std::path::PathBuf;

#[derive(Debug, thiserror::Error,)]
pub enum DataReaderError {
    #[error("File not found or could not be read: {path} - {source}")]
    FileReadError {
        path:   PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error parsing file {path}: {source}")]
    ParseError {
        path:   PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync,>,
    },
    #[error("Internal error: {0}")]
    InternalError(String,),
    #[error("The provided path is a directory: {path}. Use --recursive to process directories.")]
    IsADirectory { path: PathBuf, },
    #[error("Unsupported file format: {0}")]
    UnsupportedFileFormat(String,),
}
