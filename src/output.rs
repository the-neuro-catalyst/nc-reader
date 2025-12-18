use std::fmt;
use std::path::Path; // New import

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, Default, PartialEq,)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
    Yaml,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_,>,) -> fmt::Result {
        match self {
            OutputFormat::Text => write!(f, "text"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Yaml => write!(f, "yaml"),
        }
    }
}

impl OutputFormat {
    pub fn from_extension(path: &Path,) -> Option<OutputFormat,> {
        path.extension()
            .and_then(|ext| ext.to_str(),)
            .and_then(|ext_str| {
                match ext_str.to_lowercase().as_str() {
                    "json" => Some(OutputFormat::Json,),
                    "yaml" | "yml" => Some(OutputFormat::Yaml,),
                    "txt" => Some(OutputFormat::Text,), // Explicitly map .txt to Text
                    _ => None,                          /* No matching output format for other
                                                          * extensions */
                }
            },)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default,)]
pub enum OutputMode {
    #[default]
    Default,
    SchemaOnly,
    FullRaw,
    Analyze, // New variant for analysis-ready data
    Stream,  // New variant for streaming records
}
