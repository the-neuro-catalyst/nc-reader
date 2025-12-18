use std::fs;
use std::io::Write; // New import for writeln!
use std::path::{Path, PathBuf};

use clap::{ArgGroup, CommandFactory, Parser};
use nc_reader::file_reader::FileReaderOptions;
use nc_reader::output::{OutputFormat, OutputMode};
use tracing::{info, warn};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser, Debug,)]
#[command(
    author,
    version,
    about = "A versatile data reader tool that supports multiple file formats and provides \
             structured output.",
    long_about = "
    A versatile data reader tool that supports multiple file formats (CSV, JSON, Parquet, XML, \
                  etc.) and provides structured output in text, JSON, or YAML formats. It can \
                  infer schemas, display content summaries, and handle both individual files and \
                  directories.
    This tool aims to simplify data exploration and integration by providing a consistent \
                  interface for diverse data sources.

    Usage:
        nc_reader --file-path <FILE_PATH> [--format <FORMAT>] [--schema] [--head <LINES>] \
                  [--all] [--file-type <TYPE>] [--output-path <PATH>] [--analyze]
        nc_reader --directory-path <DIRECTORY_PATH> [--format <FORMAT>] [--schema] [--head \
                  <LINES>] [--all] [--file-type <TYPE>] [--recursive] [--filter-ext <EXT>] \
                  [--output-path <PATH>] [--analyze]

    Examples:
        # Read a CSV file and output its schema in JSON format
        nc_reader --file-path data.csv --format json --schema

        # Read the first 10 lines of a JSON file
        nc_reader --file-path config.json --head 10

        # Read a Parquet file and get full raw content
        nc_reader --file-path data.parquet --all

        # Read all CSV files in a directory recursively and output to a single YAML file
        nc_reader --directory-path my_nc_dir --recursive --filter-ext csv --format yaml \
                  --output-path output.yaml

        # Read a file, explicitly treating it as a JSON file regardless of extension
        nc_reader --file-path my_data.txt --file-type json

        # Analyze a Parquet file for column statistics
        nc_reader --file-path data.parquet --analyze --format json
        
    "
)]
#[clap(group(
    ArgGroup::new("input_source")
        .required(true)
        .args(&["file_path", "directory_path"]),
))]
struct Cli {
    /// Path to the file to read
    #[arg(long, group = "input_source")]
    file_path: Option<PathBuf,>,

    /// Path to the directory to read
    #[arg(long, group = "input_source")]
    directory_path: Option<PathBuf,>,

    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,

    /// Display only schema or structural information
    #[arg(long)]
    schema: bool,

    /// Display the first N lines of content for text-based files
    #[arg(long, value_name = "LINES")]
    head: Option<usize,>,

    /// Output full raw content (disables analytical summary)
    #[arg(long)]
    all: bool,

    /// Explicitly set the file type (e.g., csv, json, parquet, etc.)
    #[arg(long, value_name = "TYPE")]
    file_type: Option<String,>,

    /// Recursively read files in subdirectories
    #[arg(long)]
    recursive: bool,

    /// Filter files by extension when reading a directory (e.g., "csv", "json")
    #[arg(long, value_name = "EXT")]
    filter_ext: Option<String,>,

    /// Path to write the output to instead of stdout
    #[arg(long, value_name = "PATH")]
    output_path: Option<PathBuf,>,
}

// Helper function to write output
fn write_output(
    formatted_output: &str,
    output_path: Option<&Path,>,
) -> Result<(), Box<dyn std::error::Error,>,> {
    if let Some(path,) = output_path {
        let mut file = std::fs::File::create(path,).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Failed to create output file {}: {}", path.display(), e),
            ),) as Box<dyn std::error::Error,>
        },)?;
        writeln!(file, "{}", formatted_output).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Failed to write to output file {}: {}", path.display(), e),
            ),) as Box<dyn std::error::Error,>
        },)?;
    } else {
        info!("{}", formatted_output);
    }
    Ok((),)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error,>,> {
    // Initialize tracing
    let file_appender = tracing_appender::rolling::never(".", "reader.log",);
    let (non_blocking, _guard,) = tracing_appender::non_blocking(file_appender,);

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info",),),)
        .with(fmt::layer().with_writer(std::io::stderr,),)
        .with(fmt::layer().with_writer(non_blocking,).with_ansi(false,),)
        .init();

    let path = std::env::current_dir().map_err(|e| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Error getting current directory: {}", e),
        ),) as Box<dyn std::error::Error,>
    },)?;
    info!("The current directory is {}", path.display());
    if std::env::args().len() == 1 {
        // If no arguments are provided, print the help message
        Cli::command().print_help().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Error printing help: {}", e),
            ),) as Box<dyn std::error::Error,>
        },)?;
        return Ok((),);
    }

    let cli = Cli::parse(); // Removed mut

    // Infer output format from output_path extension if --format is not explicitly set
    let determined_output_format = if cli.output_path.is_some() && cli.format == OutputFormat::Text
    {
        cli.output_path
            .as_ref()
            .and_then(|path| OutputFormat::from_extension(path,),)
            .unwrap_or(OutputFormat::Text,) // Fallback to Text if extension doesn't match
    } else {
        cli.format
    };

    let output_mode = if cli.all {
        OutputMode::FullRaw
    } else if cli.schema {
        OutputMode::SchemaOnly
    } else {
        OutputMode::default()
    };

    let canonicalized_output_path = cli.output_path.as_ref().map(|p| {
        std::fs::canonicalize(p,).unwrap_or_else(|_| {
            warn!("Warning: Could not canonicalize output path. Using original path.");
            p.clone()
        },)
    },);

    let options = FileReaderOptions {
        head: cli.head,
        file_type_override: cli.file_type,
        output_mode,
        output_format: determined_output_format, // Use determined format
        recursive: cli.recursive,
        filter_exts: cli.filter_ext.map(|e| vec![e],),
        output_path: canonicalized_output_path.clone(), // Clone here to pass to options
    };

    let result = if let Some(file_path_arg,) = cli.file_path {
        let absolute_path = std::fs::canonicalize(&file_path_arg,).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Error resolving file path {}: {}",
                    file_path_arg.display(),
                    e
                ),
            ),) as Box<dyn std::error::Error,>
        },)?;

        let metadata = fs::metadata(&absolute_path,)
            .map_err(|e| Box::new(e,) as Box<dyn std::error::Error,>,)?;
        if metadata.is_dir() {
            if options.recursive {
                nc_reader::file_reader::read_directory_content(&absolute_path, options,)
                    .await
                    .map_err(|e| Box::new(e,) as Box<dyn std::error::Error,>,)?
            } else {
                return Err(Box::new(nc_reader::error::DataReaderError::IsADirectory {
                    path: absolute_path,
                },) as Box<dyn std::error::Error,>,);
            }
        } else {
            nc_reader::file_reader::read_file_content(&absolute_path, options,)
                .await
                .map_err(|e| Box::new(e,) as Box<dyn std::error::Error,>,)?
        }
    } else if let Some(directory_path,) = cli.directory_path {
        let absolute_path = std::fs::canonicalize(&directory_path,)
            .map_err(|e| Box::new(e,) as Box<dyn std::error::Error,>,)?;
        nc_reader::file_reader::read_directory_content(&absolute_path, options,)
            .await
            .map_err(|e| Box::new(e,) as Box<dyn std::error::Error,>,)?
    } else {
        return Err(Box::<dyn std::error::Error,>::from(
            "Either FILE_PATH or --directory-path must be provided.",
        ),);
    };

    let formatted_output = result.to_string_formatted(determined_output_format,); // Use determined format

    write_output(&formatted_output, canonicalized_output_path.as_deref(),)?;
    Ok((),)
}
