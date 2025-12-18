# nc_reader API Documentation

**The Perception Layer**
`nc_reader` is a high-performance, streaming-capable engine for extracting data from various file formats and normalizing it into `nc_schema::DataType`.

## 💻 CLI Interface

### Basic Usage
```bash
nc_reader --file-path <PATH> [OPTIONS]
```

### Arguments
| Argument | Description | Default |
| :--- | :--- | :--- |
| `--file-path <PATH>` | Path to the input file. | - |
| `--directory-path <PATH>` | Path to a directory to scan. | - |
| `--recursive` | Recursively read subdirectories. | `false` |
| `--format <FMT>` | Output format: `text`, `json`, `yaml`. | `text` |
| `--schema` | Output only the inferred schema. | `false` |
| `--head <N>` | Show only the first N lines/records. | - |
| `--all` | output full raw content (disables summaries). | `false` |

## 📚 Library API

### `trait RecordStream`
The primary interface for reading data. All file format readers implement this trait.

```rust
pub trait RecordStream {
    // Returns the next record as a normalized nc_schema::DataType
    fn next_record(&mut self) -> Option<Result<DataType, ReaderError>>;
}
```

### Supported Formats
- **Tabular:** CSV, Parquet, SQLite, Excel (XLSX, XLS)
- **Structured:** JSON, XML, YAML, TOML
- **Document:** PDF, Markdown, TXT
- **Archive:** ZIP, GZIP (Transparent decompression)
