# 👁️ nc_reader

**nc_reader** is the "Perception" layer of the Neuro-Catalyst stack. It is a high-performance, multi-format data extraction engine designed to handle massive files with constant memory overhead.

## 🚀 Features

- **Streaming Architecture**: Efficiently processes massive CSV, JSON, Parquet, and XML files using iterators.
- **Auto-Charset Detection**: Automatically detects and decodes text encodings (Windows-1252, Shift-JIS, etc.) using `chardetng`.
- **Comprehensive Support**:
  - **Tabular**: CSV, Parquet, SQLite, Excel (XLSX/XLSB/XLSM).
  - **Semi-structured**: JSON, XML, TOML, YAML.
  - **Documents**: PDF, Markdown, TXT.
  - **Media/Archives**: Image EXIF, ZIP, GZIP.
- **Schema Inference**: Automatically generates a structural schema while reading data.

## 💻 CLI Usage

```bash
# Read a file and output as JSON
nc_reader --file-path data.csv --format Json

# Stream a massive XML file
nc_reader --file-path large.xml --format Text --head 10

# Read a directory recursively
nc_reader --directory-path ./data --recursive --filter-ext parquet
```

## 🛠️ Library Usage: `RecordStream`

The core of `nc_reader` is the `RecordStream` trait, which provides a unified interface for all readers:

```rust
use nc_reader::reader::csv_reader::CsvStream;

// All readers return a Stream of Result<serde_json::Value, DataReaderError>
for record in csv_stream {
    let json_value = record?;
    // Process record...
}
```

## ⚙️ Configuration

The reader can be configured via `cli.yml` or CLI flags for buffer sizes, thread limits, and extraction depth.
