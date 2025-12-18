use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use nc_reader::file_reader::{
    FileFormat, FileReaderOptions, get_file_format, read_directory_content, read_file_to_data,
    read_file_to_raw_content,
};
use nc_reader::nc_reader_result::DataReaderResult;
use nc_reader::output::{OutputFormat, OutputMode};
use nc_schema::DataType;
use tempfile::tempdir;

fn create_temp_file(base_path: &Path, file_name: &str, content: &str,) -> PathBuf {
    let file_path = base_path.join(file_name,);
    let mut file = File::create(&file_path,).expect("Failed to create temp file",);
    file.write_all(content.as_bytes(),)
        .expect("Failed to write to temp file",);
    file_path
}

fn get_test_nc_path(file_name: &str,) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"),)
        .join("..",)
        .join("test_data",)
        .join(file_name,)
}

#[test]
fn test_get_file_format_csv() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.csv", "a,b,c\n1,2,3",);
    assert_eq!(get_file_format(&path), FileFormat::Csv);
}

#[test]
fn test_get_file_format_gzip() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.gz", "",);
    assert_eq!(get_file_format(&path), FileFormat::Gzip);
}

#[test]
fn test_get_file_format_image_jpg() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.jpg", "",);
    assert_eq!(get_file_format(&path), FileFormat::Image);
}

#[test]
fn test_get_file_format_image_png() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.png", "",);
    assert_eq!(get_file_format(&path), FileFormat::Image);
}

#[test]
fn test_get_file_format_json() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(
        dir.path(),
        "test.json",
        &serde_json::to_string(&serde_json::json!({}),).unwrap(),
    );
    assert_eq!(get_file_format(&path), FileFormat::Json);
}

#[test]
fn test_get_file_format_markdown() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.md", "# Hello",);
    assert_eq!(get_file_format(&path), FileFormat::Markdown);
}

#[test]
fn test_get_file_format_parquet() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.parquet", "",);
    assert_eq!(get_file_format(&path), FileFormat::Parquet);
}

#[test]
fn test_get_file_format_pdf() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.pdf", "",);
    assert_eq!(get_file_format(&path), FileFormat::Pdf);
}

#[test]
fn test_get_file_format_spreadsheet() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.xlsx", "",);
    assert_eq!(get_file_format(&path), FileFormat::Spreadsheet);
}

#[test]
fn test_get_file_format_sqlite() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.sqlite", "",);
    assert_eq!(get_file_format(&path), FileFormat::Sqlite);
}

#[test]
fn test_get_file_format_toml() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.toml", "key = \"value\"",);
    assert_eq!(get_file_format(&path), FileFormat::Toml);
}

#[test]
fn test_get_file_format_text() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.txt", "hello world",);
    assert_eq!(get_file_format(&path), FileFormat::Text);
}

#[test]
fn test_get_file_format_xml() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.xml", "<root></root>",);
    assert_eq!(get_file_format(&path), FileFormat::Xml);
}

#[test]
fn test_get_file_format_yaml() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.yaml", "key: value",);
    assert_eq!(get_file_format(&path), FileFormat::Yaml);
}

#[test]
fn test_get_file_format_zip() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.zip", "",);
    assert_eq!(get_file_format(&path), FileFormat::Zip);
}

#[test]
fn test_get_file_format_unknown() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "test.xyz", "",);
    assert_eq!(get_file_format(&path), FileFormat::Unknown);
}

#[test]
fn test_get_file_format_no_extension() {
    let dir = tempdir().unwrap();
    let path = create_temp_file(dir.path(), "testfile", "some content",);
    assert_eq!(get_file_format(&path), FileFormat::Unknown);
}

#[test]
fn test_read_file_to_nc_csv() {
    let path = get_test_nc_path("sample.csv",);
    if path.exists() {
        let result = read_file_to_data(&path, None, FileFormat::Csv,);
        assert!(result.is_ok());
        if let Ok(DataReaderResult::Csv(csv_data, _metadata,),) = result {
            assert_eq!(csv_data.column_headers.len(), 3);
        }
    } else {
        eprintln!(
            "Warning: {} not found, skipping test_read_file_to_nc_csv",
            path.display()
        );
    }
}

#[test]
fn test_read_file_to_raw_content_csv_json_output() {
    let path = get_test_nc_path("sample.csv",);
    if path.exists() {
        let result = read_file_to_raw_content(&path, None, OutputFormat::Json,);
        assert!(result.is_ok());
        let content = result.unwrap();
        assert!(content.contains("["));
        assert!(content.contains("]"));
        assert!(content.contains("{"));
        assert!(content.contains("}"));
    } else {
        eprintln!(
            "Warning: {} not found, skipping test_read_file_to_raw_content_csv_json_output",
            path.display()
        );
    }
}

#[tokio::test]
async fn test_read_directory_basic() {
    let dir = tempdir().unwrap();
    create_temp_file(dir.path(), "file1.txt", "content1",);
    create_temp_file(dir.path(), "file2.csv", "a,b\n1,2",);

    let options = FileReaderOptions {
        head:               None,
        file_type_override: None,
        output_mode:        OutputMode::Default,
        output_format:      OutputFormat::Text,
        recursive:          false,
        filter_exts:        None,
        output_path:        None,
    };

    let result = read_directory_content(dir.path(), options,).await;
    assert!(result.is_ok());

    if let Ok(DataReaderResult::DirectoryResults(results, _metadata,),) = result {
        assert_eq!(results.len(), 2);
        let paths: Vec<String,> = results
            .iter()
            .map(|(p, _,)| p.file_name().unwrap().to_string_lossy().into_owned(),)
            .collect();
        assert!(paths.contains(&"file1.txt".to_string(),));
        assert!(paths.contains(&"file2.csv".to_string(),));
    } else {
        panic!("Expected DirectoryResults, got {:?}", result);
    }
}

#[tokio::test]
async fn test_read_directory_filter() {
    let dir = tempdir().unwrap();
    create_temp_file(dir.path(), "file1.txt", "content1",);
    create_temp_file(dir.path(), "file2.csv", "a,b\n1,2",);
    create_temp_file(
        dir.path(),
        "file3.json",
        &serde_json::to_string(&serde_json::json!({}),).unwrap(),
    );

    let options = FileReaderOptions {
        head:               None,
        file_type_override: None,
        output_mode:        OutputMode::Default,
        output_format:      OutputFormat::Text,
        recursive:          false,
        filter_exts:        Some(vec!["csv".to_string()],),
        output_path:        None,
    };

    let result = read_directory_content(dir.path(), options,).await;
    assert!(result.is_ok());

    if let Ok(DataReaderResult::DirectoryResults(results, _metadata,),) = result {
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].0.file_name().unwrap().to_string_lossy(),
            "file2.csv"
        );
    } else {
        panic!("Expected DirectoryResults, got {:?}", result);
    }
}

#[tokio::test]
async fn test_read_directory_recursive() {
    let dir = tempdir().unwrap();
    create_temp_file(dir.path(), "file1.txt", "content1",);
    let subdir = dir.path().join("subdir",);
    fs::create_dir(&subdir,).unwrap();
    create_temp_file(&subdir, "file2.csv", "a,b\n1,2",);
    create_temp_file(
        &subdir,
        "file3.json",
        &serde_json::to_string(&serde_json::json!({}),).unwrap(),
    );

    let options = FileReaderOptions {
        head:               None,
        file_type_override: None,
        output_mode:        OutputMode::Default,
        output_format:      OutputFormat::Text,
        recursive:          true,
        filter_exts:        Some(vec!["csv".to_string()],),
        output_path:        None,
    };

    let result = read_directory_content(dir.path(), options,).await;
    assert!(result.is_ok());

    if let Ok(DataReaderResult::DirectoryResults(results, _metadata,),) = result {
        assert_eq!(results.len(), 1);
        assert!(results[0].0.file_name().unwrap().to_string_lossy() == "file2.csv");
        assert!(results[0].0.parent().unwrap().ends_with("subdir"));
    } else {
        panic!("Expected DirectoryResults, got {:?}", result);
    }
}

#[tokio::test]
async fn test_output_to_file_single_file() -> Result<(), Box<dyn std::error::Error,>,> {
    let temp_dir = tempdir()?;
    let input_file_path = create_temp_file(temp_dir.path(), "input.txt", "hello world",);
    let output_file_path = temp_dir.path().join("output.txt",);

    let mut cmd = assert_cmd::Command::cargo_bin("nc-reader",)?;
    cmd.arg("--file-path",)
        .arg(&input_file_path,)
        .args(&["--output-path", output_file_path.to_str().unwrap(),],)
        .assert()
        .success();

    let output_content = fs::read_to_string(&output_file_path,)?;
    assert_eq!(output_content.trim(), "hello world");

    Ok((),)
}

#[tokio::test]
async fn test_output_to_file_directory_results() -> Result<(), Box<dyn std::error::Error,>,> {
    let temp_dir = tempdir()?;
    create_temp_file(temp_dir.path(), "file1.txt", "content1",);
    create_temp_file(
        temp_dir.path(),
        "file2.json",
        &serde_json::to_string(&serde_json::json!({"key": "value"}),).unwrap(),
    );
    let output_file_path = temp_dir.path().join("dir_output.json",);
    File::create(&output_file_path,)?;

    let mut cmd = assert_cmd::Command::cargo_bin("nc-reader",)?;
    cmd.args(&["--directory-path", temp_dir.path().to_str().unwrap(),],)
        .args(&["--output-path", output_file_path.to_str().unwrap(),],)
        .args(&["--format", "json",],)
        .assert()
        .success();

    let output_content = fs::read_to_string(&output_file_path,)?;
    let json_output: serde_json::Value = serde_json::from_str(&output_content,)?;

    assert!(json_output.is_array());
    assert_eq!(json_output.as_array().unwrap().len(), 2);

    let file1_result = json_output
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["path"].as_str().unwrap().contains("file1.txt",),)
        .unwrap();
    assert_eq!(file1_result["result"][0]["content"], "content1");

    let _file2_result = json_output
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["path"].as_str().unwrap().contains("file2.json",),)
        .unwrap();
    Ok((),)
}

#[test]
fn test_read_csv_windows_1252() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("windows1252.csv",);

    // "id,name\n1,Müller" in Windows-1252
    // 'ü' is 0xFC in Windows-1252
    let content: [u8; 16] = [
        0x69, 0x64, 0x2C, 0x6E, 0x61, 0x6D, 0x65, 0x0A, 0x31, 0x2C, 0x4D, 0xFC, 0x6C, 0x6C, 0x65,
        0x72,
    ];

    fs::write(&file_path, &content,).unwrap();

    let result = read_file_to_data(&file_path, None, FileFormat::Csv,).unwrap();
    if let DataReaderResult::Csv(data, _,) = result {
        assert_eq!(data.nc_rows.len(), 1);
        let name = data.nc_rows[0]["name"].as_str().unwrap();
        assert_eq!(name, "Müller");
    } else {
        panic!("Expected Csv result");
    }
}

#[test]
fn test_json_mixed_schema_inference() {
    let dir = tempdir().unwrap();
    let json_content = "[{\"a\": 1}, {\"a\": \"string\"}, {\"a\": null}]";
    let path = create_temp_file(dir.path(), "mixed.json", json_content,);

    let result = read_file_to_data(&path, None, FileFormat::Json,).unwrap();
    if let DataReaderResult::Json(data, _,) = result {
        let schema = data.inferred_schema.unwrap();
        // The element type of the array should be an Object with key "a" being a Union
        if let DataType::Array(inner,) = schema.nc_type {
            if let DataType::Object(map,) = *inner {
                let a_type = map.get("a",).unwrap();
                if let DataType::Union(v,) = a_type {
                    assert!(v.contains(&DataType::Integer));
                    assert!(v.contains(&DataType::String));
                    assert!(v.contains(&DataType::Null));
                } else {
                    panic!("Expected Union type for 'a', got {:?}", a_type);
                }
            } else {
                panic!("Expected Object inner type, got {:?}", inner);
            }
        } else {
            panic!("Expected Array schema type, got {:?}", schema.nc_type);
        }
    } else {
        panic!("Expected Json DataReaderResult");
    }
}

#[test]
fn test_xml_mixed_schema_inference() {
    let dir = tempdir().unwrap();
    let xml_content = "<root><item id=\"1\">text</item><item id=\"string\">more \
                       text</item><item>no id</item></root>";
    let path = create_temp_file(dir.path(), "mixed.xml", xml_content,);

    let result = read_file_to_data(&path, None, FileFormat::Xml,).unwrap();
    if let DataReaderResult::Xml(data, _,) = result {
        let schema = data.inferred_schema.unwrap();
        println!("XML Schema: {:?}", schema);
        let item_schema_type = schema.children.get("item",).unwrap();
        if let nc_reader::reader::xml_reader::XmlSchemaType::Array(inner,) = item_schema_type {
            let id_type = inner.attributes.get("id",).unwrap();
            if let DataType::Union(v,) = id_type {
                assert!(v.contains(&DataType::Integer));
                assert!(v.contains(&DataType::String));
                assert!(v.contains(&DataType::Null));
            } else {
                panic!("Expected Union type for 'id', got {:?}", id_type);
            }
        } else {
            panic!("Expected Array for 'item', got {:?}", item_schema_type);
        }
    } else {
        panic!("Expected Xml DataReaderResult");
    }
}
