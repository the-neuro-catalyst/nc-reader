use std::fs::File;
use std::io::Write;
use std::path::Path;

use nc_reader::reader::json_reader::read_json_value;
use nc_reader::reader::xml_reader::read_xml_content;
use tempfile::tempdir;

#[test]
fn test_large_json_parsing() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("large.json",);

    // Generate a large JSON array (approx 10MB for a quick test, can be larger)
    {
        let mut file = File::create(&file_path,).unwrap();
        write!(file, "[").unwrap();
        for i in 0..100000 {
            if i > 0 {
                write!(file, ",").unwrap();
            }
            write!(
                file,
                "{{\"id\":{},\"name\":\"item_{}\",\"active\":true,\"values\":[1,2,3,4,5]}}",
                i, i
            )
            .unwrap();
        }
        write!(file, "]").unwrap();
    }

    let result = read_json_value(&file_path, None,);
    assert!(result.is_ok());
    let data = result.unwrap();

    if let serde_json::Value::Array(arr,) = data.value {
        assert_eq!(arr.len(), 100000);
    } else {
        panic!("Expected JSON array");
    }
}

#[test]
fn test_large_xml_parsing() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("large.xml",);

    // Generate a large XML file
    {
        let mut file = File::create(&file_path,).unwrap();
        writeln!(file, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").unwrap();
        writeln!(file, "<root>").unwrap();
        for i in 0..100000 {
            writeln!(file, "  <item id=\"{}\">", i).unwrap();
            writeln!(file, "    <name>item_{}</name>", i).unwrap();
            writeln!(
                file,
                "    <description>This is a description for item {}</description>",
                i
            )
            .unwrap();
            writeln!(file, "  </item>").unwrap();
        }
        writeln!(file, "</root>").unwrap();
    }

    let result = read_xml_content(&file_path, None,);
    assert!(result.is_ok());
    let data = result.unwrap();

    assert_eq!(data.root_element, Some("root".to_string()));
    assert!(data.element_counts.get("item").unwrap() >= &100000);
}

#[test]
fn test_xml_streaming() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("stream.xml",);

    {
        let mut file = File::create(&file_path,).unwrap();
        writeln!(file, "<root>").unwrap();
        for i in 0..10 {
            writeln!(file, "  <item id=\"{}\">content {}</item>", i, i).unwrap();
        }
        writeln!(file, "</root>").unwrap();
    }

    let stream_result = nc_reader::reader::xml_reader::create_xml_stream(&file_path,);
    assert!(stream_result.is_ok());
    let stream = stream_result.unwrap();

    let mut count = 0;
    for record_res in stream {
        assert!(record_res.is_ok());
        let record = record_res.unwrap();
        assert_eq!(record["@id"], serde_json::Value::from(count));
        // For <item>content 0</item>, it should be parsed as text if no children
        // Wait, my parse_element for <item id="0">content 0</item>
        // will have an attribute, so it will be an object with "@id" and "#text"
        assert_eq!(record["#text"], format!("content {}", count));
        count += 1;
    }
    assert_eq!(count, 10);
}

#[test]
fn test_parquet_streaming() {
    let file_path = Path::new("../test_data/sample.parquet",);
    if !file_path.exists() {
        return;
    }

    let stream_result = nc_reader::reader::parquet_reader::read_parquet_stream(file_path,);
    assert!(stream_result.is_ok());
    let stream = stream_result.unwrap();

    let mut count = 0;
    for record_res in stream {
        assert!(record_res.is_ok());
        let record = record_res.unwrap();
        assert!(record.is_object());
        count += 1;
    }
    // We don't know the exact count without reading it, but we can check it's > 0
    assert!(count > 0);
}
