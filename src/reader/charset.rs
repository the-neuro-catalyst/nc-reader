use std::io::Read;
use encoding_rs::Encoding;
use chardetng::EncodingDetector;

pub fn detect_encoding_from_buffer(buffer: &[u8]) -> &'static Encoding {
    let mut detector = EncodingDetector::new();
    detector.feed(buffer, true);
    detector.guess(None, true)
}

pub fn decode_to_string(bytes: &[u8]) -> String {
    let encoding = detect_encoding_from_buffer(bytes);
    let (res, _, _) = encoding.decode(bytes);
    res.into_owned()
}

pub fn get_decoded_reader(file: std::fs::File) -> std::io::Result<encoding_rs_io::DecodeReaderBytes<std::fs::File, Vec<u8>>> {
    let mut detector = EncodingDetector::new();
    let mut buffer = [0u8; 4096];
    
    // Sniff the first chunk
    let mut sniff_reader = &file;
    let bytes_read = sniff_reader.read(&mut buffer)?;
    detector.feed(&buffer[..bytes_read], bytes_read < buffer.len());
    let encoding = detector.guess(None, true);
    
    // Reset file position after sniffing
    use std::io::{Seek, SeekFrom};
    let mut file_to_reset = file;
    file_to_reset.seek(SeekFrom::Start(0))?;
    
    Ok(encoding_rs_io::DecodeReaderBytesBuilder::new()
        .encoding(Some(encoding))
        .build(file_to_reset))
}
