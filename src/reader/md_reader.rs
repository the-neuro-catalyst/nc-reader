use std::collections::HashMap;
use std::fs;
use std::path::Path;

use pulldown_cmark::{Event, Parser, Tag};
use serde::{Deserialize, Serialize};

use crate::error::DataReaderError;

#[derive(Debug, Serialize, Deserialize, Default, Clone,)]
pub struct MarkdownElements {
    pub headings:    HashMap<u8, usize,>, // Heading level -> count
    pub links:       usize,
    pub images:      usize,
    pub code_blocks: usize,
    pub paragraphs:  usize,
    pub lists:       usize, // Combined for ordered/unordered
}

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct MarkdownData {
    pub content:     String,
    pub first_lines: Option<Vec<String,>,>,
    pub elements:    MarkdownElements, // New field
}

fn extract_markdown_elements(content: &str,) -> MarkdownElements {
    let parser = Parser::new(content,);
    let mut elements = MarkdownElements::default();

    for event in parser {
        match event {
            Event::Start(Tag::Heading { level, .. },) => {
                *elements.headings.entry(level as u8,).or_insert(0,) += 1;
            },
            Event::Start(Tag::Link { .. },) => elements.links += 1,
            Event::Start(Tag::Image { .. },) => elements.images += 1,
            Event::Start(Tag::CodeBlock(_,),) => elements.code_blocks += 1,
            Event::Start(Tag::Paragraph,) => elements.paragraphs += 1,
            Event::Start(Tag::List(_,),) => elements.lists += 1,
            _ => {},
        }
    }
    elements
}

pub fn read_md_content(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<MarkdownData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,);

    let content = fs::read_to_string(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;

    let first_lines: Option<Vec<String,>,> = if num_lines_to_extract > 0 {
        let lines: Vec<String,> = content
            .lines()
            .take(num_lines_to_extract,)
            .map(|s: &str| s.to_string(),)
            .collect();
        Some(lines,)
    } else {
        None
    };

    let elements = extract_markdown_elements(&content,);

    Ok(MarkdownData {
        content,
        first_lines,
        elements,
    },)
}
