use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::DataReaderError;
use nc_schema::{DataType, merge_nc_types};

pub struct XmlReader<R: std::io::BufRead> {
    reader: Reader<R>,
    buf: Vec<u8>,
    path: PathBuf,
    depth: usize,
    root_tag: Option<String>,
}

impl<R: std::io::BufRead> XmlReader<R> {
    pub fn new(reader_input: R, path: PathBuf) -> Self {
        let mut reader = Reader::from_reader(reader_input);
        reader.config_mut().trim_text(true);
        Self {
            reader,
            buf: Vec::new(),
            path,
            depth: 0,
            root_tag: None,
        }
    }

    fn parse_element(&mut self, start: quick_xml::events::BytesStart) -> Result<Value, DataReaderError> {
        let mut map = Map::new();
        
        // Handle attributes
        for attr_result in start.attributes() {
            let attr = attr_result.map_err(|e| DataReaderError::ParseError {
                path: self.path.clone(),
                source: Box::new(e),
            })?;
            let key = String::from_utf8_lossy(attr.key.into_inner()).to_string();
            let value_str = String::from_utf8_lossy(&attr.value).to_string();
            
            // Convert to JSON type
            let value = if let Ok(i) = value_str.parse::<i64>() {
                Value::Number(i.into())
            } else if let Ok(f) = value_str.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::String(value_str))
            } else if value_str.to_lowercase() == "true" {
                Value::Bool(true)
            } else if value_str.to_lowercase() == "false" {
                Value::Bool(false)
            } else {
                Value::String(value_str)
            };

            map.insert(format!("@{}", key), value);
        }

        let mut text_content = String::new();
        let mut children = Map::new();

        loop {
            self.buf.clear();
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(e)) => {
                    let name = String::from_utf8_lossy(e.name().into_inner()).to_string();
                    let e_owned = e.into_owned();
                    let child_value = self.parse_element(e_owned)?;
                    
                    // Handle multiple children with same name by converting to array
                    match children.entry(name.clone()) {
                        serde_json::map::Entry::Vacant(entry) => {
                            entry.insert(child_value);
                        }
                        serde_json::map::Entry::Occupied(mut entry) => {
                            if let Value::Array(arr) = entry.get_mut() {
                                arr.push(child_value);
                            } else {
                                let old_val = entry.insert(Value::Array(vec![]));
                                if let Value::Array(arr) = entry.get_mut() {
                                    arr.push(old_val);
                                    arr.push(child_value);
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(_)) => break,
                Ok(Event::Text(e)) => {
                    text_content.push_str(&String::from_utf8_lossy(&e));
                }
                Ok(Event::Empty(e)) => {
                    let name = String::from_utf8_lossy(e.name().into_inner()).to_string();
                    let mut child_map = Map::new();
                    for attr_result in e.attributes() {
                        let attr = attr_result.map_err(|e| DataReaderError::ParseError {
                            path: self.path.clone(),
                            source: Box::new(e),
                        })?;
                        let key = String::from_utf8_lossy(attr.key.into_inner()).to_string();
                        let value_str = String::from_utf8_lossy(&attr.value).to_string();
                        
                        let value = if let Ok(i) = value_str.parse::<i64>() {
                            Value::Number(i.into())
                        } else if let Ok(f) = value_str.parse::<f64>() {
                            serde_json::Number::from_f64(f)
                                .map(Value::Number)
                                .unwrap_or(Value::String(value_str))
                        } else if value_str.to_lowercase() == "true" {
                            Value::Bool(true)
                        } else if value_str.to_lowercase() == "false" {
                            Value::Bool(false)
                        } else {
                            Value::String(value_str)
                        };
                        
                        child_map.insert(format!("@{}", key), value);
                    }
                    
                    let child_value = if child_map.is_empty() { Value::Null } else { Value::Object(child_map) };
                    
                    match children.entry(name) {
                        serde_json::map::Entry::Vacant(entry) => {
                            entry.insert(child_value);
                        }
                        serde_json::map::Entry::Occupied(mut entry) => {
                            if let Value::Array(arr) = entry.get_mut() {
                                arr.push(child_value);
                            } else {
                                let old_val = entry.insert(Value::Array(vec![]));
                                if let Value::Array(arr) = entry.get_mut() {
                                    arr.push(old_val);
                                    arr.push(child_value);
                                }
                            }
                        }
                    }
                }
                Ok(Event::CData(e)) => {
                    text_content.push_str(&String::from_utf8_lossy(&e));
                }
                Ok(Event::Eof) => return Err(DataReaderError::ParseError {
                    path: self.path.clone(),
                    source: Box::new(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Unexpected EOF while parsing element")),
                }),
                _ => {}
            }
        }

        if children.is_empty() {
            if map.is_empty() {
                // If it's just text, return the text value (attempting to parse as number/bool)
                let trimmed = text_content.trim();
                if trimmed.is_empty() {
                    return Ok(Value::Null);
                }
                if let Ok(i) = trimmed.parse::<i64>() {
                    return Ok(Value::Number(i.into()));
                }
                if let Ok(f) = trimmed.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(f) {
                        return Ok(Value::Number(num));
                    }
                }
                if trimmed.to_lowercase() == "true" { return Ok(Value::Bool(true)); }
                if trimmed.to_lowercase() == "false" { return Ok(Value::Bool(false)); }
                return Ok(Value::String(trimmed.to_string()));
            } else {
                if !text_content.trim().is_empty() {
                    map.insert("#text".to_string(), Value::String(text_content.trim().to_string()));
                }
                return Ok(Value::Object(map));
            }
        } else {
            // Merge map (attributes) and children
            for (k, v) in children {
                map.insert(k, v);
            }
            if !text_content.trim().is_empty() {
                map.insert("#text".to_string(), Value::String(text_content.trim().to_string()));
            }
            return Ok(Value::Object(map));
        }
    }
}

impl<R: std::io::BufRead> Iterator for XmlReader<R> {
    type Item = Result<serde_json::Value, DataReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buf.clear();
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(e)) => {
                    self.depth += 1;
                    if self.depth == 1 {
                        self.root_tag = Some(String::from_utf8_lossy(e.name().into_inner()).to_string());
                        continue;
                    }
                    if self.depth == 2 {
                        // This is a record!
                        let e_owned = e.into_owned();
                        let res = self.parse_element(e_owned);
                        self.depth -= 1; // parse_element consumed the End event
                        return Some(res);
                    }
                }
                Ok(Event::End(_)) => {
                    self.depth -= 1;
                }
                Ok(Event::Empty(e)) => {
                    if self.depth == 1 {
                        let mut map = Map::new();
                        for attr_result in e.attributes() {
                            if let Ok(attr) = attr_result {
                                let key = String::from_utf8_lossy(attr.key.into_inner()).to_string();
                                let value_str = String::from_utf8_lossy(&attr.value).to_string();
                                
                                let value = if let Ok(i) = value_str.parse::<i64>() {
                                    Value::Number(i.into())
                                } else if let Ok(f) = value_str.parse::<f64>() {
                                    serde_json::Number::from_f64(f)
                                        .map(Value::Number)
                                        .unwrap_or(Value::String(value_str))
                                } else if value_str.to_lowercase() == "true" {
                                    Value::Bool(true)
                                } else if value_str.to_lowercase() == "false" {
                                    Value::Bool(false)
                                } else {
                                    Value::String(value_str)
                                };
                                
                                map.insert(format!("@{}", key), value);
                            }
                        }
                        return Some(Ok(if map.is_empty() { Value::Null } else { Value::Object(map) }));
                    }
                }
                Ok(Event::Eof) => return None,
                Err(e) => return Some(Err(DataReaderError::ParseError {
                    path: self.path.clone(),
                    source: Box::new(e),
                })),
                _ => {}
            }
        }
    }
}

pub fn create_xml_stream(
    file_path: &Path,
) -> Result<crate::nc_reader_result::RecordStream, DataReaderError> {
    let file = File::open(file_path).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;
    let decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;
    let reader = BufReader::new(decoder);
    let xml_reader = XmlReader::new(reader, file_path.to_path_buf());
    Ok(Box::new(xml_reader))
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq,)]
pub enum XmlSchemaType {
    Element(XmlSchema,),
    Array(Box<XmlSchema,>,),
    Union(Vec<XmlSchemaType,>,),
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq,)]
pub struct XmlSchema {
    pub tag_name:          String,
    pub attributes:        HashMap<String, DataType,>,
    pub children:          HashMap<String, XmlSchemaType,>,
    pub has_text_content:  bool,
    pub text_content_type: Option<DataType,>,
    pub min_occurs:        usize,
    pub max_occurs:        Option<usize,>,
}

#[derive(Debug, Serialize, Deserialize, Clone,)]
pub struct XmlData {
    pub content:         String,
    pub root_element:    Option<String,>,
    pub element_counts:  HashMap<String, usize,>,
    pub first_lines:     Option<Vec<String,>,>,
    pub inferred_schema: Option<XmlSchema,>,
}

fn identify_nc_type(s: &str,) -> DataType {
    if s.parse::<i64>().is_ok() {
        DataType::Integer
    } else if s.parse::<f64>().is_ok() {
        DataType::Float
    } else if s.to_lowercase() == "true" || s.to_lowercase() == "false" {
        DataType::Boolean
    } else if s.is_empty() {
        DataType::Null
    } else {
        DataType::String
    }
}

fn infer_xml_schema<R: std::io::BufRead,>(
    reader_input: R,
    file_path: &Path,
) -> Result<XmlSchema, DataReaderError,> {
    let mut reader = Reader::from_reader(reader_input,);
    reader.config_mut().trim_text(true,);

    let mut buf = Vec::new();
    let mut _root_schema: Option<XmlSchema,> = None;
    let mut element_stack: Vec<(XmlSchema, HashMap<String, usize,>,),> = Vec::new();

    loop {
        match reader.read_event_into(&mut buf,) {
            Err(e,) => {
                return Err(DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                },);
            },
            Ok(Event::Eof,) => break,
            Ok(Event::Start(e,),) => {
                let tag_name = String::from_utf8_lossy(e.name().into_inner(),).to_string();
                let mut attributes = HashMap::new();
                for attr_result in e.attributes() {
                    let attr = attr_result.map_err(|e| DataReaderError::ParseError {
                        path:   file_path.to_path_buf(),
                        source: Box::new(e,),
                    },)?;
                    let key = String::from_utf8_lossy(attr.key.into_inner(),).to_string();
                    let value = String::from_utf8_lossy(&attr.value,).to_string();
                    attributes.insert(key, identify_nc_type(&value,),);
                }

                let new_schema = XmlSchema {
                    tag_name: tag_name.clone(),
                    attributes,
                    children: HashMap::new(),
                    has_text_content: false,
                    text_content_type: None,
                    min_occurs: 1,
                    max_occurs: Some(1,),
                };

                element_stack.push((new_schema, HashMap::new(),),);
            },
            Ok(Event::End(_,),) => {
                if let Some((child_schema, _,),) = element_stack.pop() {
                    if let Some((parent_schema, child_occurrence_counts,),) =
                        element_stack.last_mut()
                    {
                        let child_tag_name = child_schema.tag_name.clone();
                        *child_occurrence_counts
                            .entry(child_tag_name.clone(),)
                            .or_insert(0,) += 1;

                        let occurrences =
                            *child_occurrence_counts.get(&child_tag_name,).unwrap_or(&1,);

                        if occurrences == 1 {
                            parent_schema
                                .children
                                .insert(child_tag_name, XmlSchemaType::Element(child_schema,),);
                        } else {
                            let existing_entry = parent_schema
                                .children
                                .entry(child_tag_name,)
                                .or_insert_with(|| {
                                    XmlSchemaType::Array(Box::new(XmlSchema {
                                        tag_name:          child_schema.tag_name.clone(),
                                        attributes:        HashMap::new(),
                                        children:          HashMap::new(),
                                        has_text_content:  false,
                                        text_content_type: None,
                                        min_occurs:        0,
                                        max_occurs:        None,
                                    },),)
                                },);

                            if let XmlSchemaType::Array(existing_array_schema,) = existing_entry {
                                **existing_array_schema =
                                    merge_xml_schemas(existing_array_schema, &child_schema,);
                                existing_array_schema.min_occurs = 0;
                                existing_array_schema.max_occurs = None;
                            } else {
                                let mut merged_array_schema = merge_xml_schemas(
                                    &child_schema,
                                    &match existing_entry.clone() {
                                        XmlSchemaType::Element(s,) => s,
                                        _ => child_schema.clone(),
                                    },
                                );
                                merged_array_schema.min_occurs = 0;
                                merged_array_schema.max_occurs = None;
                                *existing_entry =
                                    XmlSchemaType::Array(Box::new(merged_array_schema,),);
                            }
                        }
                    } else {
                        _root_schema = Some(child_schema,);
                    }
                }
            },
            Ok(Event::Text(e,),) => {
                if let Some((current_schema, _,),) = element_stack.last_mut() {
                    let text = String::from_utf8_lossy(&e,).to_string();
                    if !text.trim().is_empty() {
                        current_schema.has_text_content = true;
                        let new_type = identify_nc_type(&text,);
                        current_schema.text_content_type = match &current_schema.text_content_type {
                            Some(prev_type,) => {
                                Some(merge_nc_types(prev_type.clone(), new_type,),)
                            },
                            None => Some(new_type,),
                        };
                    }
                }
            },
            Ok(Event::Empty(e,),) => {
                let tag_name = String::from_utf8_lossy(e.name().into_inner(),).to_string();
                let mut attributes = HashMap::new();
                for attr_result in e.attributes() {
                    let attr = attr_result.map_err(|e| DataReaderError::ParseError {
                        path:   file_path.to_path_buf(),
                        source: Box::new(e,),
                    },)?;
                    let key = String::from_utf8_lossy(attr.key.into_inner(),).to_string();
                    let value = String::from_utf8_lossy(&attr.value,).to_string();
                    attributes.insert(key, identify_nc_type(&value,),);
                }

                let new_schema = XmlSchema {
                    tag_name: tag_name.clone(),
                    attributes,
                    children: HashMap::new(),
                    has_text_content: false,
                    text_content_type: None,
                    min_occurs: 0,
                    max_occurs: Some(1,),
                };

                if let Some((parent_schema, child_occurrence_counts,),) = element_stack.last_mut() {
                    let child_tag_name = new_schema.tag_name.clone();
                    *child_occurrence_counts
                        .entry(child_tag_name.clone(),)
                        .or_insert(0,) += 1;

                    let occurrences = *child_occurrence_counts.get(&child_tag_name,).unwrap_or(&1,);

                    if occurrences == 1 {
                        parent_schema
                            .children
                            .insert(child_tag_name, XmlSchemaType::Element(new_schema,),);
                    } else {
                        let existing_entry = parent_schema
                            .children
                            .entry(child_tag_name,)
                            .or_insert_with(|| {
                                XmlSchemaType::Array(Box::new(XmlSchema {
                                    tag_name:          new_schema.tag_name.clone(),
                                    attributes:        HashMap::new(),
                                    children:          HashMap::new(),
                                    has_text_content:  false,
                                    text_content_type: None,
                                    min_occurs:        0,
                                    max_occurs:        None,
                                },),)
                            },);

                        if let XmlSchemaType::Array(existing_array_schema,) = existing_entry {
                            **existing_array_schema =
                                merge_xml_schemas(existing_array_schema, &new_schema,);
                            existing_array_schema.min_occurs = 0;
                            existing_array_schema.max_occurs = None;
                        } else {
                            let mut merged_array_schema = merge_xml_schemas(
                                &new_schema,
                                &match existing_entry.clone() {
                                    XmlSchemaType::Element(s,) => s,
                                    _ => new_schema.clone(),
                                },
                            );
                            merged_array_schema.min_occurs = 0;
                            merged_array_schema.max_occurs = None;
                            *existing_entry = XmlSchemaType::Array(Box::new(merged_array_schema,),);
                        }
                    }
                } else {
                    _root_schema = Some(new_schema,);
                }
            },
            _ => {},
        }
        buf.clear();
    }

    if let Some(schema,) = _root_schema {
        Ok(schema,)
    } else {
        Err(DataReaderError::ParseError {
            path:   file_path.to_path_buf(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Could not infer XML schema",
            ),),
        },)
    }
}

fn merge_xml_schemas(a: &XmlSchema, b: &XmlSchema,) -> XmlSchema {
    let mut all_keys: std::collections::HashSet<String,> = a.attributes.keys().cloned().collect();
    all_keys.extend(b.attributes.keys().cloned(),);

    let mut merged_attributes = HashMap::new();
    for key in all_keys {
        let type_a = a.attributes.get(&key,).cloned().unwrap_or(DataType::Null,);
        let type_b = b.attributes.get(&key,).cloned().unwrap_or(DataType::Null,);
        merged_attributes.insert(key, merge_nc_types(type_a, type_b,),);
    }

    let mut merged_children = a.children.clone();
    for (key, b_schema_type,) in &b.children {
        merged_children
            .entry(key.clone(),)
            .and_modify(
                |a_schema_type| match (a_schema_type.clone(), b_schema_type.clone(),) {
                    (XmlSchemaType::Element(a_child,), XmlSchemaType::Element(b_child,),) => {
                        *a_schema_type =
                            XmlSchemaType::Element(merge_xml_schemas(&a_child, &b_child,),);
                    },
                    (XmlSchemaType::Array(a_child,), XmlSchemaType::Array(b_child,),) => {
                        *a_schema_type = XmlSchemaType::Array(Box::new(merge_xml_schemas(
                            &a_child, &b_child,
                        ),),);
                    },
                    (XmlSchemaType::Element(a_child,), XmlSchemaType::Array(b_child,),) => {
                        *a_schema_type = XmlSchemaType::Array(Box::new(merge_xml_schemas(
                            &a_child, &b_child,
                        ),),);
                    },
                    (XmlSchemaType::Array(a_child,), XmlSchemaType::Element(b_child,),) => {
                        *a_schema_type = XmlSchemaType::Array(Box::new(merge_xml_schemas(
                            &a_child, &b_child,
                        ),),);
                    },
                    (XmlSchemaType::Union(mut v1,), XmlSchemaType::Union(v2,),) => {
                        for t in v2 {
                            if !v1.contains(&t,) {
                                v1.push(t,);
                            }
                        }
                        *a_schema_type = XmlSchemaType::Union(v1,);
                    },
                    (XmlSchemaType::Union(mut v,), other,) => {
                        if !v.contains(&other,) {
                            v.push(other,);
                        }
                        *a_schema_type = XmlSchemaType::Union(v,);
                    },
                    (other, XmlSchemaType::Union(mut v,),) => {
                        if !v.contains(&other,) {
                            v.push(other,);
                        }
                        *a_schema_type = XmlSchemaType::Union(v,);
                    },
                    (a_type, b_type,) => {
                        if a_type != b_type {
                            *a_schema_type = XmlSchemaType::Union(vec![a_type, b_type],);
                        }
                    },
                },
            )
            .or_insert(b_schema_type.clone(),);
    }

    XmlSchema {
        tag_name:          a.tag_name.clone(),
        attributes:        merged_attributes,
        children:          merged_children,
        has_text_content:  a.has_text_content || b.has_text_content,
        text_content_type: match (&a.text_content_type, &b.text_content_type,) {
            (Some(at,), Some(bt,),) => Some(merge_nc_types(at.clone(), bt.clone(),),),
            (Some(at,), None,) => Some(at.clone(),),
            (None, Some(bt,),) => Some(bt.clone(),),
            (None, None,) => None,
        },
        min_occurs:        std::cmp::min(a.min_occurs, b.min_occurs,),
        max_occurs:        match (a.max_occurs, b.max_occurs,) {
            (Some(ma,), Some(mb,),) => Some(std::cmp::max(ma, mb,),),
            (None, Some(_,),) => None,
            (Some(_,), None,) => None,
            (None, None,) => None,
        },
    }
}

pub fn read_xml_content(
    file_path: &Path,
    head: Option<usize,>,
) -> Result<XmlData, DataReaderError,> {
    let num_lines_to_extract = head.unwrap_or(0,);

    let first_lines: Option<Vec<String,>,> = if num_lines_to_extract > 0 {
        use std::io::{BufRead, BufReader};
        let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
        let decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
            path: file_path.to_path_buf(),
            source: e,
        })?;
        let reader = BufReader::new(decoder,);
        let lines: Vec<String,> = reader
            .lines()
            .take(num_lines_to_extract,)
            .filter_map(|l| l.ok(),)
            .collect();
        if lines.is_empty() { None } else { Some(lines,) }
    } else {
        None
    };

    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;
    let decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;
    let buf_reader = BufReader::new(decoder,);
    let mut reader = Reader::from_reader(buf_reader,);
    reader.config_mut().trim_text(true,);

    let mut buf = Vec::new();
    let mut root_element: Option<String,> = None;
    let mut element_counts: HashMap<String, usize,> = HashMap::new();

    loop {
        match reader.read_event_into(&mut buf,) {
            Err(e,) => {
                return Err(DataReaderError::ParseError {
                    path:   file_path.to_path_buf(),
                    source: Box::new(e,),
                },);
            },
            Ok(Event::Eof,) => break,
            Ok(Event::Start(e,),) => {
                let tag_name = String::from_utf8_lossy(e.name().into_inner(),).to_string();
                if root_element.is_none() {
                    root_element = Some(tag_name.clone(),);
                }
                *element_counts.entry(tag_name,).or_insert(0,) += 1;
            },
            Ok(Event::Empty(e,),) => {
                let tag_name = String::from_utf8_lossy(e.name().into_inner(),).to_string();
                if root_element.is_none() {
                    root_element = Some(tag_name.clone(),);
                }
                *element_counts.entry(tag_name,).or_insert(0,) += 1;
            },
            _ => {},
        }
        buf.clear();
    }

    let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
        path:   file_path.to_path_buf(),
        source: e,
    },)?;
    let decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
        path: file_path.to_path_buf(),
        source: e,
    })?;
    let buf_reader = BufReader::new(decoder,);
    let inferred_schema = infer_xml_schema(buf_reader, file_path,).ok();

    let content = if file_path.metadata().map(|m| m.len(),).unwrap_or(0,) < 10 * 1024 * 1024 {
        let file = File::open(file_path,).map_err(|e| DataReaderError::FileReadError {
            path:   file_path.to_path_buf(),
            source: e,
        },)?;
        let mut decoder = crate::reader::charset::get_decoded_reader(file).map_err(|e| DataReaderError::FileReadError {
            path: file_path.to_path_buf(),
            source: e,
        })?;
        let mut s = String::new();
        if decoder.read_to_string(&mut s).is_err() {
            "[Error reading content]".to_string()
        } else {
            s
        }
    } else {
        "[Content too large for memory, use streaming or head for details]".to_string()
    };

    Ok(XmlData {
        content,
        root_element,
        element_counts,
        first_lines,
        inferred_schema,
    },)
}
