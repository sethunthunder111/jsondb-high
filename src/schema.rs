use serde::{Deserialize, Serialize};
use serde_json::Value;
use regex::Regex;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum SchemaType {
    Object,
    Array,
    String,
    Number,
    Boolean,
    Null,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub properties: Option<HashMap<String, Schema>>,
    pub required: Option<Vec<String>>,
    
    // String constraints
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub pattern: Option<String>,
    
    // Number constraints
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub exclusive_minimum: Option<f64>,
    pub exclusive_maximum: Option<f64>,
    
    // Array constraints
    pub items: Option<Box<Schema>>,
    pub min_items: Option<usize>,
    pub max_items: Option<usize>,
    pub unique_items: Option<bool>,
    
    // Enum
    pub r#enum: Option<Vec<Value>>,
}

#[derive(Debug)]
pub enum ValidationError {
    TypeMismatch { expected: SchemaType, found: String },
    MissingRequired(String),
    MinLength(usize),
    MaxLength(usize),
    PatternMismatch(String),
    Minimum(f64),
    Maximum(f64),
    MinItems(usize),
    MaxItems(usize),
    UniqueItems,
    EnumMismatch,
    PropertyError(String, Box<ValidationError>),
    ItemError(usize, Box<ValidationError>),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::TypeMismatch { expected, found } => write!(f, "Type mismatch: expected {:?}, found {}", expected, found),
            ValidationError::MissingRequired(prop) => write!(f, "Missing required property: {}", prop),
            ValidationError::MinLength(len) => write!(f, "String too short: min length {}", len),
            ValidationError::MaxLength(len) => write!(f, "String too long: max length {}", len),
            ValidationError::PatternMismatch(p) => write!(f, "String does not match pattern: {}", p),
            ValidationError::Minimum(val) => write!(f, "Value too small: min {}", val),
            ValidationError::Maximum(val) => write!(f, "Value too large: max {}", val),
            ValidationError::MinItems(len) => write!(f, "Array too short: min items {}", len),
            ValidationError::MaxItems(len) => write!(f, "Array too long: max items {}", len),
            ValidationError::UniqueItems => write!(f, "Array items must be unique"),
            ValidationError::EnumMismatch => write!(f, "Value not in allowed enum"),
            ValidationError::PropertyError(prop, err) => write!(f, "In property '{}': {}", prop, err),
            ValidationError::ItemError(idx, err) => write!(f, "In item {}: {}", idx, err),
        }
    }
}

pub fn validate(value: &Value, schema: &Schema) -> Result<(), ValidationError> {
    // 1. Check type
    match (&schema.schema_type, value) {
        (SchemaType::Object, Value::Object(_)) => {}
        (SchemaType::Array, Value::Array(_)) => {}
        (SchemaType::String, Value::String(_)) => {}
        (SchemaType::Number, Value::Number(_)) => {}
        (SchemaType::Boolean, Value::Bool(_)) => {}
        (SchemaType::Null, Value::Null) => {}
        (expected, found) => {
            let found_str = match found {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            return Err(ValidationError::TypeMismatch { expected: expected.clone(), found: found_str.to_string() });
        }
    }

    // 2. Enum check
    if let Some(allowed) = &schema.r#enum {
        if !allowed.contains(value) {
            return Err(ValidationError::EnumMismatch);
        }
    }

    // 3. Detailed constraints
    match value {
        Value::String(s) => {
            if let Some(min) = schema.min_length {
                if s.len() < min { return Err(ValidationError::MinLength(min)); }
            }
            if let Some(max) = schema.max_length {
                if s.len() > max { return Err(ValidationError::MaxLength(max)); }
            }
            if let Some(pattern_str) = &schema.pattern {
                let re = Regex::new(pattern_str).map_err(|_| ValidationError::PatternMismatch(pattern_str.clone()))?;
                if !re.is_match(s) {
                    return Err(ValidationError::PatternMismatch(pattern_str.clone()));
                }
            }
        }
        Value::Number(n) => {
            if let Some(val) = n.as_f64() {
                if let Some(min) = schema.minimum {
                    if val < min { return Err(ValidationError::Minimum(min)); }
                }
                if let Some(max) = schema.maximum {
                    if val > max { return Err(ValidationError::Maximum(max)); }
                }
                if let Some(emin) = schema.exclusive_minimum {
                    if val <= emin { return Err(ValidationError::Minimum(emin)); }
                }
                if let Some(emax) = schema.exclusive_maximum {
                    if val >= emax { return Err(ValidationError::Maximum(emax)); }
                }
            }
        }
        Value::Array(arr) => {
            if let Some(min) = schema.min_items {
                if arr.len() < min { return Err(ValidationError::MinItems(min)); }
            }
            if let Some(max) = schema.max_items {
                if arr.len() > max { return Err(ValidationError::MaxItems(max)); }
            }
            if let Some(true) = schema.unique_items {
                let mut unique = arr.clone();
                unique.sort_by(|a, b| a.to_string().cmp(&b.to_string())); // Simple unique check
                let original_len = arr.len();
                unique.dedup();
                if unique.len() < original_len {
                    return Err(ValidationError::UniqueItems);
                }
            }
            if let Some(item_schema) = &schema.items {
                for (i, item) in arr.iter().enumerate() {
                    validate(item, item_schema).map_err(|e| ValidationError::ItemError(i, Box::new(e)))?;
                }
            }
        }
        Value::Object(obj) => {
            if let Some(required) = &schema.required {
                for req in required {
                    if !obj.contains_key(req) {
                        return Err(ValidationError::MissingRequired(req.clone()));
                    }
                }
            }
            if let Some(props) = &schema.properties {
                for (key, prop_schema) in props {
                    if let Some(val) = obj.get(key) {
                        validate(val, prop_schema).map_err(|e| ValidationError::PropertyError(key.clone(), Box::new(e)))?;
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}
