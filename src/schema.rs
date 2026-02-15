use serde::{Deserialize, Serialize};
use serde_json::Value;
use regex::Regex;
use std::collections::{HashMap, BTreeSet};
use once_cell::sync::OnceCell;

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
    #[serde(skip)]
    pub compiled_pattern: OnceCell<Regex>,
    
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

// Helper struct for comparing Values without full serialization
#[derive(Eq)]
struct OrdValue<'a>(&'a Value);

impl<'a> PartialEq for OrdValue<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl<'a> PartialOrd for OrdValue<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for OrdValue<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use Value::*;
        match (self.0, other.0) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Null, _) => std::cmp::Ordering::Less,
            (_, Null) => std::cmp::Ordering::Greater,

            (Bool(a), Bool(b)) => a.cmp(b),
            (Bool(_), _) => std::cmp::Ordering::Less,
            (_, Bool(_)) => std::cmp::Ordering::Greater,

            (Number(a), Number(b)) => {
                // To maintain backward compatibility with previous implementation which relied on to_string(),
                // we compare string representations of numbers.
                // This ensures that 1 and 1.0 are treated as different (legacy behavior),
                // while avoiding full object serialization for complex structures.
                a.to_string().cmp(&b.to_string())
            },
            (Number(_), _) => std::cmp::Ordering::Less,
            (_, Number(_)) => std::cmp::Ordering::Greater,

            (String(a), String(b)) => a.cmp(b),
            (String(_), _) => std::cmp::Ordering::Less,
            (_, String(_)) => std::cmp::Ordering::Greater,

            (Array(a), Array(b)) => {
                for (ia, ib) in a.iter().zip(b.iter()) {
                    let ord = OrdValue(ia).cmp(&OrdValue(ib));
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                a.len().cmp(&b.len())
            }
            (Array(_), _) => std::cmp::Ordering::Less,
            (_, Array(_)) => std::cmp::Ordering::Greater,

            (Object(a), Object(b)) => {
                // serde_json::Map is backed by BTreeMap by default (sorted keys)
                // If "preserve_order" feature is used, it might be IndexMap.
                // In either case, iterating follows the map's order.
                // The legacy implementation relied on to_string(), which follows iteration order.
                // So we iterate and compare.
                let mut i_a = a.iter();
                let mut i_b = b.iter();
                loop {
                    match (i_a.next(), i_b.next()) {
                        (Some((ka, va)), Some((kb, vb))) => {
                            let k_ord = ka.cmp(kb);
                            if k_ord != std::cmp::Ordering::Equal {
                                return k_ord;
                            }
                            let v_ord = OrdValue(va).cmp(&OrdValue(vb));
                            if v_ord != std::cmp::Ordering::Equal {
                                return v_ord;
                            }
                        }
                        (Some(_), None) => return std::cmp::Ordering::Greater,
                        (None, Some(_)) => return std::cmp::Ordering::Less,
                        (None, None) => return std::cmp::Ordering::Equal,
                    }
                }
            }
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
                let re = schema.compiled_pattern.get_or_try_init(|| {
                    Regex::new(pattern_str).map_err(|_| ValidationError::PatternMismatch(pattern_str.clone()))
                })?;
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
                let mut seen = BTreeSet::new();
                for item in arr {
                    if !seen.insert(OrdValue(item)) {
                        return Err(ValidationError::UniqueItems);
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_unique_items_simple() {
        let schema = Schema {
            schema_type: SchemaType::Array,
            properties: None,
            required: None,
            min_length: None,
            max_length: None,
            pattern: None,
            compiled_pattern: OnceCell::new(),
            minimum: None,
            maximum: None,
            exclusive_minimum: None,
            exclusive_maximum: None,
            items: None,
            min_items: None,
            max_items: None,
            unique_items: Some(true),
            r#enum: None,
        };

        let valid = json!([1, 2, 3]);
        assert!(validate(&valid, &schema).is_ok());

        let invalid = json!([1, 2, 1]);
        assert!(validate(&invalid, &schema).is_err());
    }

    #[test]
    fn test_unique_items_objects() {
        let schema = Schema {
            schema_type: SchemaType::Array,
            properties: None,
            required: None,
            min_length: None,
            max_length: None,
            pattern: None,
            compiled_pattern: OnceCell::new(),
            minimum: None,
            maximum: None,
            exclusive_minimum: None,
            exclusive_maximum: None,
            items: None,
            min_items: None,
            max_items: None,
            unique_items: Some(true),
            r#enum: None,
        };

        let valid = json!([
            {"a": 1, "b": 2},
            {"a": 1, "b": 3}
        ]);
        assert!(validate(&valid, &schema).is_ok());

        let invalid = json!([
            {"a": 1, "b": 2},
            {"a": 1, "b": 2}
        ]);
        assert!(validate(&invalid, &schema).is_err());
    }

    #[test]
    fn test_unique_items_nested() {
        let schema = Schema {
            schema_type: SchemaType::Array,
            properties: None,
            required: None,
            min_length: None,
            max_length: None,
            pattern: None,
            compiled_pattern: OnceCell::new(),
            minimum: None,
            maximum: None,
            exclusive_minimum: None,
            exclusive_maximum: None,
            items: None,
            min_items: None,
            max_items: None,
            unique_items: Some(true),
            r#enum: None,
        };

        let valid = json!([
            {"a": {"b": 1}},
            {"a": {"b": 2}}
        ]);
        assert!(validate(&valid, &schema).is_ok());

        let invalid = json!([
            {"a": {"b": 1}},
            {"a": {"b": 1}}
        ]);
        assert!(validate(&invalid, &schema).is_err());
    }

    #[test]
    fn test_unique_items_number_types() {
        let schema = Schema {
            schema_type: SchemaType::Array,
            properties: None,
            required: None,
            min_length: None,
            max_length: None,
            pattern: None,
            compiled_pattern: OnceCell::new(),
            minimum: None,
            maximum: None,
            exclusive_minimum: None,
            exclusive_maximum: None,
            items: None,
            min_items: None,
            max_items: None,
            unique_items: Some(true),
            r#enum: None,
        };

        // 1 and 1.0 are different in to_string() representation, hence unique
        let valid = json!([1, 1.0]);
        assert!(validate(&valid, &schema).is_ok());
    }
}
