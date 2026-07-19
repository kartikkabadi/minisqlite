use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Integer(_) => "INTEGER",
            Value::Real(_) => "REAL",
            Value::Text(_) => "TEXT",
            Value::Blob(_) => "BLOB",
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Real(f) => Some(*f as i64),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Real(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Integer(i) => *i != 0,
            Value::Real(f) => *f != 0.0,
            Value::Text(s) => !s.is_empty(),
            Value::Blob(b) => !b.is_empty(),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(compare_values(self, other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_values(self, other)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Real(r) => {
                if r.fract() == 0.0 && r.abs() < 1e15 {
                    write!(f, "{:.1}", r)
                } else {
                    write!(f, "{}", r)
                }
            }
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => {
                write!(f, "x'")?;
                for byte in b {
                    write!(f, "{:02x}", byte)?;
                }
                write!(f, "'")
            }
        }
    }
}

pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.total_cmp(y),
        (Value::Integer(x), Value::Real(y)) => (*x as f64).total_cmp(y),
        (Value::Real(x), Value::Integer(y)) => x.total_cmp(&(*y as f64)),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        (Value::Integer(_) | Value::Real(_), Value::Text(_)) => Ordering::Less,
        (Value::Text(_), Value::Integer(_) | Value::Real(_)) => Ordering::Greater,
        (Value::Blob(_), _) => Ordering::Greater,
        (_, Value::Blob(_)) => Ordering::Less,
    }
}

pub fn encode_value(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match v {
        Value::Null => out.push(0),
        Value::Integer(i) => {
            out.push(1);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::Real(f) => {
            out.push(2);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Text(s) => {
            out.push(3);
            out.extend_from_slice(&(s.len() as u32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Blob(b) => {
            out.push(4);
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
    }
    out
}

pub fn decode_value(bytes: &[u8], offset: &mut usize) -> Option<Value> {
    if *offset >= bytes.len() {
        return None;
    }
    let tag = bytes[*offset];
    *offset += 1;
    match tag {
        0 => Some(Value::Null),
        1 => {
            if *offset + 8 > bytes.len() { return None; }
            let v = i64::from_be_bytes([
                bytes[*offset], bytes[*offset + 1], bytes[*offset + 2], bytes[*offset + 3],
                bytes[*offset + 4], bytes[*offset + 5], bytes[*offset + 6], bytes[*offset + 7],
            ]);
            *offset += 8;
            Some(Value::Integer(v))
        }
        2 => {
            if *offset + 8 > bytes.len() { return None; }
            let bits = u64::from_be_bytes([
                bytes[*offset], bytes[*offset + 1], bytes[*offset + 2], bytes[*offset + 3],
                bytes[*offset + 4], bytes[*offset + 5], bytes[*offset + 6], bytes[*offset + 7],
            ]);
            *offset += 8;
            Some(Value::Real(f64::from_bits(bits)))
        }
        3 => {
            if *offset + 4 > bytes.len() { return None; }
            let len = u32::from_be_bytes([
                bytes[*offset], bytes[*offset + 1], bytes[*offset + 2], bytes[*offset + 3],
            ]) as usize;
            *offset += 4;
            if *offset + len > bytes.len() { return None; }
            let s = String::from_utf8_lossy(&bytes[*offset..*offset + len]).to_string();
            *offset += len;
            Some(Value::Text(s))
        }
        4 => {
            if *offset + 4 > bytes.len() { return None; }
            let len = u32::from_be_bytes([
                bytes[*offset], bytes[*offset + 1], bytes[*offset + 2], bytes[*offset + 3],
            ]) as usize;
            *offset += 4;
            if *offset + len > bytes.len() { return None; }
            let b = bytes[*offset..*offset + len].to_vec();
            *offset += len;
            Some(Value::Blob(b))
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeAffinity {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

impl TypeAffinity {
    pub fn from_type_name(name: &str) -> Self {
        let upper = name.to_uppercase();
        if upper.contains("INT") {
            TypeAffinity::Integer
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            TypeAffinity::Text
        } else if upper.contains("BLOB") || upper.is_empty() {
            TypeAffinity::Blob
        } else if upper.contains("REAL")
            || upper.contains("FLOA")
            || upper.contains("DOUB")
        {
            TypeAffinity::Real
        } else {
            TypeAffinity::Numeric
        }
    }

    pub fn apply(&self, val: &Value) -> Value {
        match self {
            TypeAffinity::Integer => match val {
                Value::Text(s) => s.parse::<i64>().map(Value::Integer).unwrap_or_else(|_| val.clone()),
                Value::Real(f) => Value::Integer(*f as i64),
                _ => val.clone(),
            },
            TypeAffinity::Real => match val {
                Value::Integer(i) => Value::Real(*i as f64),
                Value::Text(s) => s.parse::<f64>().map(Value::Real).unwrap_or_else(|_| val.clone()),
                _ => val.clone(),
            },
            TypeAffinity::Text => match val {
                Value::Integer(i) => Value::Text(i.to_string()),
                Value::Real(f) => Value::Text(f.to_string()),
                _ => val.clone(),
            },
            TypeAffinity::Numeric => match val {
                Value::Text(s) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Integer(i)
                    } else if let Ok(f) = s.parse::<f64>() {
                        Value::Real(f)
                    } else {
                        val.clone()
                    }
                }
                _ => val.clone(),
            },
            TypeAffinity::Blob => val.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: String,
    pub affinity: TypeAffinity,
    pub primary_key: bool,
    pub autoincrement: bool,
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<Value>,
    pub default_expr: Option<crate::sql::Expr>,
    pub check_expr: Option<crate::sql::Expr>,
}

impl ColumnDef {
    pub fn new(name: &str, type_name: &str) -> Self {
        ColumnDef {
            name: name.to_string(),
            type_name: type_name.to_string(),
            affinity: TypeAffinity::from_type_name(type_name),
            primary_key: false,
            autoincrement: false,
            not_null: false,
            unique: false,
            default: None,
            default_expr: None,
            check_expr: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub root_page: u32,
    pub autoinc_counter: i64,
}

impl TableSchema {
    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub root_page: u32,
}

pub type Row = Vec<Value>;
