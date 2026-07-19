use crate::types::Value;

pub fn call_function(name: &str, args: &[Value]) -> Value {
    let upper = name.to_uppercase();
    match upper.as_str() {
        // String functions
        "UPPER" => args.first().map_or(Value::Null, |v| match v {
            Value::Text(s) => Value::Text(s.to_uppercase()),
            _ => Value::Null,
        }),
        "LOWER" => args.first().map_or(Value::Null, |v| match v {
            Value::Text(s) => Value::Text(s.to_lowercase()),
            _ => Value::Null,
        }),
        "LENGTH" | "LEN" => args.first().map_or(Value::Null, |v| match v {
            Value::Text(s) => Value::Integer(s.len() as i64),
            Value::Blob(b) => Value::Integer(b.len() as i64),
            Value::Null => Value::Null,
            _ => Value::Integer(format!("{}", v).len() as i64),
        }),
        "TRIM" => args.first().map_or(Value::Null, |v| match v {
            Value::Text(s) => Value::Text(s.trim().to_string()),
            _ => Value::Null,
        }),
        "LTRIM" => args.first().map_or(Value::Null, |v| match v {
            Value::Text(s) => Value::Text(s.trim_start().to_string()),
            _ => Value::Null,
        }),
        "RTRIM" => args.first().map_or(Value::Null, |v| match v {
            Value::Text(s) => Value::Text(s.trim_end().to_string()),
            _ => Value::Null,
        }),
        "SUBSTR" | "SUBSTRING" => {
            if let Some(Value::Text(s)) = args.first() {
                let start = args.get(1).and_then(Value::as_i64).unwrap_or(1) as usize;
                let length = args.get(2).and_then(Value::as_i64);
                let start0 = if start > 0 { start - 1 } else { 0 };
                if let Some(len) = length {
                    Value::Text(s.chars().skip(start0).take(len as usize).collect())
                } else {
                    Value::Text(s.chars().skip(start0).collect())
                }
            } else {
                Value::Null
            }
        }
        "REPLACE" => {
            if let (Some(Value::Text(s)), Some(Value::Text(from)), Some(Value::Text(to))) =
                (args.first(), args.get(1), args.get(2))
            {
                Value::Text(s.replace(from, to))
            } else {
                Value::Null
            }
        }
        "INSTR" => {
            if let (Some(Value::Text(s)), Some(Value::Text(sub))) = (args.first(), args.get(1)) {
                Value::Integer(s.find(sub).map(|p| p as i64 + 1).unwrap_or(0))
            } else {
                Value::Null
            }
        }
        "QUOTE" => args.first().map_or(Value::Null, |v| Value::Text(format!("'{}'", v))),

        // Math functions
        "ABS" => args.first().map_or(Value::Null, |v| match v {
            Value::Integer(i) => Value::Integer(i.abs()),
            Value::Real(f) => Value::Real(f.abs()),
            _ => Value::Null,
        }),
        "ROUND" => {
            let val = args.first().and_then(Value::as_f64).unwrap_or(0.0);
            let digits = args.get(1).and_then(Value::as_i64).unwrap_or(0) as i32;
            let factor = 10f64.powi(digits);
            let rounded = (val * factor).round() / factor;
            Value::Real(rounded)
        }
        "RANDOM" => Value::Integer(random_i64()),
        "MAX" => {
            if let Some(first) = args.first() {
                args.iter().skip(1).fold(first.clone(), |acc, v| {
                    if compare_for_max(&acc, v) { acc } else { v.clone() }
                })
            } else {
                Value::Null
            }
        }
        "MIN" => {
            if let Some(first) = args.first() {
                args.iter().skip(1).fold(first.clone(), |acc, v| {
                    if compare_for_min(&acc, v) { acc } else { v.clone() }
                })
            } else {
                Value::Null
            }
        }

        // Type functions
        "TYPEOF" => args.first().map_or(Value::Null, |v| Value::Text(v.type_name().to_string())),
        "COALESCE" => args.iter().find(|v| !v.is_null()).cloned().unwrap_or(Value::Null),
        "IFNULL" => args
            .iter()
            .find(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| args.last().cloned().unwrap_or(Value::Null)),
        "NULLIF" => {
            if let (Some(a), Some(b)) = (args.first(), args.get(1)) {
                if a == b { Value::Null } else { a.clone() }
            } else {
                Value::Null
            }
        }
        "LAST_INSERT_ROWID" => Value::Integer(0), // overridden by executor context

        // Date/time stubs
        "DATE" | "TIME" | "DATETIME" | "JULIANDAY" | "STRFTIME" => Value::Text("".to_string()),

        // Default: treat as null
        _ => Value::Null,
    }
}

fn compare_for_max(a: &Value, b: &Value) -> bool {
    use crate::types::compare_values;
    compare_values(a, b) != std::cmp::Ordering::Less
}

fn compare_for_min(a: &Value, b: &Value) -> bool {
    use crate::types::compare_values;
    compare_values(a, b) != std::cmp::Ordering::Greater
}

fn random_i64() -> i64 {
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (seed >> 33) as i64
}
