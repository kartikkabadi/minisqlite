use crate::types::{ColumnDef, IndexSchema, TableSchema, TypeAffinity, Value, decode_value, encode_value};

pub fn encode_table_schema(schema: &TableSchema) -> Vec<u8> {
    let mut out = Vec::new();
    write_string(&mut out, &schema.name);
    out.extend_from_slice(&(schema.columns.len() as u32).to_be_bytes());
    for col in &schema.columns {
        write_string(&mut out, &col.name);
        write_string(&mut out, &col.type_name);
        out.push(col.affinity.clone() as u8);
        out.push(pack_flags(col));
        if let Some(d) = &col.default {
            out.push(1);
            out.extend_from_slice(&encode_value(d));
        } else {
            out.push(0);
        }
        // check_expr is not persisted for simplicity
    }
    out.extend_from_slice(&schema.root_page.to_be_bytes());
    out.extend_from_slice(&schema.autoinc_counter.to_be_bytes());
    out
}

pub fn decode_table_schema(bytes: &[u8]) -> Option<TableSchema> {
    let mut off = 0;
    let name = read_string(bytes, &mut off)?;
    let col_count = read_u32(bytes, &mut off)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let col_name = read_string(bytes, &mut off)?;
        let type_name = read_string(bytes, &mut off)?;
        let aff = bytes.get(off).copied()?;
        off += 1;
        let flags = bytes.get(off).copied()?;
        off += 1;
        let mut col = ColumnDef::new(&col_name, &type_name);
        col.affinity = affinity_from_u8(aff);
        unpack_flags(flags, &mut col);
        let has_default = bytes.get(off).copied()?;
        off += 1;
        if has_default != 0 {
            col.default = decode_value(bytes, &mut off);
        }
        columns.push(col);
    }
    let root_page = read_u32(bytes, &mut off)?;
    let autoinc_counter = read_i64(bytes, &mut off)?;
    Some(TableSchema {
        name,
        columns,
        root_page,
        autoinc_counter,
    })
}

pub fn encode_index_schema(schema: &IndexSchema) -> Vec<u8> {
    let mut out = Vec::new();
    write_string(&mut out, &schema.name);
    write_string(&mut out, &schema.table_name);
    out.extend_from_slice(&(schema.columns.len() as u32).to_be_bytes());
    for col in &schema.columns {
        write_string(&mut out, col);
    }
    out.push(if schema.unique { 1 } else { 0 });
    out.extend_from_slice(&schema.root_page.to_be_bytes());
    out
}

pub fn decode_index_schema(bytes: &[u8]) -> Option<IndexSchema> {
    let mut off = 0;
    let name = read_string(bytes, &mut off)?;
    let table_name = read_string(bytes, &mut off)?;
    let col_count = read_u32(bytes, &mut off)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        columns.push(read_string(bytes, &mut off)?);
    }
    let unique = bytes.get(off).copied()? != 0;
    off += 1;
    let root_page = read_u32(bytes, &mut off)?;
    Some(IndexSchema {
        name,
        table_name,
        columns,
        unique,
        root_page,
    })
}

fn pack_flags(col: &ColumnDef) -> u8 {
    let mut f = 0u8;
    if col.primary_key { f |= 0x01; }
    if col.autoincrement { f |= 0x02; }
    if col.not_null { f |= 0x04; }
    if col.unique { f |= 0x08; }
    f
}

fn unpack_flags(f: u8, col: &mut ColumnDef) {
    col.primary_key = f & 0x01 != 0;
    col.autoincrement = f & 0x02 != 0;
    col.not_null = f & 0x04 != 0;
    col.unique = f & 0x08 != 0;
}

fn affinity_from_u8(v: u8) -> TypeAffinity {
    match v {
        0 => TypeAffinity::Integer,
        1 => TypeAffinity::Real,
        2 => TypeAffinity::Text,
        3 => TypeAffinity::Blob,
        _ => TypeAffinity::Numeric,
    }
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_string(bytes: &[u8], off: &mut usize) -> Option<String> {
    let len = read_u32(bytes, off)? as usize;
    if *off + len > bytes.len() { return None; }
    let s = String::from_utf8_lossy(&bytes[*off..*off + len]).to_string();
    *off += len;
    Some(s)
}

fn read_u32(bytes: &[u8], off: &mut usize) -> Option<u32> {
    if *off + 4 > bytes.len() { return None; }
    let v = u32::from_be_bytes([bytes[*off], bytes[*off + 1], bytes[*off + 2], bytes[*off + 3]]);
    *off += 4;
    Some(v)
}

fn read_i64(bytes: &[u8], off: &mut usize) -> Option<i64> {
    if *off + 8 > bytes.len() { return None; }
    let v = i64::from_be_bytes([
        bytes[*off], bytes[*off + 1], bytes[*off + 2], bytes[*off + 3],
        bytes[*off + 4], bytes[*off + 5], bytes[*off + 6], bytes[*off + 7],
    ]);
    *off += 8;
    Some(v)
}
