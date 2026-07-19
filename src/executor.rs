use crate::btree::BTree;
use crate::catalog::{decode_index_schema, decode_table_schema, encode_index_schema, encode_table_schema};
use crate::pager::Pager;
use crate::sql::{Assignment, BinOp, ColSpec, ColumnConstraint, Expr, JoinClause, JoinType, Order, SelectCol, SelectStmt, Statement, TableConstraint, TableRef, UnaryOp};
use crate::types::{ColumnDef, IndexSchema, Row, TableSchema, TypeAffinity, Value, compare_values, decode_value, encode_value};
use crate::functions::call_function;
use crate::wal::WriteAheadLog;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::rc::Rc;

#[derive(Debug)]
struct DatabaseState {
    tables: HashMap<String, Table>,
    indexes: HashMap<String, Index>,
}

#[derive(Debug)]
pub struct Database {
    pager: Pager,
    wal: WriteAheadLog,
    tables: HashMap<String, Table>,
    indexes: HashMap<String, Index>,
    in_transaction: bool,
    transaction_backup: Option<DatabaseState>,
    pub last_insert_rowid: i64,
    db_path: String,
}

#[derive(Debug, Clone)]
struct Table {
    schema: TableSchema,
    btree: BTree,
    next_rowid: i64,
}

#[derive(Debug, Clone)]
struct Index {
    schema: IndexSchema,
    btree: BTree,
}

#[derive(Debug)]
pub enum ExecuteResult {
    Ok(String),
    Rows { header: Vec<String>, rows: Vec<Row> },
}

#[derive(Clone)]
struct SourceRow {
    table_name: String,
    alias: Option<String>,
    schema: Rc<TableSchema>,
    rowid: i64,
    values: Row,
}

struct EvalContext<'a> {
    rows: &'a [SourceRow],
    aliases: &'a HashMap<String, Value>,
}

impl<'a> EvalContext<'a> {
    fn new(rows: &'a [SourceRow], aliases: &'a HashMap<String, Value>) -> Self {
        Self { rows, aliases }
    }
}

impl Database {
    pub fn open(path: &str) -> io::Result<Self> {
        let pager = Pager::open(path)?;
        let wal = WriteAheadLog::open(path)?;
        let mut db = Database {
            pager,
            wal,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            in_transaction: false,
            transaction_backup: None,
            last_insert_rowid: 0,
            db_path: path.to_string(),
        };

        if db.pager.page_count == 1 {
            let mut catalog = BTree::create(&mut db.pager)?;
            catalog.flush(&mut db.pager)?;
            db.pager.catalog_root = catalog.root();
            db.pager.flush()?;
        }

        db.load_catalog()?;

        for entry in db.wal.get_recovery_entries().iter() {
            db.pager.write_page(entry.page_num, &entry.data)?;
        }
        if !db.wal.get_recovery_entries().is_empty() {
            db.pager.flush()?;
            db.wal.rollback()?;
        }

        Ok(db)
    }

    fn load_catalog(&mut self) -> io::Result<()> {
        let catalog_root = self.pager.catalog_root;
        if catalog_root == 0 {
            return Ok(());
        }
        let mut catalog = BTree::open(catalog_root);
        catalog.load(&mut self.pager)?;

        self.tables.clear();
        self.indexes.clear();

        for (key, payload) in catalog.scan() {
            if key.starts_with(b"table:") {
                if let Some(schema) = decode_table_schema(payload) {
                    let mut btree = BTree::open(schema.root_page);
                    btree.load(&mut self.pager)?;
                    let next_rowid = btree.scan().map(|(k, _)| decode_rowid_key(k)).max().unwrap_or(0) + 1;
                    self.tables.insert(
                        schema.name.clone(),
                        Table { schema, btree, next_rowid },
                    );
                }
            } else if key.starts_with(b"index:") {
                if let Some(schema) = decode_index_schema(payload) {
                    let mut btree = BTree::open(schema.root_page);
                    btree.load(&mut self.pager)?;
                    self.indexes.insert(schema.name.clone(), Index { schema, btree });
                }
            }
        }
        Ok(())
    }

    fn save_catalog(&mut self) -> io::Result<()> {
        let mut catalog = BTree::open(self.pager.catalog_root);
        for (name, table) in &mut self.tables {
            let new_root = table.btree.flush(&mut self.pager)?;
            table.schema.root_page = new_root;
            let key = make_key(b"table:", name);
            catalog.insert_kv(key, encode_table_schema(&table.schema));
        }
        for (name, index) in &mut self.indexes {
            let new_root = index.btree.flush(&mut self.pager)?;
            index.schema.root_page = new_root;
            let key = make_key(b"index:", name);
            catalog.insert_kv(key, encode_index_schema(&index.schema));
        }
        self.pager.catalog_root = catalog.flush(&mut self.pager)?;
        Ok(())
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<ExecuteResult, String> {
        match stmt {
            Statement::CreateTable { name, columns, if_not_exists, constraints } => {
                self.create_table(name, columns, constraints, if_not_exists)
            }
            Statement::DropTable { name, if_exists } => self.drop_table(name, if_exists),
            Statement::CreateIndex { name, table, columns, unique, if_not_exists } => {
                self.create_index(name, table, columns, unique, if_not_exists)
            }
            Statement::DropIndex { name, if_exists } => self.drop_index(name, if_exists),
            Statement::Insert { table, columns, values, or_replace } => {
                self.insert(table, columns, values, or_replace)
            }
            Statement::Select(select) => self.select(select),
            Statement::Update { table, assignments, where_clause } => {
                self.update(table, assignments, where_clause)
            }
            Statement::Delete { table, where_clause } => self.delete(table, where_clause),
            Statement::AlterAddColumn { table, column } => self.alter_add_column(table, column),
            Statement::Begin => self.begin(),
            Statement::Commit => self.commit(),
            Statement::Rollback => self.rollback(),
            Statement::Pragma { name, value } => self.pragma(name, value),
            Statement::Vacuum => self.vacuum(),
            Statement::Explain(stmt) => self.explain(*stmt),
        }
    }

    fn create_table(&mut self, name: String, columns: Vec<ColSpec>, table_constraints: Vec<TableConstraint>, if_not_exists: bool) -> Result<ExecuteResult, String> {
        if self.tables.contains_key(&name) {
            if if_not_exists {
                return Ok(ExecuteResult::Ok(format!("Table {} already exists", name)));
            } else {
                return Err(format!("Table {} already exists", name));
            }
        }
        let mut schema = TableSchema {
            name: name.clone(),
            columns: Vec::new(),
            root_page: 0,
            autoinc_counter: 1,
        };
        let mut btree = BTree::create(&mut self.pager).map_err(|e| e.to_string())?;
        schema.root_page = btree.root();
        for tc in table_constraints {
            match tc {
                TableConstraint::PrimaryKey(cols) => {
                    for cn in cols {
                        if let Some(i) = schema.col_index(&cn) {
                            schema.columns[i].primary_key = true;
                        }
                    }
                }
                TableConstraint::Unique(cols) => {
                    for cn in cols {
                        if let Some(i) = schema.col_index(&cn) {
                            schema.columns[i].unique = true;
                        }
                    }
                }
                _ => {}
            }
        }
        for spec in columns {
            let mut col = ColumnDef::new(&spec.name, &spec.type_name);
            for c in spec.constraints {
                match c {
                    ColumnConstraint::PrimaryKey { autoincrement } => {
                        col.primary_key = true;
                        col.autoincrement = autoincrement;
                    }
                    ColumnConstraint::NotNull => col.not_null = true,
                    ColumnConstraint::Unique => col.unique = true,
                    ColumnConstraint::Default(expr) => {
                        if let Some(v) = eval_const_expr(&expr) {
                            col.default = Some(v);
                        } else {
                            col.default_expr = Some(expr);
                        }
                    }
                    ColumnConstraint::Check(expr) => col.check_expr = Some(expr),
                    ColumnConstraint::References { .. } => {}
                }
            }
            schema.columns.push(col);
        }
        // INTEGER PRIMARY KEY is an alias for the rowid and auto-increments.
        let pk_cols: Vec<usize> = schema.columns.iter().enumerate()
            .filter(|(_, c)| c.primary_key).map(|(i, _)| i).collect();
        if pk_cols.len() == 1 {
            let i = pk_cols[0];
            if matches!(schema.columns[i].affinity, TypeAffinity::Integer) {
                schema.columns[i].autoincrement = true;
            }
        }
        self.tables.insert(
            name.clone(),
            Table { schema, btree, next_rowid: 1 },
        );
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("Table {} created", name)))
    }

    fn drop_table(&mut self, name: String, if_exists: bool) -> Result<ExecuteResult, String> {
        if !self.tables.contains_key(&name) {
            if if_exists {
                return Ok(ExecuteResult::Ok(format!("Table {} does not exist", name)));
            } else {
                return Err(format!("Table {} not found", name));
            }
        }
        let to_remove: Vec<String> = self.indexes
            .iter()
            .filter(|(_, idx)| idx.schema.table_name.eq_ignore_ascii_case(&name))
            .map(|(n, _)| n.clone())
            .collect();
        for n in to_remove {
            self.indexes.remove(&n);
        }
        self.tables.remove(&name);
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("Table {} dropped", name)))
    }

    fn create_index(&mut self, name: String, table: String, columns: Vec<String>, unique: bool, if_not_exists: bool) -> Result<ExecuteResult, String> {
        if self.indexes.contains_key(&name) {
            if if_not_exists {
                return Ok(ExecuteResult::Ok(format!("Index {} already exists", name)));
            } else {
                return Err(format!("Index {} already exists", name));
            }
        }
        let table_ref = self.tables.get(&table).ok_or_else(|| format!("Table {} not found", table))?;
        for c in &columns {
            if table_ref.schema.col_index(c).is_none() {
                return Err(format!("Column {} not found in {}", c, table));
            }
        }
        let mut btree = BTree::create(&mut self.pager).map_err(|e| e.to_string())?;
        let col_idx: Vec<usize> = columns.iter().map(|c| table_ref.schema.col_index(c).unwrap()).collect();
        for (key, payload) in table_ref.btree.scan() {
            let row = decode_row(payload).ok_or("Corrupt row")?;
            let rowid = decode_rowid_key(key);
            let mut index_key = Vec::new();
            for &i in &col_idx {
                let encoded = encode_value(&row[i]);
                index_key.extend_from_slice(&encoded);
            }
            index_key.extend_from_slice(&rowid.to_be_bytes());
            btree.insert_kv(index_key, Vec::new());
        }
        let root = btree.flush(&mut self.pager).map_err(|e| e.to_string())?;
        let schema = IndexSchema {
            name: name.clone(),
            table_name: table,
            columns,
            unique,
            root_page: root,
        };
        self.indexes.insert(name.clone(), Index { schema, btree });
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("Index {} created", name)))
    }

    fn drop_index(&mut self, name: String, if_exists: bool) -> Result<ExecuteResult, String> {
        if self.indexes.remove(&name).is_none() && !if_exists {
            return Err(format!("Index {} not found", name));
        }
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("Index {} dropped", name)))
    }

    fn insert(&mut self, table: String, columns: Option<Vec<String>>, values: Vec<Vec<Expr>>, or_replace: bool) -> Result<ExecuteResult, String> {
        let table_name = table;
        let mut inserted = 0;
        for row_values in values {
            let col_names = columns.clone().unwrap_or_else(|| {
                self.tables.get(&table_name).map(|t| t.schema.columns.iter().map(|c| c.name.clone()).collect()).unwrap_or_default()
            });
            if row_values.len() != col_names.len() {
                return Err("Column count mismatch".to_string());
            }
            let mut provided = HashMap::new();
            for (i, name) in col_names.iter().enumerate() {
                let v = eval_expr(self, &row_values[i], &EvalContext::new(&[], &HashMap::new()), None)?;
                provided.insert(name.clone(), v);
            }

            let (schema, next_rowid, autoinc_counter) = {
                let t = self.tables.get(&table_name).ok_or("Table not found")?;
                (t.schema.clone(), t.next_rowid, t.schema.autoinc_counter)
            };
            let mut row = Row::with_capacity(schema.columns.len());
            let mut pk_idx: Option<usize> = None;
            let mut pk_val: Option<Value> = None;
            for (i, col) in schema.columns.iter().enumerate() {
                let raw = if let Some(v) = provided.get(&col.name) {
                    v.clone()
                } else if let Some(expr) = &col.default_expr {
                    eval_expr(self, expr, &EvalContext::new(&[], &HashMap::new()), None)?
                } else if let Some(v) = &col.default {
                    v.clone()
                } else if col.primary_key && col.autoincrement {
                    Value::Integer(autoinc_counter)
                } else {
                    Value::Null
                };
                let v = col.affinity.apply(&raw);
                if col.primary_key {
                    pk_idx = Some(i);
                    pk_val = Some(v.clone());
                }
                row.push(v);
            }

            for (i, col) in schema.columns.iter().enumerate() {
                if col.not_null && row[i].is_null() {
                    return Err(format!("NOT NULL constraint failed: {}", col.name));
                }
            }

            let mut rowid = pk_val.as_ref().and_then(Value::as_i64).unwrap_or(next_rowid);
            if pk_idx.map_or(false, |i| schema.columns[i].autoincrement) && provided.get(&schema.columns[pk_idx.unwrap()].name).is_none() {
                rowid = rowid.max(autoinc_counter);
            }

            if let Some(i) = pk_idx {
                if schema.columns[i].primary_key && schema.columns[i].autoincrement && matches!(schema.columns[i].affinity, TypeAffinity::Integer) {
                    row[i] = Value::Integer(rowid);
                }
            }

            let existing: Vec<(i64, Row)> = {
                let t = self.tables.get(&table_name).ok_or("Table not found")?;
                t.btree.scan().map(|(k, p)| (decode_rowid_key(k), decode_row(p).unwrap())).collect()
            };

            for (i, col) in schema.columns.iter().enumerate() {
                if (col.primary_key || col.unique) && !row[i].is_null() {
                    for (eid, er) in &existing {
                        if row[i] == er[i] {
                            if col.primary_key {
                                if or_replace {
                                    let t = self.tables.get_mut(&table_name).unwrap();
                                    t.btree.delete(*eid);
                                    break;
                                } else {
                                    return Err(format!("PRIMARY KEY constraint failed: {}", col.name));
                                }
                            } else {
                                return Err(format!("UNIQUE constraint failed: {}", col.name));
                            }
                        }
                    }
                }
            }

            {
                let t = self.tables.get_mut(&table_name).unwrap();
                if pk_idx.map_or(false, |i| t.schema.columns[i].autoincrement) && provided.get(&t.schema.columns[pk_idx.unwrap()].name).is_none() {
                    t.schema.autoinc_counter = t.schema.autoinc_counter.max(rowid + 1);
                }
                t.next_rowid = t.next_rowid.max(rowid + 1);
                t.btree.insert(rowid, &encode_row(&row));
            }
            self.last_insert_rowid = rowid;
            inserted += 1;
        }
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("{} row(s) inserted", inserted)))
    }

    fn select(&mut self, select: SelectStmt) -> Result<ExecuteResult, String> {
        let mut rows = eval_from(self, &select.from, &select.joins)?;
        if let Some(w) = &select.where_clause {
            let empty_aliases = HashMap::new();
            rows.retain(|r| eval_expr(self, w, &EvalContext::new(r, &empty_aliases), None).unwrap_or(Value::Null).is_truthy());
        }
        let is_aggregate = !select.group_by.is_empty()
            || select.columns.iter().any(|c| has_aggregate(&c.expr))
            || select.having.as_ref().map_or(false, |h| has_aggregate(h));
        let mut result: Vec<(Vec<Value>, HashMap<String, Value>)> = Vec::new();
        let first_ctx = rows.first().cloned().unwrap_or_default();
        if is_aggregate {
            let groups = group_rows(self, rows, &select.group_by)?;
            for group in groups {
                let (values, aliases) = eval_select_columns(self, &select.columns, &group[0], Some(&group))?;
                if let Some(h) = &select.having {
                    if !eval_expr(self, h, &EvalContext::new(&group[0], &aliases), Some(&group))?.is_truthy() {
                        continue;
                    }
                }
                result.push((values, aliases));
            }
        } else {
            for r in rows {
                let (values, aliases) = eval_select_columns(self, &select.columns, &r, None)?;
                result.push((values, aliases));
            }
        }
        if select.distinct {
            let mut seen = BTreeSet::new();
            result.retain(|(v, _)| seen.insert(v.clone()));
        }
        if !select.order_by.is_empty() {
            result.sort_by(|(a, al_a), (b, al_b)| compare_result_rows(self, &select.order_by, a, al_a, b, al_b));
        }
        let mut values: Vec<Vec<Value>> = result.into_iter().map(|(v, _)| v).collect();
        if let Some(limit_expr) = &select.limit {
            let limit = eval_const_expr(limit_expr).and_then(|v| v.as_i64()).ok_or("LIMIT must be an integer")? as usize;
            let offset = if let Some(o) = &select.offset {
                eval_const_expr(o).and_then(|v| v.as_i64()).unwrap_or(0) as usize
            } else {
                0
            };
            values = values.into_iter().skip(offset).take(limit).collect();
        }
        let header = make_header(self, &select.columns, &first_ctx, &select.from);
        Ok(ExecuteResult::Rows { header, rows: values.into_iter().map(Row::from).collect() })
    }

    fn update(&mut self, table: String, assignments: Vec<Assignment>, where_clause: Option<Expr>) -> Result<ExecuteResult, String> {
        let table_name = table;
        let snapshot: Vec<(i64, Row)> = {
            let t = self.tables.get(&table_name).ok_or("Table not found")?;
            t.btree.scan().map(|(k, p)| (decode_rowid_key(k), decode_row(p).unwrap())).collect()
        };
        let schema = self.tables.get(&table_name).ok_or("Table not found")?.schema.clone();
        let mut updates: Vec<(i64, Row)> = Vec::new();
        for (rowid, row) in snapshot {
            let sr = SourceRow { table_name: table_name.clone(), alias: None, schema: Rc::new(schema.clone()), rowid, values: row.clone() };
            if let Some(w) = &where_clause {
                if !eval_expr(self, w, &EvalContext::new(&[sr.clone()], &HashMap::new()), None)?.is_truthy() {
                    continue;
                }
            }
            let mut new_row = row.clone();
            for a in &assignments {
                let idx = schema.col_index(&a.column).ok_or("Column not found")?;
                let v = eval_expr(self, &a.expr, &EvalContext::new(&[sr.clone()], &HashMap::new()), None)?;
                new_row[idx] = schema.columns[idx].affinity.apply(&v);
            }
            for (i, col) in schema.columns.iter().enumerate() {
                if col.not_null && new_row[i].is_null() {
                    return Err(format!("NOT NULL constraint failed: {}", col.name));
                }
            }
            updates.push((rowid, new_row));
        }
        let count = updates.len();
        {
            let t = self.tables.get_mut(&table_name).unwrap();
            for (rowid, row) in updates {
                t.btree.insert(rowid, &encode_row(&row));
            }
        }
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("{} row(s) updated", count)))
    }

    fn delete(&mut self, table: String, where_clause: Option<Expr>) -> Result<ExecuteResult, String> {
        let table_name = table;
        let snapshot: Vec<(i64, Row)> = {
            let t = self.tables.get(&table_name).ok_or("Table not found")?;
            t.btree.scan().map(|(k, p)| (decode_rowid_key(k), decode_row(p).unwrap())).collect()
        };
        let schema = self.tables.get(&table_name).ok_or("Table not found")?.schema.clone();
        let mut rowids: Vec<i64> = Vec::new();
        for (rowid, row) in snapshot {
            let sr = SourceRow { table_name: table_name.clone(), alias: None, schema: Rc::new(schema.clone()), rowid, values: row };
            if let Some(w) = &where_clause {
                if !eval_expr(self, w, &EvalContext::new(&[sr], &HashMap::new()), None)?.is_truthy() {
                    continue;
                }
            }
            rowids.push(rowid);
        }
        let count = rowids.len();
        {
            let t = self.tables.get_mut(&table_name).unwrap();
            for id in rowids {
                t.btree.delete(id);
            }
        }
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok(format!("{} row(s) deleted", count)))
    }

    fn alter_add_column(&mut self, table: String, column: ColSpec) -> Result<ExecuteResult, String> {
        let table_name = table;
        let mut col = ColumnDef::new(&column.name, &column.type_name);
        for c in column.constraints {
            match c {
                ColumnConstraint::PrimaryKey { autoincrement } => {
                    col.primary_key = true;
                    col.autoincrement = autoincrement;
                }
                ColumnConstraint::NotNull => col.not_null = true,
                ColumnConstraint::Unique => col.unique = true,
                ColumnConstraint::Default(expr) => {
                    if let Some(v) = eval_const_expr(&expr) {
                        col.default = Some(v);
                    } else {
                        col.default_expr = Some(expr);
                    }
                }
                ColumnConstraint::Check(expr) => col.check_expr = Some(expr),
                ColumnConstraint::References { .. } => {}
            }
        }
        let default_val = if let Some(v) = &col.default {
            v.clone()
        } else if let Some(expr) = &col.default_expr {
            eval_expr(self, expr, &EvalContext::new(&[], &HashMap::new()), None)?
        } else {
            Value::Null
        };

        let mut entries: Vec<(i64, Row)> = Vec::new();
        {
            let t = self.tables.get_mut(&table_name).ok_or("Table not found")?;
            t.schema.columns.push(col);
            let new_col = t.schema.columns.last().unwrap();
            for (key, payload) in t.btree.scan() {
                let mut row = decode_row(payload).ok_or("Corrupt row")?;
                row.push(new_col.affinity.apply(&default_val));
                entries.push((decode_rowid_key(key), row));
            }
        }
        {
            let t = self.tables.get_mut(&table_name).unwrap();
            for (rowid, row) in entries {
                t.btree.insert(rowid, &encode_row(&row));
            }
        }
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok("Column added".to_string()))
    }

    fn begin(&mut self) -> Result<ExecuteResult, String> {
        if self.in_transaction {
            return Ok(ExecuteResult::Ok("transaction already active".to_string()));
        }
        self.in_transaction = true;
        self.transaction_backup = Some(DatabaseState {
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
        });
        self.wal.begin().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok("BEGIN".to_string()))
    }

    fn commit(&mut self) -> Result<ExecuteResult, String> {
        if !self.in_transaction {
            return Ok(ExecuteResult::Ok("no active transaction".to_string()));
        }
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        self.wal.commit().map_err(|e| e.to_string())?;
        self.in_transaction = false;
        self.transaction_backup = None;
        Ok(ExecuteResult::Ok("COMMIT".to_string()))
    }

    fn rollback(&mut self) -> Result<ExecuteResult, String> {
        if !self.in_transaction {
            return Ok(ExecuteResult::Ok("no active transaction".to_string()));
        }
        if let Some(state) = self.transaction_backup.take() {
            self.tables = state.tables;
            self.indexes = state.indexes;
        }
        self.wal.rollback().map_err(|e| e.to_string())?;
        self.in_transaction = false;
        Ok(ExecuteResult::Ok("ROLLBACK".to_string()))
    }

    fn vacuum(&mut self) -> Result<ExecuteResult, String> {
        self.save_catalog().map_err(|e| e.to_string())?;
        self.pager.flush().map_err(|e| e.to_string())?;
        Ok(ExecuteResult::Ok("VACUUM complete".to_string()))
    }

    fn explain(&mut self, stmt: Statement) -> Result<ExecuteResult, String> {
        Ok(ExecuteResult::Ok(format!("{:#?}", stmt)))
    }

    fn pragma(&mut self, name: String, value: Option<Expr>) -> Result<ExecuteResult, String> {
        let name_upper = name.to_uppercase();
        match name_upper.as_str() {
            "TABLE_INFO" => {
                let table_name = match value {
                    Some(Expr::Text(s)) => s,
                    Some(Expr::Column { table: None, name }) => name,
                    _ => return Err("PRAGMA table_info(table)".to_string()),
                };
                let table = self.tables.get(&table_name).ok_or("Table not found")?;
                let header = vec![
                    "cid".to_string(),
                    "name".to_string(),
                    "type".to_string(),
                    "notnull".to_string(),
                    "dflt_value".to_string(),
                    "pk".to_string(),
                ];
                let mut rows = Vec::new();
                for (i, col) in table.schema.columns.iter().enumerate() {
                    let dflt = match &col.default {
                        Some(v) => format!("{}", v),
                        None if col.default_expr.is_some() => "(expr)".to_string(),
                        None => "NULL".to_string(),
                    };
                    rows.push(vec![
                        Value::Integer(i as i64),
                        Value::Text(col.name.clone()),
                        Value::Text(col.type_name.clone()),
                        Value::Integer(if col.not_null { 1 } else { 0 }),
                        Value::Text(dflt),
                        Value::Integer(if col.primary_key { 1 } else { 0 }),
                    ]);
                }
                Ok(ExecuteResult::Rows { header, rows })
            }
            "USER_VERSION" => {
                if let Some(v) = value {
                    let _ = eval_const_expr(&v).ok_or("Invalid PRAGMA value")?;
                }
                Ok(ExecuteResult::Rows {
                    header: vec!["user_version".to_string()],
                    rows: vec![vec![Value::Integer(0)]],
                })
            }
            _ => Ok(ExecuteResult::Ok(format!("PRAGMA {} = OK", name))),
        }
    }

    pub fn list_tables(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn show_schemas(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (_, table) in &self.tables {
            let cols: Vec<String> = table
                .schema
                .columns
                .iter()
                .map(|c| {
                    let mut s = format!("{} {}", c.name, c.type_name);
                    if c.primary_key {
                        s.push_str(" PRIMARY KEY");
                        if c.autoincrement {
                            s.push_str(" AUTOINCREMENT");
                        }
                    }
                    if c.not_null {
                        s.push_str(" NOT NULL");
                    }
                    if c.unique {
                        s.push_str(" UNIQUE");
                    }
                    s
                })
                .collect();
            out.push(format!("CREATE TABLE {} ({})", table.schema.name, cols.join(", ")));
        }
        out
    }

    pub fn list_indexes(&self) -> Vec<String> {
        let mut names: Vec<String> = self.indexes.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn dump_all(&self) -> Vec<(String, Vec<Row>)> {
        let mut out = Vec::new();
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        for name in names {
            if let Some(table) = self.tables.get(&name) {
                let mut rows = Vec::new();
                for (_, payload) in table.btree.scan() {
                    if let Some(row) = decode_row(payload) {
                        rows.push(row);
                    }
                }
                out.push((name, rows));
            }
        }
        out
    }

    pub fn get_stats(&self) -> Vec<(String, String)> {
        let mut stats = Vec::new();
        stats.push(("pagecount".to_string(), self.pager.page_count.to_string()));
        stats.push(("tables".to_string(), self.tables.len().to_string()));
        stats.push(("indexes".to_string(), self.indexes.len().to_string()));
        stats.push(("freelistpages".to_string(), self.pager.freelist.len().to_string()));
        stats
    }

    fn execute_select_rows(&mut self, select: &SelectStmt) -> Result<(Vec<String>, Vec<Row>), String> {
        match self.select(select.clone())? {
            ExecuteResult::Rows { header, rows } => Ok((header, rows)),
            ExecuteResult::Ok(msg) => Err(format!("Subquery returned non-row result: {}", msg)),
        }
    }
}

fn make_key(prefix: &[u8], name: &str) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(name.as_bytes());
    k
}

fn find_col(cols: &[ColumnDef], name: &str) -> Option<usize> {
    cols.iter().position(|c| c.name.eq_ignore_ascii_case(name))
}

fn decode_rowid_key(key: &[u8]) -> i64 {
    if key.len() < 8 {
        return 0;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&key[..8]);
    i64::from_be_bytes(b)
}

fn decode_row(bytes: &[u8]) -> Option<Row> {
    let mut off = 0;
    let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    off += 4;
    let mut row = Row::with_capacity(count);
    for _ in 0..count {
        row.push(decode_value(bytes, &mut off)?);
    }
    Some(row)
}

pub fn encode_row(row: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(row.len() as u32).to_be_bytes());
    for v in row {
        out.extend_from_slice(&encode_value(v));
    }
    out
}

fn eval_const_expr(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Null => Some(Value::Null),
        Expr::Boolean(b) => Some(Value::Integer(*b as i64)),
        Expr::Integer(i) => Some(Value::Integer(*i)),
        Expr::Real(f) => Some(Value::Real(*f)),
        Expr::Text(s) => Some(Value::Text(s.clone())),
        Expr::Blob(b) => Some(Value::Blob(b.clone())),
        Expr::Unary { op: UnaryOp::Neg, expr } => {
            eval_const_expr(expr).map(|v| match v {
                Value::Integer(i) => Value::Integer(-i),
                Value::Real(f) => Value::Real(-f),
                _ => v,
            })
        }
        Expr::Binary { left, op: BinOp::Add, right } => {
            let a = eval_const_expr(left)?;
            let b = eval_const_expr(right)?;
            Some(add_values(&a, &b))
        }
        Expr::Binary { left, op: BinOp::Sub, right } => {
            let a = eval_const_expr(left)?;
            let b = eval_const_expr(right)?;
            Some(sub_values(&a, &b))
        }
        _ => None,
    }
}

fn add_values(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Value::Integer(x + y),
        (Value::Real(x), Value::Real(y)) => Value::Real(x + y),
        (Value::Integer(x), Value::Real(y)) => Value::Real(*x as f64 + y),
        (Value::Real(x), Value::Integer(y)) => Value::Real(x + *y as f64),
        (Value::Text(x), Value::Text(y)) => Value::Text(format!("{}{}", x, y)),
        _ => Value::Null,
    }
}

fn sub_values(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Value::Integer(x - y),
        (Value::Real(x), Value::Real(y)) => Value::Real(x - y),
        (Value::Integer(x), Value::Real(y)) => Value::Real(*x as f64 - y),
        (Value::Real(x), Value::Integer(y)) => Value::Real(x - *y as f64),
        _ => Value::Null,
    }
}

fn mul_values(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Value::Integer(x * y),
        (Value::Real(x), Value::Real(y)) => Value::Real(x * y),
        (Value::Integer(x), Value::Real(y)) => Value::Real(*x as f64 * y),
        (Value::Real(x), Value::Integer(y)) => Value::Real(x * *y as f64),
        _ => Value::Null,
    }
}

fn div_values(a: &Value, b: &Value) -> Value {
    let a = a.as_f64().unwrap_or(0.0);
    let b = b.as_f64().unwrap_or(0.0);
    if b == 0.0 {
        Value::Null
    } else {
        Value::Real(a / b)
    }
}

fn mod_values(a: &Value, b: &Value) -> Value {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        if y == 0 {
            Value::Null
        } else {
            Value::Integer(x % y)
        }
    } else {
        Value::Null
    }
}

fn eval_from(db: &mut Database, from: &Option<TableRef>, joins: &[JoinClause]) -> Result<Vec<Vec<SourceRow>>, String> {
    let mut rows: Vec<Vec<SourceRow>> = Vec::new();
    if let Some(f) = from {
        if f.subquery.is_some() {
            return Err("Subqueries in FROM are not supported".to_string());
        }
        let table = db.tables.get(&f.name).ok_or_else(|| format!("Table {} not found", f.name))?;
        for (key, payload) in table.btree.scan() {
            let row = decode_row(payload).ok_or("Corrupt row")?;
            let rowid = decode_rowid_key(key);
            rows.push(vec![SourceRow {
                table_name: f.name.clone(),
                alias: f.alias.clone(),
                schema: Rc::new(table.schema.clone()),
                rowid,
                values: row,
            }]);
        }
    } else {
        rows.push(vec![]);
    }

    for join in joins {
        let jt = &join.table;
        if jt.subquery.is_some() {
            return Err("Subqueries in JOIN are not supported".to_string());
        }
        let jrows: Vec<(i64, Row, Rc<TableSchema>)> = {
            let jtable = db.tables.get(&jt.name).ok_or_else(|| format!("Table {} not found", jt.name))?;
            jtable.btree.scan().map(|(key, payload)| {
                (decode_rowid_key(key), decode_row(payload).unwrap(), Rc::new(jtable.schema.clone()))
            }).collect()
        };
        let jschema_len = jrows.first().map(|(_, _, s)| s.columns.len()).unwrap_or(0);
        let mut new_rows = Vec::new();
        for base in &rows {
            let mut matched = false;
            for (rowid, row, schema) in &jrows {
                let joined = SourceRow {
                    table_name: jt.name.clone(),
                    alias: jt.alias.clone(),
                    schema: schema.clone(),
                    rowid: *rowid,
                    values: row.clone(),
                };
                let mut combined = base.clone();
                combined.push(joined);
                if let Some(on) = &join.on {
                    let empty = HashMap::new();
                    if eval_expr(db, on, &EvalContext::new(&combined, &empty), None)?.is_truthy() {
                        new_rows.push(combined);
                        matched = true;
                    }
                } else {
                    new_rows.push(combined);
                    matched = true;
                }
            }
            if join.join_type == JoinType::Left && !matched {
                let mut combined = base.clone();
                combined.push(SourceRow {
                    table_name: jt.name.clone(),
                    alias: jt.alias.clone(),
                    schema: jrows.first().map(|(_, _, s)| s.clone()).unwrap_or(Rc::new(TableSchema { name: jt.name.clone(), columns: Vec::new(), root_page: 0, autoinc_counter: 1 })),
                    rowid: 0,
                    values: vec![Value::Null; jschema_len],
                });
                new_rows.push(combined);
            }
        }
        rows = new_rows;
    }
    Ok(rows)
}

fn eval_expr(db: &mut Database, expr: &Expr, ctx: &EvalContext, agg_rows: Option<&[Vec<SourceRow>]>) -> Result<Value, String> {
    match expr {
        Expr::Null => Ok(Value::Null),
        Expr::Boolean(b) => Ok(Value::Integer(*b as i64)),
        Expr::Integer(i) => Ok(Value::Integer(*i)),
        Expr::Real(f) => Ok(Value::Real(*f)),
        Expr::Text(s) => Ok(Value::Text(s.clone())),
        Expr::Blob(b) => Ok(Value::Blob(b.clone())),
        Expr::Column { table, name } => resolve_column(ctx, table, name),
        Expr::Alias(inner, _) => eval_expr(db, inner, ctx, agg_rows),
        Expr::Unary { op, expr } => {
            let v = eval_expr(db, expr, ctx, agg_rows)?;
            Ok(apply_unary(op, &v))
        }
        Expr::Binary { left, op, right } => {
            let l = eval_expr(db, left, ctx, agg_rows)?;
            let r = eval_expr(db, right, ctx, agg_rows)?;
            apply_binary(op, &l, &r)
        }
        Expr::Function { name, args, distinct } => {
            if is_aggregate_fn(name) {
                let group = agg_rows.ok_or("Aggregate function not in aggregate context")?;
                compute_aggregate(db, name, args, *distinct, group)
            } else {
                let vals: Result<Vec<Value>, String> = args.iter().map(|a| eval_expr(db, a, ctx, agg_rows)).collect();
                Ok(call_function(name, &vals?))
            }
        }
        Expr::Case { expr, when, else_ } => {
            for (cond, then) in when {
                if expr.is_some() {
                    let base = eval_expr(db, expr.as_ref().unwrap(), ctx, agg_rows)?;
                    let check = eval_expr(db, cond, ctx, agg_rows)?;
                    if base == check {
                        return eval_expr(db, then, ctx, agg_rows);
                    }
                } else if eval_expr(db, cond, ctx, agg_rows)?.is_truthy() {
                    return eval_expr(db, then, ctx, agg_rows);
                }
            }
            if let Some(e) = else_ {
                eval_expr(db, e, ctx, agg_rows)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Cast { expr, type_name } => {
            let v = eval_expr(db, expr, ctx, agg_rows)?;
            Ok(apply_cast(&v, type_name))
        }
        Expr::Between { expr, negated, low, high } => {
            let v = eval_expr(db, expr, ctx, agg_rows)?;
            let l = eval_expr(db, low, ctx, agg_rows)?;
            let h = eval_expr(db, high, ctx, agg_rows)?;
            let ord = compare_values(&v, &l) != Ordering::Less && compare_values(&v, &h) != Ordering::Greater;
            Ok(Value::Integer(if *negated { !ord } else { ord } as i64))
        }
        Expr::InList { expr, negated, list } => {
            let v = eval_expr(db, expr, ctx, agg_rows)?;
            let mut found = false;
            for e in list {
                let lv = eval_expr(db, e, ctx, agg_rows)?;
                if v == lv {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer(if *negated { !found } else { found } as i64))
        }
        Expr::InSubquery { expr, negated, query } => {
            let v = eval_expr(db, expr, ctx, agg_rows)?;
            let (_, rows) = db.execute_select_rows(query)?;
            let mut found = false;
            for r in rows {
                if !r.is_empty() && r[0] == v {
                    found = true;
                    break;
                }
            }
            Ok(Value::Integer(if *negated { !found } else { found } as i64))
        }
        Expr::Like { expr, negated, pattern, escape } => {
            let v = eval_expr(db, expr, ctx, agg_rows)?;
            let p = eval_expr(db, pattern, ctx, agg_rows)?;
            let esc = if let Some(e) = escape {
                Some(eval_expr(db, e, ctx, agg_rows)?)
            } else {
                None
            };
            let mut matched = like_match(&v, &p, esc.as_ref())?;
            if *negated {
                matched = !matched;
            }
            Ok(Value::Integer(matched as i64))
        }
        Expr::Exists(query) => {
            let (_, rows) = db.execute_select_rows(query)?;
            Ok(Value::Integer(if rows.is_empty() { 0 } else { 1 }))
        }
        Expr::IsNull(e, negated) => {
            let v = eval_expr(db, e, ctx, agg_rows)?;
            let is_null = v.is_null();
            Ok(Value::Integer(if *negated { !is_null } else { is_null } as i64))
        }
        Expr::Subquery(query) => {
            let (_, rows) = db.execute_select_rows(query)?;
            if rows.is_empty() || rows[0].is_empty() {
                Ok(Value::Null)
            } else {
                Ok(rows[0][0].clone())
            }
        }
        Expr::Nested(e) => eval_expr(db, e, ctx, agg_rows),
        Expr::Star => Err("* not allowed in expression".to_string()),
    }
}

fn resolve_column(ctx: &EvalContext, table: &Option<String>, name: &str) -> Result<Value, String> {
    if let Some(t) = table {
        for sr in ctx.rows {
            let matches = sr.alias.as_ref() == Some(t) || sr.table_name.eq_ignore_ascii_case(t);
            if matches {
                if let Some(idx) = sr.schema.col_index(name) {
                    return Ok(rowid_value(sr, idx));
                }
            }
        }
        return Err(format!("Column {}.{} not found", t, name));
    }
    for sr in ctx.rows {
        if let Some(idx) = sr.schema.col_index(name) {
            return Ok(rowid_value(sr, idx));
        }
    }
    if let Some(v) = ctx.aliases.get(name) {
        return Ok(v.clone());
    }
    Err(format!("Column {} not found", name))
}

fn rowid_value(sr: &SourceRow, idx: usize) -> Value {
    let col = &sr.schema.columns[idx];
    if col.primary_key && col.autoincrement && matches!(col.affinity, TypeAffinity::Integer) {
        Value::Integer(sr.rowid)
    } else {
        sr.values[idx].clone()
    }
}

fn apply_unary(op: &UnaryOp, v: &Value) -> Value {
    match op {
        UnaryOp::Neg => match v {
            Value::Integer(i) => Value::Integer(-i),
            Value::Real(f) => Value::Real(-f),
            _ => Value::Null,
        },
        UnaryOp::Pos => v.clone(),
        UnaryOp::Not => Value::Integer((!v.is_truthy()) as i64),
        UnaryOp::BitNot => v.as_i64().map(|i| Value::Integer(!i)).unwrap_or(Value::Null),
    }
}

fn apply_binary(op: &BinOp, l: &Value, r: &Value) -> Result<Value, String> {
    use std::cmp::Ordering;
    let ord = compare_values(l, r);
    Ok(match op {
        BinOp::Eq => Value::Integer((ord == Ordering::Equal) as i64),
        BinOp::Neq => Value::Integer((ord != Ordering::Equal) as i64),
        BinOp::Lt => Value::Integer((ord == Ordering::Less) as i64),
        BinOp::Gt => Value::Integer((ord == Ordering::Greater) as i64),
        BinOp::Lte => Value::Integer((ord != Ordering::Greater) as i64),
        BinOp::Gte => Value::Integer((ord != Ordering::Less) as i64),
        BinOp::Add => add_values(l, r),
        BinOp::Sub => sub_values(l, r),
        BinOp::Mul => mul_values(l, r),
        BinOp::Div => div_values(l, r),
        BinOp::Mod => mod_values(l, r),
        BinOp::Concat => Value::Text(format!("{}{}", value_to_string(l), value_to_string(r))),
        BinOp::And => Value::Integer((l.is_truthy() && r.is_truthy()) as i64),
        BinOp::Or => Value::Integer((l.is_truthy() || r.is_truthy()) as i64),
    })
}

fn value_to_string(v: &Value) -> String {
    format!("{}", v)
}

fn apply_cast(v: &Value, type_name: &str) -> Value {
    let upper = type_name.to_uppercase();
    if upper.contains("INT") {
        v.as_i64().map(Value::Integer).unwrap_or(Value::Null)
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        v.as_f64().map(Value::Real).unwrap_or(Value::Null)
    } else if upper.contains("TEXT") || upper.contains("CHAR") || upper.contains("CLOB") {
        Value::Text(value_to_string(v))
    } else if upper.contains("BLOB") {
        match v {
            Value::Text(s) => Value::Blob(s.as_bytes().to_vec()),
            Value::Blob(b) => Value::Blob(b.clone()),
            _ => Value::Blob(value_to_string(v).as_bytes().to_vec()),
        }
    } else {
        v.clone()
    }
}

fn like_match(value: &Value, pattern: &Value, escape: Option<&Value>) -> Result<bool, String> {
    let text = value.as_str().unwrap_or("");
    let pat = pattern.as_str().ok_or("LIKE pattern must be text")?;
    let esc = escape.map(|v| v.as_str().unwrap_or("") .chars().next());
    Ok(sql_like(text, pat, esc.flatten()))
}

fn sql_like(text: &str, pattern: &str, escape: Option<char>) -> bool {
    let t: Vec<char> = text.to_uppercase().chars().collect();
    let p: Vec<char> = pattern.to_uppercase().chars().collect();
    sql_like_recur(&t, 0, &p, 0, escape)
}

fn sql_like_recur(t: &Vec<char>, ti: usize, p: &Vec<char>, pi: usize, escape: Option<char>) -> bool {
    if pi == p.len() {
        return ti == t.len();
    }
    let pc = p[pi];
    if Some(pc) == escape {
        if pi + 1 < p.len() {
            if ti < t.len() && t[ti] == p[pi + 1] {
                return sql_like_recur(t, ti + 1, p, pi + 2, escape);
            }
        }
        return false;
    }
    if pc == '%' {
        if sql_like_recur(t, ti, p, pi + 1, escape) {
            return true;
        }
        if ti < t.len() && sql_like_recur(t, ti + 1, p, pi, escape) {
            return true;
        }
        return false;
    }
    if pc == '_' {
        return if ti < t.len() { sql_like_recur(t, ti + 1, p, pi + 1, escape) } else { false };
    }
    if ti < t.len() && t[ti] == pc {
        sql_like_recur(t, ti + 1, p, pi + 1, escape)
    } else {
        false
    }
}

fn has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, .. } if is_aggregate_fn(name) => true,
        Expr::Binary { left, right, .. } => has_aggregate(left) || has_aggregate(right),
        Expr::Unary { expr, .. } => has_aggregate(expr),
        Expr::Alias(inner, _) => has_aggregate(inner),
        Expr::Case { expr, when, else_ } => {
            expr.as_ref().map_or(false, |e| has_aggregate(e))
                || when.iter().any(|(a, b)| has_aggregate(a) || has_aggregate(b))
                || else_.as_ref().map_or(false, |e| has_aggregate(e))
        }
        Expr::Cast { expr, .. } => has_aggregate(expr),
        Expr::Between { expr, low, high, .. } => has_aggregate(expr) || has_aggregate(low) || has_aggregate(high),
        Expr::InList { expr, list, .. } => has_aggregate(expr) || list.iter().any(|e| has_aggregate(e)),
        Expr::InSubquery { expr, .. } => has_aggregate(expr),
        Expr::Like { expr, pattern, escape, .. } => {
            has_aggregate(expr) || has_aggregate(pattern) || escape.as_ref().map_or(false, |e| has_aggregate(e))
        }
        Expr::IsNull(e, _) => has_aggregate(e),
        Expr::Exists(_) | Expr::Subquery(_) => false,
        Expr::Nested(e) => has_aggregate(e),
        _ => false,
    }
}

fn is_aggregate_fn(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" | "GROUP_CONCAT"
    )
}

fn group_rows(db: &mut Database, rows: Vec<Vec<SourceRow>>, group_by: &[Expr]) -> Result<Vec<Vec<Vec<SourceRow>>>, String> {
    if group_by.is_empty() {
        return Ok(vec![rows]);
    }
    let mut map: BTreeMap<Vec<Value>, Vec<Vec<SourceRow>>> = BTreeMap::new();
    let empty = HashMap::new();
    for r in rows {
        let mut key = Vec::new();
        for e in group_by {
            key.push(eval_expr(db, e, &EvalContext::new(&r, &empty), None)?);
        }
        map.entry(key).or_default().push(r);
    }
    Ok(map.into_values().collect())
}

fn compute_aggregate(db: &mut Database, name: &str, args: &[Expr], distinct: bool, group: &[Vec<SourceRow>]) -> Result<Value, String> {
    let upper = name.to_uppercase();
    let empty = HashMap::new();
    match upper.as_str() {
        "COUNT" => {
            if args.is_empty() || (args.len() == 1 && matches!(args[0], Expr::Star)) {
                Ok(Value::Integer(group.len() as i64))
            } else {
                let mut vals = Vec::new();
                for r in group {
                    let v = eval_expr(db, &args[0], &EvalContext::new(r, &empty), None)?;
                    if !v.is_null() {
                        vals.push(v);
                    }
                }
                if distinct {
                    vals.sort_by(compare_values);
                    vals.dedup_by(|a, b| compare_values(a, b) == Ordering::Equal);
                }
                Ok(Value::Integer(vals.len() as i64))
            }
        }
        "SUM" | "AVG" | "TOTAL" => {
            let mut total = 0.0;
            let mut count = 0;
            let mut all_int = true;
            for r in group {
                let v = eval_expr(db, &args[0], &EvalContext::new(r, &empty), None)?;
                if let Some(n) = v.as_f64() {
                    total += n;
                    count += 1;
                    if !matches!(v, Value::Integer(_)) {
                        all_int = false;
                    }
                }
            }
            if count == 0 {
                return if upper == "TOTAL" { Ok(Value::Real(0.0)) } else { Ok(Value::Null) };
            }
            if upper == "AVG" {
                Ok(Value::Real(total / count as f64))
            } else if upper == "TOTAL" {
                Ok(Value::Real(total))
            } else {
                if all_int && total.fract() == 0.0 {
                    Ok(Value::Integer(total as i64))
                } else {
                    Ok(Value::Real(total))
                }
            }
        }
        "MIN" | "MAX" => {
            let mut vals = Vec::new();
            for r in group {
                let v = eval_expr(db, &args[0], &EvalContext::new(r, &empty), None)?;
                if !v.is_null() {
                    vals.push(v);
                }
            }
            if vals.is_empty() {
                return Ok(Value::Null);
            }
            let ord = if upper == "MIN" { Ordering::Less } else { Ordering::Greater };
            let mut best = vals[0].clone();
            for v in vals.iter().skip(1) {
                if compare_values(v, &best) == ord {
                    best = v.clone();
                }
            }
            Ok(best)
        }
        "GROUP_CONCAT" => {
            let sep = if args.len() >= 2 {
                eval_expr(db, &args[1], &EvalContext::new(&group[0], &empty), None)?
                    .as_str().unwrap_or(",").to_string()
            } else {
                ",".to_string()
            };
            let mut vals = Vec::new();
            for r in group {
                let v = eval_expr(db, &args[0], &EvalContext::new(r, &empty), None)?;
                if !v.is_null() {
                    vals.push(value_to_string(&v));
                }
            }
            if distinct {
                vals.sort();
                vals.dedup();
            }
            Ok(Value::Text(vals.join(&sep)))
        }
        _ => Err(format!("Unknown aggregate {}", name)),
    }
}

fn eval_select_columns(db: &mut Database, columns: &[SelectCol], ctx: &[SourceRow], agg_rows: Option<&[Vec<SourceRow>]>) -> Result<(Vec<Value>, HashMap<String, Value>), String> {
    let mut values = Vec::new();
    let mut aliases = HashMap::new();
    for col in columns {
        let mut vals = eval_select_column(db, col, ctx, agg_rows)?;
        if let Some(alias) = &col.alias {
            if vals.len() == 1 {
                aliases.insert(alias.clone(), vals[0].clone());
                values.push(vals.remove(0));
                continue;
            }
        }
        values.append(&mut vals);
    }
    Ok((values, aliases))
}

fn eval_select_column(db: &mut Database, col: &SelectCol, ctx: &[SourceRow], agg_rows: Option<&[Vec<SourceRow>]>) -> Result<Vec<Value>, String> {
    match &col.expr {
        Expr::Star => {
            let mut out = Vec::new();
            for sr in ctx {
                for i in 0..sr.values.len() {
                    out.push(rowid_value(sr, i));
                }
            }
            Ok(out)
        }
        Expr::Column { table, name } if name == "*" => {
            for sr in ctx {
                let matches = if let Some(t) = table {
                    sr.alias.as_ref() == Some(t) || sr.table_name.eq_ignore_ascii_case(t)
                } else {
                    true
                };
                if matches {
                    return Ok((0..sr.values.len()).map(|i| rowid_value(sr, i)).collect());
                }
            }
            Err(format!("Table {} not found", table.as_ref().unwrap_or(&"*".to_string())))
        }
        _ => {
            let empty = HashMap::new();
            Ok(vec![eval_expr(db, &col.expr, &EvalContext::new(ctx, &empty), agg_rows)?])
        }
    }
}

fn make_header(db: &Database, columns: &[SelectCol], ctx: &[SourceRow], from: &Option<TableRef>) -> Vec<String> {
    let mut out = Vec::new();
    for col in columns {
        if let Some(alias) = &col.alias {
            out.push(alias.clone());
            continue;
        }
        match &col.expr {
            Expr::Star => {
                if ctx.is_empty() {
                    if let Some(f) = from {
                        if let Some(t) = db.tables.get(&f.name) {
                            for c in &t.schema.columns {
                                out.push(c.name.clone());
                            }
                        }
                    }
                } else {
                    for sr in ctx {
                        for c in &sr.schema.columns {
                            out.push(c.name.clone());
                        }
                    }
                }
            }
            Expr::Column { table, name } if name == "*" => {
                let target = table.clone();
                if ctx.is_empty() {
                    if let Some(f) = from {
                        if let Some(t) = db.tables.get(&f.name) {
                            for c in &t.schema.columns {
                                out.push(c.name.clone());
                            }
                        }
                    }
                } else {
                    for sr in ctx {
                        let matches = if let Some(ref t) = target {
                            sr.alias.as_ref() == Some(t) || sr.table_name.eq_ignore_ascii_case(t)
                        } else {
                            true
                        };
                        if matches {
                            for c in &sr.schema.columns {
                                out.push(c.name.clone());
                            }
                        }
                    }
                }
            }
            _ => out.push(expr_to_name(&col.expr)),
        }
    }
    out
}

fn expr_to_name(expr: &Expr) -> String {
    match expr {
        Expr::Null => "NULL".to_string(),
        Expr::Boolean(b) => b.to_string(),
        Expr::Integer(i) => i.to_string(),
        Expr::Real(f) => f.to_string(),
        Expr::Text(s) => s.clone(),
        Expr::Blob(b) => format!("x'{}'", b.iter().map(|x| format!("{:02x}", x)).collect::<String>()),
        Expr::Column { table: None, name } => name.clone(),
        Expr::Column { table: Some(t), name } => format!("{}.{}", t, name),
        Expr::Function { name, args, distinct } => {
            let mut s = name.clone();
            s.push('(');
            if *distinct { s.push_str("DISTINCT "); }
            s.push_str(&args.iter().map(expr_to_name).collect::<Vec<_>>().join(", "));
            s.push(')');
            s
        }
        Expr::Unary { op, expr } => format!("{}{}", unary_op_str(op), expr_to_name(expr)),
        Expr::Binary { left, op, right } => format!("{} {} {}", expr_to_name(left), bin_op_str(op), expr_to_name(right)),
        Expr::Case { .. } => "CASE".to_string(),
        Expr::Cast { .. } => "CAST".to_string(),
        Expr::Between { .. } => "BETWEEN".to_string(),
        Expr::InList { .. } => "IN".to_string(),
        Expr::InSubquery { .. } => "IN".to_string(),
        Expr::Like { .. } => "LIKE".to_string(),
        Expr::Exists(_) => "EXISTS".to_string(),
        Expr::IsNull(_, _) => "ISNULL".to_string(),
        Expr::Subquery(_) => "(SELECT)".to_string(),
        Expr::Nested(e) => expr_to_name(e),
        Expr::Star => "*".to_string(),
        Expr::Alias(inner, alias) => format!("{} AS {}", expr_to_name(inner), alias),
    }
}

fn unary_op_str(op: &UnaryOp) -> &'static str {
    match op {
        UnaryOp::Neg => "-",
        UnaryOp::Pos => "+",
        UnaryOp::Not => "NOT ",
        UnaryOp::BitNot => "~",
    }
}

fn bin_op_str(op: &BinOp) -> &'static str {
    match op {
        BinOp::Eq => "=",
        BinOp::Neq => "!=",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Lte => "<=",
        BinOp::Gte => ">=",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Concat => "||",
        BinOp::And => "AND",
        BinOp::Or => "OR",
    }
}

fn compare_result_rows(db: &mut Database, order_by: &[(Expr, Order)], a: &[Value], al_a: &HashMap<String, Value>, b: &[Value], al_b: &HashMap<String, Value>) -> Ordering {
    for (expr, order) in order_by {
        let ctx_a = EvalContext::new(&[], al_a);
        let va = eval_expr(db, expr, &ctx_a, None).unwrap_or(Value::Null);
        let ctx_b = EvalContext::new(&[], al_b);
        let vb = eval_expr(db, expr, &ctx_b, None).unwrap_or(Value::Null);
        let mut ord = compare_values(&va, &vb);
        if *order == Order::Desc {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}
