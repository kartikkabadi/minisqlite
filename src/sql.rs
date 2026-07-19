use std::collections::HashSet;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Column {
        table: Option<String>,
        name: String,
    },
    Alias(Box<Expr>, String),
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Case {
        expr: Option<Box<Expr>>,
        when: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        type_name: String,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    InList {
        expr: Box<Expr>,
        negated: bool,
        list: Vec<Expr>,
    },
    InSubquery {
        expr: Box<Expr>,
        negated: bool,
        query: Box<SelectStmt>,
    },
    Like {
        expr: Box<Expr>,
        negated: bool,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
    },
    Exists(Box<SelectStmt>),
    IsNull(Box<Expr>, bool),
    Subquery(Box<SelectStmt>),
    Nested(Box<Expr>),
    Star,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Pos,
    Not,
    BitNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectCol {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    pub subquery: Option<Box<SelectStmt>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct: bool,
    pub columns: Vec<SelectCol>,
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<(Expr, Order)>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColSpec {
    pub name: String,
    pub type_name: String,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey { autoincrement: bool },
    NotNull,
    Unique,
    Default(Expr),
    Check(Expr),
    References { table: String, column: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        table: String,
        ref_columns: Vec<String>,
    },
    Check(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColSpec>,
        if_not_exists: bool,
        constraints: Vec<TableConstraint>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
        or_replace: bool,
    },
    Select(SelectStmt),
    Update {
        table: String,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    AlterAddColumn {
        table: String,
        column: ColSpec,
    },
    Begin,
    Commit,
    Rollback,
    Vacuum,
    Explain(Box<Statement>),
    Pragma {
        name: String,
        value: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Eof,
    Keyword(String),
    Ident(String),
    String(String),
    Number(String),
    Punct(char),
}

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(input: &str) -> Self {
        let tokens = tokenize(input);
        Parser { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<Statement, String> {
        let stmt = self.parse_statement()?;
        self.expect(Token::Eof)?;
        Ok(stmt)
    }

    // ---------------- tokenizer helpers ----------------
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned()?;
        self.pos += 1;
        Some(t)
    }

    fn at(&self, token: &Token) -> bool {
        self.peek() == Some(token)
    }

    fn expect(&mut self, token: Token) -> Result<(), String> {
        if self.next() == Some(token.clone()) {
            Ok(())
        } else {
            Err(format!("Expected {:?}", token))
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), String> {
        if let Some(Token::Keyword(ref s)) = self.peek() {
            if s.eq_ignore_ascii_case(kw) {
                self.pos += 1;
                return Ok(());
            }
        }
        Err(format!("Expected keyword {}", kw))
    }

    fn match_keyword(&mut self, kw: &str) -> bool {
        if let Some(Token::Keyword(ref s)) = self.peek() {
            if s.eq_ignore_ascii_case(kw) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Token::Ident(s)) | Some(Token::Keyword(s)) => Ok(s),
            _ => Err("Expected identifier".to_string()),
        }
    }

    // ---------------- statements ----------------
    fn parse_statement(&mut self) -> Result<Statement, String> {
        match self.peek() {
            Some(Token::Keyword(kw)) => {
                let upper = kw.to_uppercase();
                match upper.as_str() {
                    "SELECT" => Ok(Statement::Select(self.parse_select()?)),
                    "CREATE" => self.parse_create(),
                    "DROP" => self.parse_drop(),
                    "INSERT" => self.parse_insert(),
                    "UPDATE" => self.parse_update(),
                    "DELETE" => self.parse_delete(),
                    "BEGIN" | "START" => {
                        self.pos += 1;
                        self.skip_optional_transaction();
                        Ok(Statement::Begin)
                    }
                    "COMMIT" => {
                        self.pos += 1;
                        self.skip_optional_transaction();
                        Ok(Statement::Commit)
                    }
                    "END" => {
                        self.pos += 1;
                        self.skip_optional_transaction();
                        Ok(Statement::Commit)
                    }
                    "ROLLBACK" => {
                        self.pos += 1;
                        self.skip_optional_transaction();
                        Ok(Statement::Rollback)
                    }
                    "VACUUM" => {
                        self.pos += 1;
                        Ok(Statement::Vacuum)
                    }
                    "ALTER" => self.parse_alter(),
                    "EXPLAIN" => {
                        self.pos += 1;
                        let query_plan = self.match_keyword("QUERY");
                        if query_plan {
                            self.expect_keyword("PLAN")?;
                        }
                        let stmt = Box::new(self.parse_statement()?);
                        Ok(Statement::Explain(stmt))
                    }
                    "PRAGMA" => self.parse_pragma(),
                    _ => Err(format!("Unexpected keyword {}", kw)),
                }
            }
            Some(Token::Punct(';')) => {
                self.pos += 1;
                self.parse_statement()
            }
            _ => Err("Expected statement".to_string()),
        }
    }

    fn skip_optional_transaction(&mut self) {
        self.match_keyword("TRANSACTION");
        self.match_keyword("WORK");
    }

    fn parse_create(&mut self) -> Result<Statement, String> {
        self.expect_keyword("CREATE")?;
        let unique = self.match_keyword("UNIQUE");
        if self.match_keyword("TABLE") {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect(Token::Punct('('))?;
            let mut columns = Vec::new();
            let mut constraints = Vec::new();
            loop {
                if self.at(&Token::Punct(')')) {
                    break;
                }
                if let Some(Token::Keyword(kw)) = self.peek() {
                    let u = kw.to_uppercase();
                    if u == "PRIMARY" || u == "UNIQUE" || u == "FOREIGN" || u == "CHECK" || u == "CONSTRAINT" {
                        constraints.push(self.parse_table_constraint()?);
                    } else {
                        columns.push(self.parse_column_def()?);
                    }
                } else {
                    columns.push(self.parse_column_def()?);
                }
                if !self.eat_punct(',') {
                    break;
                }
            }
            self.expect(Token::Punct(')'))?;
            Ok(Statement::CreateTable {
                name,
                columns,
                if_not_exists,
                constraints,
            })
        } else if self.match_keyword("INDEX") {
            let if_not_exists = self.parse_if_not_exists()?;
            let name = self.expect_ident()?;
            self.expect_keyword("ON")?;
            let table = self.expect_ident()?;
            self.expect(Token::Punct('('))?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.expect_ident()?);
                if !self.eat_punct(',') {
                    break;
                }
            }
            self.expect(Token::Punct(')'))?;
            Ok(Statement::CreateIndex {
                name,
                table,
                columns,
                unique,
                if_not_exists,
            })
        } else if unique {
            Err("Expected TABLE or INDEX after UNIQUE".to_string())
        } else {
            Err("Expected TABLE or INDEX after CREATE".to_string())
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, String> {
        if self.match_keyword("IF") {
            self.expect_keyword("NOT")?;
            self.expect_keyword("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, String> {
        self.expect_keyword("DROP")?;
        if self.match_keyword("TABLE") {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropTable { name, if_exists })
        } else if self.match_keyword("INDEX") {
            let if_exists = self.parse_if_exists()?;
            let name = self.expect_ident()?;
            Ok(Statement::DropIndex { name, if_exists })
        } else {
            Err("Expected TABLE or INDEX after DROP".to_string())
        }
    }

    fn parse_if_exists(&mut self) -> Result<bool, String> {
        if self.match_keyword("IF") {
            self.expect_keyword("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_alter(&mut self) -> Result<Statement, String> {
        self.expect_keyword("ALTER")?;
        self.expect_keyword("TABLE")?;
        let table = self.expect_ident()?;
        if self.match_keyword("ADD") {
            self.match_keyword("COLUMN");
            let column = self.parse_column_def()?;
            Ok(Statement::AlterAddColumn { table, column })
        } else {
            Err("Expected ADD COLUMN".to_string())
        }
    }

    fn parse_pragma(&mut self) -> Result<Statement, String> {
        self.expect_keyword("PRAGMA")?;
        let name = self.expect_ident()?;
        let mut value = None;
        if self.eat_punct('(') {
            value = Some(self.parse_expr()?);
            self.expect(Token::Punct(')'))?;
        } else if self.eat_punct('=') {
            value = Some(self.parse_expr()?);
        }
        Ok(Statement::Pragma { name, value })
    }

    fn parse_column_def(&mut self) -> Result<ColSpec, String> {
        let name = self.expect_ident()?;
        let type_name = self.parse_type_name();
        let mut constraints = Vec::new();
        loop {
            if let Some(Token::Keyword(kw)) = self.peek() {
                let u = kw.to_uppercase();
                if u == "PRIMARY"
                    || u == "NOT"
                    || u == "UNIQUE"
                    || u == "DEFAULT"
                    || u == "CHECK"
                    || u == "REFERENCES"
                    || u == "AUTOINCREMENT"
                {
                    constraints.push(self.parse_column_constraint()?);
                    continue;
                }
            }
            if self.at(&Token::Punct(','))
                || self.at(&Token::Punct(')'))
                || self.at(&Token::Punct(';'))
                || self.at(&Token::Eof)
            {
                break;
            }
            // skip unknown tokens to avoid infinite loops
            if self.pos >= self.tokens.len() {
                break;
            }
            self.pos += 1;
        }
        Ok(ColSpec {
            name,
            type_name,
            constraints,
        })
    }

    fn parse_type_name(&mut self) -> String {
        let mut parts = Vec::new();
        while let Some(Token::Ident(s)) | Some(Token::Keyword(s)) = self.peek() {
            let u = s.to_uppercase();
            if u == "PRIMARY"
                || u == "NOT"
                || u == "UNIQUE"
                || u == "DEFAULT"
                || u == "CHECK"
                || u == "REFERENCES"
                || u == "AUTOINCREMENT"
                || u == ","
                || u == ")"
            {
                break;
            }
            if KEYWORDS.contains(&u.as_str()) && !TYPE_KEYWORDS.contains(&u.as_str()) {
                break;
            }
            parts.push(s.clone());
            self.pos += 1;
            // optional size spec
            if self.eat_punct('(') {
                let mut inner = String::from("(");
                while !self.at(&Token::Punct(')')) && !self.at(&Token::Eof) {
                    if let Some(t) = self.next() {
                        inner.push_str(&token_to_string(&t));
                    }
                }
                inner.push(')');
                self.expect(Token::Punct(')')).ok();
                parts.push(inner);
            }
        }
        if parts.is_empty() {
            "TEXT".to_string()
        } else {
            parts.join(" ")
        }
    }

    fn parse_column_constraint(&mut self) -> Result<ColumnConstraint, String> {
        if self.match_keyword("PRIMARY") {
            self.expect_keyword("KEY")?;
            let autoincrement = self.match_keyword("AUTOINCREMENT");
            Ok(ColumnConstraint::PrimaryKey { autoincrement })
        } else if self.match_keyword("NOT") {
            self.expect_keyword("NULL")?;
            Ok(ColumnConstraint::NotNull)
        } else if self.match_keyword("UNIQUE") {
            Ok(ColumnConstraint::Unique)
        } else if self.match_keyword("DEFAULT") {
            Ok(ColumnConstraint::Default(self.parse_expr()?))
        } else if self.match_keyword("CHECK") {
            self.expect(Token::Punct('('))?;
            let expr = self.parse_expr()?;
            self.expect(Token::Punct(')'))?;
            Ok(ColumnConstraint::Check(expr))
        } else if self.match_keyword("REFERENCES") {
            let table = self.expect_ident()?;
            let column = if self.eat_punct('(') {
                let c = self.expect_ident()?;
                self.expect(Token::Punct(')'))?;
                c
            } else {
                String::new()
            };
            Ok(ColumnConstraint::References { table, column })
        } else if self.match_keyword("AUTOINCREMENT") {
            Ok(ColumnConstraint::PrimaryKey { autoincrement: true })
        } else {
            Err("Expected column constraint".to_string())
        }
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint, String> {
        if self.match_keyword("CONSTRAINT") {
            let _ = self.expect_ident();
        }
        if self.match_keyword("PRIMARY") {
            self.expect_keyword("KEY")?;
            self.expect(Token::Punct('('))?;
            let cols = self.parse_ident_list()?;
            self.expect(Token::Punct(')'))?;
            Ok(TableConstraint::PrimaryKey(cols))
        } else if self.match_keyword("UNIQUE") {
            self.expect(Token::Punct('('))?;
            let cols = self.parse_ident_list()?;
            self.expect(Token::Punct(')'))?;
            Ok(TableConstraint::Unique(cols))
        } else if self.match_keyword("FOREIGN") {
            self.expect_keyword("KEY")?;
            self.expect(Token::Punct('('))?;
            let columns = self.parse_ident_list()?;
            self.expect(Token::Punct(')'))?;
            self.expect_keyword("REFERENCES")?;
            let table = self.expect_ident()?;
            let ref_columns = if self.eat_punct('(') {
                let c = self.parse_ident_list()?;
                self.expect(Token::Punct(')'))?;
                c
            } else {
                Vec::new()
            };
            Ok(TableConstraint::ForeignKey {
                columns,
                table,
                ref_columns,
            })
        } else if self.match_keyword("CHECK") {
            self.expect(Token::Punct('('))?;
            let expr = self.parse_expr()?;
            self.expect(Token::Punct(')'))?;
            Ok(TableConstraint::Check(expr))
        } else {
            Err("Expected table constraint".to_string())
        }
    }

    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.expect_keyword("INSERT")?;
        let or_replace = self.parse_or_replace()?;
        self.expect_keyword("INTO")?;
        let table = self.expect_ident()?;
        let columns = if self.eat_punct('(') {
            let cols = self.parse_ident_list()?;
            self.expect(Token::Punct(')'))?;
            Some(cols)
        } else {
            None
        };
        self.expect_keyword("VALUES")?;
        let mut values = Vec::new();
        loop {
            self.expect(Token::Punct('('))?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_expr()?);
                if !self.eat_punct(',') {
                    break;
                }
            }
            self.expect(Token::Punct(')'))?;
            values.push(row);
            if !self.eat_punct(',') {
                break;
            }
        }
        Ok(Statement::Insert {
            table,
            columns,
            values,
            or_replace,
        })
    }

    fn parse_or_replace(&mut self) -> Result<bool, String> {
        if self.match_keyword("OR") {
            if self.match_keyword("REPLACE")
                || self.match_keyword("IGNORE")
                || self.match_keyword("ABORT")
                || self.match_keyword("FAIL")
                || self.match_keyword("ROLLBACK")
            {
                Ok(true)
            } else {
                Err("Expected REPLACE, IGNORE, ABORT, FAIL, or ROLLBACK after OR".to_string())
            }
        } else {
            Ok(false)
        }
    }

    fn parse_update(&mut self) -> Result<Statement, String> {
        self.expect_keyword("UPDATE")?;
        let table = self.expect_ident()?;
        self.expect_keyword("SET")?;
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_ident()?;
            self.expect(Token::Punct('='))?;
            let expr = self.parse_expr()?;
            assignments.push(Assignment { column, expr });
            if !self.eat_punct(',') {
                break;
            }
        }
        let where_clause = if self.match_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement, String> {
        self.expect_keyword("DELETE")?;
        self.match_keyword("FROM");
        let table = self.expect_ident()?;
        let where_clause = if self.match_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete { table, where_clause })
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        loop {
            out.push(self.expect_ident()?);
            if !self.eat_punct(',') {
                break;
            }
        }
        Ok(out)
    }

    // ---------------- select ----------------
    fn parse_select(&mut self) -> Result<SelectStmt, String> {
        self.expect_keyword("SELECT")?;
        let distinct = self.match_keyword("DISTINCT");
        let mut columns = Vec::new();
        if self.eat_punct('*') {
            columns.push(SelectCol { expr: Expr::Star, alias: None });
        } else {
            loop {
                columns.push(self.parse_select_col()?);
                if !self.eat_punct(',') {
                    break;
                }
            }
        }
        let mut from: Option<TableRef> = None;
        let mut joins: Vec<JoinClause> = Vec::new();
        if self.match_keyword("FROM") {
            let (f, j) = self.parse_from()?;
            from = f;
            joins = j;
        }
        let where_clause = if self.match_keyword("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.match_keyword("GROUP") {
            self.expect_keyword("BY")?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };
        let having = if self.match_keyword("HAVING") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let order_by = if self.match_keyword("ORDER") {
            self.expect_keyword("BY")?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };
        let limit = if self.match_keyword("LIMIT") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let offset = if self.match_keyword("OFFSET") {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(SelectStmt {
            distinct,
            columns,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_col(&mut self) -> Result<SelectCol, String> {
        let expr = self.parse_expr()?;
        let alias = if self.match_keyword("AS") {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(SelectCol { expr, alias })
    }

    fn parse_from(&mut self) -> Result<(Option<TableRef>, Vec<JoinClause>), String> {
        let first = self.parse_table_ref()?;
        let mut joins = Vec::new();
        loop {
            if self.match_keyword("JOIN") {
                joins.push(JoinClause {
                    join_type: JoinType::Inner,
                    table: self.parse_table_ref()?,
                    on: if self.match_keyword("ON") {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    },
                });
            } else if self.match_keyword("INNER") {
                self.expect_keyword("JOIN")?;
                joins.push(JoinClause {
                    join_type: JoinType::Inner,
                    table: self.parse_table_ref()?,
                    on: if self.match_keyword("ON") {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    },
                });
            } else if self.match_keyword("LEFT") {
                self.match_keyword("OUTER");
                self.expect_keyword("JOIN")?;
                joins.push(JoinClause {
                    join_type: JoinType::Left,
                    table: self.parse_table_ref()?,
                    on: if self.match_keyword("ON") {
                        Some(self.parse_expr()?)
                    } else {
                        None
                    },
                });
            } else if self.match_keyword("CROSS") {
                self.expect_keyword("JOIN")?;
                joins.push(JoinClause {
                    join_type: JoinType::Cross,
                    table: self.parse_table_ref()?,
                    on: None,
                });
            } else {
                break;
            }
        }
        Ok((Some(first), joins))
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, String> {
        if self.eat_punct('(') {
            // subquery
            let sub = self.parse_select()?;
            self.expect(Token::Punct(')'))?;
            let alias = if self.match_keyword("AS") || matches!(self.peek(), Some(Token::Ident(_))) {
                Some(self.expect_ident()?)
            } else {
                None
            };
            return Ok(TableRef {
                name: alias.clone().unwrap_or_default(),
                alias,
                subquery: Some(Box::new(sub)),
            });
        }
        let name = self.expect_ident()?;
        let alias = if self.match_keyword("AS") {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(TableRef {
            name,
            alias,
            subquery: None,
        })
    }

    fn parse_order_by(&mut self) -> Result<Vec<(Expr, Order)>, String> {
        let mut out = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let order = if self.match_keyword("ASC") {
                Order::Asc
            } else if self.match_keyword("DESC") {
                Order::Desc
            } else {
                Order::Asc
            };
            out.push((expr, order));
            if !self.eat_punct(',') {
                break;
            }
        }
        Ok(out)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, String> {
        let mut out = Vec::new();
        loop {
            out.push(self.parse_expr()?);
            if !self.eat_punct(',') {
                break;
            }
        }
        Ok(out)
    }

    // ---------------- expressions ----------------
    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;
        while self.match_keyword("OR") {
            let right = self.parse_and()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not()?;
        while self.match_keyword("AND") {
            let right = self.parse_not()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, String> {
        if self.match_keyword("NOT") {
            let e = self.parse_not()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(e),
            });
        }
        self.parse_between()
    }

    fn parse_between(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_comparison()?;
        let mut negated = false;
        if self.match_keyword("NOT") {
            negated = true;
        }
        if self.match_keyword("BETWEEN") {
            let low = self.parse_comparison()?;
            self.expect_keyword("AND")?;
            let high = self.parse_comparison()?;
            left = Expr::Between {
                expr: Box::new(left),
                negated,
                low: Box::new(low),
                high: Box::new(high),
            };
        } else if self.match_keyword("IN") {
            self.expect(Token::Punct('('))?;
            if self.match_keyword("SELECT") {
                let sub = self.parse_select()?;
                self.expect(Token::Punct(')'))?;
                left = Expr::InSubquery {
                    expr: Box::new(left),
                    negated,
                    query: Box::new(sub),
                };
            } else {
                let mut list = Vec::new();
                if !self.at(&Token::Punct(')')) {
                    loop {
                        list.push(self.parse_expr()?);
                        if !self.eat_punct(',') {
                            break;
                        }
                    }
                }
                self.expect(Token::Punct(')'))?;
                left = Expr::InList {
                    expr: Box::new(left),
                    negated,
                    list,
                };
            }
        } else if self.match_keyword("LIKE") {
            let pattern = self.parse_comparison()?;
            let escape = if self.match_keyword("ESCAPE") {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            left = Expr::Like {
                expr: Box::new(left),
                negated,
                pattern: Box::new(pattern),
                escape,
            };
        } else if self.match_keyword("IS") {
            if self.match_keyword("NOT") {
                self.expect_keyword("NULL")?;
                left = Expr::IsNull(Box::new(left), true);
            } else {
                self.expect_keyword("NULL")?;
                left = Expr::IsNull(Box::new(left), false);
            }
        } else if negated {
            // we consumed NOT but it wasn't followed by BETWEEN/IN/LIKE/IS
            return Err("Unexpected NOT".to_string());
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_concat()?;
        loop {
            let op = if self.eat_punct('=') {
                Some(BinOp::Eq)
            } else if self.eat_two_punct('!', '=') {
                Some(BinOp::Neq)
            } else if self.eat_two_punct('<', '>') {
                Some(BinOp::Neq)
            } else if self.eat_two_punct('<', '=') {
                Some(BinOp::Lte)
            } else if self.eat_two_punct('>', '=') {
                Some(BinOp::Gte)
            } else if self.eat_punct('<') {
                Some(BinOp::Lt)
            } else if self.eat_punct('>') {
                Some(BinOp::Gt)
            } else {
                None
            };
            if let Some(op) = op {
                let right = self.parse_concat()?;
                left = Expr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_concat(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_add()?;
        while self.eat_two_punct('|', '|') {
            let right = self.parse_add()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinOp::Concat,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_add(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_mul()?;
        loop {
            let op = if self.eat_punct('+') {
                Some(BinOp::Add)
            } else if self.eat_punct('-') {
                Some(BinOp::Sub)
            } else {
                None
            };
            if let Some(op) = op {
                let right = self.parse_mul()?;
                left = Expr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary()?;
        loop {
            let op = if self.eat_punct('*') {
                Some(BinOp::Mul)
            } else if self.eat_punct('/') {
                Some(BinOp::Div)
            } else if self.eat_punct('%') {
                Some(BinOp::Mod)
            } else {
                None
            };
            if let Some(op) = op {
                let right = self.parse_unary()?;
                left = Expr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        if self.eat_punct('-') {
            let e = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(e),
            });
        }
        if self.eat_punct('+') {
            return self.parse_unary();
        }
        if self.match_keyword("NOT") {
            let e = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(e),
            });
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, String> {
        let expr = self.parse_primary()?;
        // Dotted identifiers and function calls start from a bare column name.
        if let Expr::Column { table: None, name } = expr {
            let mut parts = vec![name];
            while self.eat_punct('.') {
                if self.eat_punct('*') {
                    let table = parts.join(".");
                    return Ok(Expr::Column {
                        table: if table.is_empty() { None } else { Some(table) },
                        name: "*".to_string(),
                    });
                }
                let next = self.parse_primary()?;
                if let Expr::Column { table: None, name } = next {
                    parts.push(name);
                } else {
                    return Err("Invalid dotted expression".to_string());
                }
            }
            let name = parts.pop().unwrap();
            let table = if parts.is_empty() {
                None
            } else {
                Some(parts.join("."))
            };
            let mut expr = Expr::Column { table, name };
            if self.eat_punct('(') {
                if let Expr::Column { table: None, name } = expr {
                    let mut distinct = false;
                    if self.match_keyword("DISTINCT") {
                        distinct = true;
                    }
                    let mut args = Vec::new();
                    if !self.at(&Token::Punct(')')) {
                        loop {
                            if self.eat_punct('*') {
                                args.push(Expr::Star);
                            } else {
                                args.push(self.parse_expr()?);
                            }
                            if !self.eat_punct(',') {
                                break;
                            }
                        }
                    }
                    self.expect(Token::Punct(')'))?;
                    expr = Expr::Function {
                        name: name.to_uppercase(),
                        args,
                        distinct,
                    };
                } else {
                    return Err("Expected function name".to_string());
                }
            }
            return Ok(expr);
        }
        // Literals, NULL, parenthesized expressions, etc. cannot be followed by . or (.
        if self.eat_punct('.') || self.eat_punct('(') {
            return Err("Expected identifier".to_string());
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Token::Keyword(kw)) => {
                let u = kw.to_uppercase();
                if u == "NULL" {
                    self.pos += 1;
                    Ok(Expr::Null)
                } else if u == "TRUE" {
                    self.pos += 1;
                    Ok(Expr::Boolean(true))
                } else if u == "FALSE" {
                    self.pos += 1;
                    Ok(Expr::Boolean(false))
                } else if u == "CURRENT_TIMESTAMP" || u == "CURRENT_DATE" || u == "CURRENT_TIME" {
                    self.pos += 1;
                    Ok(Expr::Function { name: u, args: Vec::new(), distinct: false })
                } else if u == "CASE" {
                    self.pos += 1;
                    let expr = if !self.match_keyword("WHEN") {
                        let e = self.parse_expr()?;
                        self.expect_keyword("WHEN")?;
                        Some(Box::new(e))
                    } else {
                        None
                    };
                    let mut when = Vec::new();
                    loop {
                        let cond = if let Some(ref e) = expr {
                            // simple case: equality
                            Expr::Binary {
                                left: e.clone(),
                                op: BinOp::Eq,
                                right: Box::new(self.parse_expr()?),
                            }
                        } else {
                            self.parse_expr()?
                        };
                        self.expect_keyword("THEN")?;
                        let then = self.parse_expr()?;
                        when.push((cond, then));
                        if self.match_keyword("WHEN") {
                            continue;
                        }
                        break;
                    }
                    let else_ = if self.match_keyword("ELSE") {
                        Some(Box::new(self.parse_expr()?))
                    } else {
                        None
                    };
                    self.expect_keyword("END")?;
                    Ok(Expr::Case { expr, when, else_ })
                } else if u == "CAST" {
                    self.pos += 1;
                    self.expect(Token::Punct('('))?;
                    let e = self.parse_expr()?;
                    self.expect_keyword("AS")?;
                    let type_name = self.parse_type_name();
                    self.expect(Token::Punct(')'))?;
                    Ok(Expr::Cast {
                        expr: Box::new(e),
                        type_name,
                    })
                } else if u == "EXISTS" {
                    self.pos += 1;
                    self.expect(Token::Punct('('))?;
                    let sub = self.parse_select()?;
                    self.expect(Token::Punct(')'))?;
                    Ok(Expr::Exists(Box::new(sub)))
                } else if u == "NOT" {
                    // handled in parse_not
                    Err("Unexpected NOT".to_string())
                } else {
                    // keyword used as identifier (e.g. column name)
                    self.pos += 1;
                    Ok(Expr::Column {
                        table: None,
                        name: kw,
                    })
                }
            }
            Some(Token::Ident(s)) => {
                self.pos += 1;
                Ok(Expr::Column {
                    table: None,
                    name: s,
                })
            }
            Some(Token::String(s)) => {
                self.pos += 1;
                Ok(Expr::Text(s))
            }
            Some(Token::Number(s)) => {
                self.pos += 1;
                parse_number(&s)
            }
            Some(Token::Punct('(')) => {
                self.pos += 1;
                if self.match_keyword("SELECT") {
                    let sub = self.parse_select()?;
                    self.expect(Token::Punct(')'))?;
                    Ok(Expr::Subquery(Box::new(sub)))
                } else {
                    let e = self.parse_expr()?;
                    self.expect(Token::Punct(')'))?;
                    Ok(Expr::Nested(Box::new(e)))
                }
            }
            _ => Err(format!("Unexpected token {:?}", self.peek())),
        }
    }

    // ---------------- helper ----------------
    fn eat_punct(&mut self, c: char) -> bool {
        if self.at(&Token::Punct(c)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_two_punct(&mut self, c1: char, c2: char) -> bool {
        if self.at(&Token::Punct(c1)) {
            let next = self.tokens.get(self.pos + 1);
            if next == Some(&Token::Punct(c2)) {
                self.pos += 2;
                return true;
            }
        }
        false
    }
}

// ---------------- tokenizer ----------------
fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            // line comment
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            // block comment
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
            continue;
        }
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() {
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        s.push('\'');
                        i += 2;
                        continue;
                    } else {
                        i += 1;
                        break;
                    }
                }
                s.push(chars[i]);
                i += 1;
            }
            tokens.push(Token::String(s));
            continue;
        }
        if c == '"' || c == '`' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != quote {
                s.push(chars[i]);
                i += 1;
            }
            i += 1;
            tokens.push(Token::Ident(s));
            continue;
        }
        if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).map(|x| x.is_ascii_digit()).unwrap_or(false))
        {
            let mut s = String::new();
            if c == '.' {
                s.push('0');
            } else {
                s.push(c);
                i += 1;
            }
            let mut dot_seen = c == '.';
            while i < chars.len() {
                let ch = chars[i];
                if ch.is_ascii_digit() {
                    s.push(ch);
                    i += 1;
                } else if ch == '.' && !dot_seen {
                    dot_seen = true;
                    s.push(ch);
                    i += 1;
                } else if (ch == 'e' || ch == 'E') && (chars.get(i + 1) == Some(&'+') || chars.get(i + 1) == Some(&'-') || chars.get(i + 1).map(|x| x.is_ascii_digit()).unwrap_or(false)) {
                    s.push(ch);
                    i += 1;
                    if chars.get(i) == Some(&'+') || chars.get(i) == Some(&'-') {
                        s.push(chars[i]);
                        i += 1;
                    }
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        s.push(chars[i]);
                        i += 1;
                    }
                } else {
                    break;
                }
            }
            tokens.push(Token::Number(s));
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let mut s = String::new();
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                s.push(chars[i]);
                i += 1;
            }
            let u = s.to_uppercase();
            if KEYWORDS.contains(u.as_str()) {
                tokens.push(Token::Keyword(s));
            } else {
                tokens.push(Token::Ident(s));
            }
            continue;
        }
        // multi-char operators
        if c == '<' {
            if chars.get(i + 1) == Some(&'=') {
                tokens.push(Token::Punct('<'));
                tokens.push(Token::Punct('='));
                i += 2;
                continue;
            } else if chars.get(i + 1) == Some(&'>') {
                tokens.push(Token::Punct('<'));
                tokens.push(Token::Punct('>'));
                i += 2;
                continue;
            }
        }
        if c == '!' && chars.get(i + 1) == Some(&'=') {
            tokens.push(Token::Punct('!'));
            tokens.push(Token::Punct('='));
            i += 2;
            continue;
        }
        if c == '>' && chars.get(i + 1) == Some(&'=') {
            tokens.push(Token::Punct('>'));
            tokens.push(Token::Punct('='));
            i += 2;
            continue;
        }
        if c == '|' && chars.get(i + 1) == Some(&'|') {
            tokens.push(Token::Punct('|'));
            tokens.push(Token::Punct('|'));
            i += 2;
            continue;
        }
        tokens.push(Token::Punct(c));
        i += 1;
    }
    tokens.push(Token::Eof);
    tokens
}

fn parse_number(s: &str) -> Result<Expr, String> {
    if s.contains('.') || s.contains('e') || s.contains('E') {
        s.parse::<f64>()
            .map(Expr::Real)
            .map_err(|_| format!("Invalid number {}", s))
    } else {
        s.parse::<i64>()
            .map(Expr::Integer)
            .map_err(|_| format!("Invalid number {}", s))
    }
}

fn token_to_string(t: &Token) -> String {
    match t {
        Token::Eof => String::new(),
        Token::Keyword(s) | Token::Ident(s) | Token::String(s) | Token::Number(s) => s.clone(),
        Token::Punct(c) => c.to_string(),
    }
}

fn is_select_separator(k: &str) -> bool {
    matches!(
        k.to_uppercase().as_str(),
        "FROM" | "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "OFFSET"
    )
}

fn is_table_ref_separator(k: &str) -> bool {
    matches!(
        k.to_uppercase().as_str(),
        "JOIN" | "INNER" | "LEFT" | "RIGHT" | "CROSS" | "WHERE" | "GROUP" | "ORDER" | "LIMIT" | "OFFSET" | "ON" | "," | ";"
    )
}

static KEYWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    for kw in [
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "CREATE", "TABLE", "INDEX", "ON",
        "JOIN", "INNER", "LEFT", "RIGHT", "CROSS", "OUTER", "GROUP", "BY", "HAVING", "ORDER",
        "ASC", "DESC", "LIMIT", "OFFSET", "DISTINCT", "ALL", "AS", "AND", "OR", "NOT", "NULL",
        "IS", "IN", "BETWEEN", "LIKE", "GLOB", "REGEXP", "MATCH", "ESCAPE", "CASE", "WHEN",
        "THEN", "ELSE", "END", "CAST", "EXPLAIN", "PRAGMA", "VACUUM", "BEGIN", "COMMIT",
        "ROLLBACK", "END", "TRANSACTION", "SAVEPOINT", "RELEASE", "DELETE", "UPDATE", "SET",
        "ALTER", "ADD", "COLUMN", "DROP", "IF", "EXISTS", "UNIQUE", "PRIMARY", "KEY",
        "AUTOINCREMENT", "DEFAULT", "CHECK", "REFERENCES", "FOREIGN", "CONSTRAINT", "TRUE",
        "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "OR", "REPLACE",
        "IGNORE", "ABORT", "FAIL", "WORK", "QUERY", "PLAN",
    ] {
        set.insert(kw);
    }
    set
});
static TYPE_KEYWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    for kw in [
        "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "INT2", "INT8",
        "REAL", "DOUBLE", "FLOAT", "NUMERIC", "DECIMAL", "BOOLEAN", "DATE", "DATETIME",
        "TEXT", "VARCHAR", "CHAR", "CLOB", "BLOB", "STRING", "TIMESTAMP", "TIME",
    ] {
        set.insert(kw);
    }
    set
});
