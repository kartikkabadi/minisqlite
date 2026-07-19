mod btree;
mod catalog;
mod executor;
mod functions;
mod pager;
mod sql;
mod types;
mod wal;

use executor::{Database, ExecuteResult};
use sql::Parser;
use std::io::{self, BufRead, Write};
use std::time::Instant;
use types::Value;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).map(|s| s.as_str()).unwrap_or("test.db");

    println!("MiniSQLite v0.2.0");
    println!("A from-scratch relational database engine in Rust");
    println!("Database file: {}", db_path);
    println!("Type .help for commands\n");

    let mut db = match Database::open(db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Fatal: cannot open database: {}", e);
            std::process::exit(1);
        }
    };

    let stdin = io::stdin();
    let mut input_buffer = String::new();
    let mut in_transaction = false;

    loop {
        let prompt = if in_transaction { "   ...> " } else { "minisql> " };
        print!("{}", prompt);
        io::stdout().flush().unwrap();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("I/O error: {}", e);
                break;
            }
        }

        let trimmed = line.trim();

        if trimmed.starts_with('.') && input_buffer.is_empty() {
            match trimmed.to_lowercase().as_str() {
                ".quit" | ".exit" | ".q" => break,
                ".tables" => {
                    let tables = db.list_tables();
                    if tables.is_empty() {
                        println!("(no tables)");
                    } else {
                        for t in &tables {
                            println!("  {}", t);
                        }
                    }
                }
                ".schema" => {
                    for s in db.show_schemas() {
                        println!("{};", s);
                    }
                }
                ".indexes" => {
                    for idx in db.list_indexes() {
                        println!("  {}", idx);
                    }
                }
                ".dump" => {
                    println!("-- MiniSQLite dump");
                    for s in db.show_schemas() {
                        println!("{};", s);
                    }
                    for (tbl, rows) in db.dump_all() {
                        for row in rows {
                            println!(
                                "INSERT INTO {} VALUES ({});",
                                tbl,
                                row.iter()
                                    .map(sql_literal)
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                        }
                    }
                }
                ".help" => {
                    println!("  .tables    List all tables");
                    println!("  .schema    Show CREATE statements");
                    println!("  .indexes   List indexes");
                    println!("  .dump      Dump database as SQL");
                    println!("  .stats     Show database statistics");
                    println!("  .quit      Exit");
                }
                ".stats" => {
                    let stats = db.get_stats();
                    for (k, v) in stats {
                        println!("  {}: {}", k, v);
                    }
                }
                _ => println!("Unknown command: {}", trimmed),
            }
            continue;
        }

        if trimmed.is_empty() && input_buffer.is_empty() {
            continue;
        }

        input_buffer.push_str(trimmed);
        input_buffer.push(' ');

        if !input_buffer.trim_end().ends_with(';') && !trimmed.is_empty() {
            continue;
        }

        let sql_str = input_buffer.trim().trim_end_matches(';').trim().to_string();
        input_buffer.clear();

        if sql_str.is_empty() {
            continue;
        }

        let upper = sql_str.to_uppercase();
        if upper.starts_with("BEGIN") {
            in_transaction = true;
        }
        if upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("END")
        {
            in_transaction = false;
        }

        let start = Instant::now();
        let mut parser = Parser::new(&sql_str);
        match parser.parse() {
            Ok(stmt) => match db.execute(stmt) {
                Ok(ExecuteResult::Ok(msg)) => {
                    println!("{} ({:.3}ms)", msg, start.elapsed().as_secs_f64() * 1000.0);
                }
                Ok(ExecuteResult::Rows { header, rows }) => {
                    print_table(&header, &rows);
                    println!(
                        "({} row(s) in {:.3}ms)",
                        rows.len(),
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            },
            Err(e) => {
                eprintln!("Parse error: {}", e);
            }
        }
    }

    println!("Bye!");
}

fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Blob(b) => format!("x'{}'", b.iter().map(|x| format!("{:02x}", x)).collect::<String>()),
    }
}

fn print_table(header: &[String], rows: &[Vec<Value>]) {
    if rows.is_empty() {
        println!("(empty set)");
        return;
    }
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(val.to_string().len());
            }
        }
    }
    let sep: Vec<String> = widths.iter().map(|w| "─".repeat(w + 2)).collect();
    println!("┌{}┐", sep.join("┬"));
    let hl: Vec<String> = header
        .iter()
        .enumerate()
        .map(|(i, h)| format!(" {:<width$} ", h, width = widths[i]))
        .collect();
    println!("│{}│", hl.join("│"));
    println!("├{}┤", sep.join("┼"));
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| format!(" {:>width$} ", v, width = widths[i]))
            .collect();
        println!("│{}│", cells.join("│"));
    }
    println!("└{}┘", sep.join("┴"));
}
