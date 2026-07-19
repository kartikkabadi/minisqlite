use minisqlite::{Database, ExecuteResult, Value};

fn main() {
    let path = "/tmp/minisqlite_example.db";
    let _ = std::fs::remove_file(path);
    let mut db = Database::open(path).expect("open database");

    db.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)")
        .unwrap();

    db.execute_sql("INSERT INTO users (name, active) VALUES ('Alice', 1), ('Bob', 0)")
        .unwrap();

    match db
        .execute_sql("SELECT * FROM users WHERE active = 1")
        .unwrap()
    {
        ExecuteResult::Rows { header, rows } => {
            println!("{}", header.join(", "));
            for row in rows {
                println!(
                    "{}",
                    row.iter()
                        .map(Value::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        _ => panic!("expected rows"),
    }
}
