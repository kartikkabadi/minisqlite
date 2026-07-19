use minisqlite::{Database, ExecuteResult, Value};

fn temp_db() -> (Database, String) {
    let path = format!("/tmp/minisqlite_test_{}.db", std::process::id());
    let _ = std::fs::remove_file(&path);
    let db = Database::open(&path).expect("open");
    (db, path)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
}

#[test]
fn create_insert_select() {
    let (mut db, path) = temp_db();

    db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute_sql("INSERT INTO t (name) VALUES ('Alice'), ('Bob')")
        .unwrap();

    match db.execute_sql("SELECT * FROM t ORDER BY id").unwrap() {
        ExecuteResult::Rows { header, rows } => {
            assert_eq!(header, vec!["id", "name"]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Integer(1), Value::Text("Alice".into())]);
            assert_eq!(rows[1], vec![Value::Integer(2), Value::Text("Bob".into())]);
        }
        _ => panic!("expected rows"),
    }

    cleanup(&path);
}

#[test]
fn aggregates_and_where() {
    let (mut db, path) = temp_db();

    db.execute_sql("CREATE TABLE sales (region TEXT, amount REAL)")
        .unwrap();
    db.execute_sql("INSERT INTO sales VALUES ('US', 100.0), ('US', 200.0), ('EU', 50.0)")
        .unwrap();

    match db
        .execute_sql("SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING total > 25 ORDER BY total DESC")
        .unwrap()
    {
        ExecuteResult::Rows { header, rows } => {
            assert_eq!(header, vec!["region", "total"]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Text("US".into()));
            assert_eq!(rows[0][1], Value::Real(300.0));
            assert_eq!(rows[1][0], Value::Text("EU".into()));
            assert_eq!(rows[1][1], Value::Real(50.0));
        }
        _ => panic!("expected rows"),
    }

    cleanup(&path);
}

#[test]
fn persistence_round_trip() {
    let path = format!("/tmp/minisqlite_persist_{}.db", std::process::id());
    let _ = std::fs::remove_file(&path);

    {
        let mut db = Database::open(&path).expect("open");
        db.execute_sql("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute_sql("INSERT INTO t (v) VALUES ('hello')").unwrap();
    }

    {
        let mut db = Database::open(&path).expect("reopen");
        match db.execute_sql("SELECT v FROM t").unwrap() {
            ExecuteResult::Rows { header, rows } => {
                assert_eq!(header, vec!["v"]);
                assert_eq!(rows, vec![vec![Value::Text("hello".into())]]);
            }
            _ => panic!("expected rows"),
        }
    }

    cleanup(&path);
}
