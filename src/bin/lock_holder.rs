use std::io::{self, Read, Write};

use minisqlite::{Durability, StoreBuilder};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: lock_holder <path>");
        std::process::exit(2);
    }
    let path = &args[1];
    let _store = StoreBuilder::new(path)
        .durability(Durability::Memory)
        .open()
        .expect("lock holder failed to open store");

    let mut stdout = io::stdout();
    writeln!(stdout, "LOCKED").unwrap();
    stdout.flush().unwrap();

    let mut stdin = io::stdin();
    let mut buf = [0u8; 1];
    let _ = stdin.read(&mut buf);
}
