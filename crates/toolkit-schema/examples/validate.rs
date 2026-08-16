use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use debug_logging_toolkit_schema::LogEvent;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: cargo run -p debug-logging-toolkit-schema --example validate -- <file>")?;
    let reader = BufReader::new(File::open(&path)?);
    let mut valid = 0_u64;
    let mut invalid = 0_u64;

    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<LogEvent>(&line) {
            Ok(_) => valid += 1,
            Err(error) => {
                invalid += 1;
                if invalid <= 20 {
                    eprintln!("{}:{}: {error}", path.display(), index + 1);
                }
            }
        }
    }

    println!("valid={valid} invalid={invalid} file={}", path.display());
    if invalid > 20 {
        eprintln!("suppressed {} additional validation errors", invalid - 20);
    }
    if invalid > 0 {
        std::process::exit(1);
    }
    Ok(())
}
