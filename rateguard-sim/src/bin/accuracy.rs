//! The accuracy report (spec §6.3).
//!
//!     cargo run -p rateguard-sim --release --bin accuracy              # print it
//!     cargo run -p rateguard-sim --release --bin accuracy -- --write   # into the guide
//!     cargo run -p rateguard-sim --release --bin accuracy -- --check   # the guide up to date?
//!
//! guide/how-it-works.md holds the table between `<!-- accuracy: begin -->`
//! and `<!-- accuracy: end -->`. CI runs `--check`: a change that moves the
//! numbers must come with the regenerated table.

use std::path::PathBuf;
use std::process::ExitCode;

use rateguard_sim::accuracy;

const BEGIN: &str = "<!-- accuracy: begin -->\n";
const END: &str = "<!-- accuracy: end -->";

const PAGE: &str = "guide/how-it-works.md";

fn page() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(PAGE)
}

// The page with `table` between the markers.
fn with_table(page: &str, table: &str) -> Option<String> {
    let begin = page.find(BEGIN)? + BEGIN.len();
    let end = page[begin..].find(END)? + begin;
    Some(format!("{}{table}{}", &page[..begin], &page[end..]))
}

fn main() -> ExitCode {
    let table = accuracy::report();
    let mode = std::env::args().nth(1);
    if mode.is_none() {
        print!("{table}");
        return ExitCode::SUCCESS;
    }
    let path = page();
    let current = std::fs::read_to_string(&path).expect("the guide next to the workspace");
    let Some(updated) = with_table(&current, &table) else {
        eprintln!("{PAGE} has no {} … {} section", BEGIN.trim(), END);
        return ExitCode::FAILURE;
    };
    match mode.as_deref() {
        Some("--write") => {
            std::fs::write(&path, updated).expect("the guide is writable");
            ExitCode::SUCCESS
        }
        Some("--check") if updated == current => ExitCode::SUCCESS,
        Some("--check") => {
            eprintln!(
                "The accuracy report in {PAGE} is out of date. Regenerate it:\n\
                 cargo run -p rateguard-sim --release --bin accuracy -- --write\n\n{table}"
            );
            ExitCode::FAILURE
        }
        _ => {
            eprintln!("usage: accuracy [--write | --check]");
            ExitCode::FAILURE
        }
    }
}
