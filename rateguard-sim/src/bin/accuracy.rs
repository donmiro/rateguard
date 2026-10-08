//! The accuracy report (spec §6.3).
//!
//!     cargo run -p rateguard-sim --release --bin accuracy              # print it
//!     cargo run -p rateguard-sim --release --bin accuracy -- --write   # into README.md
//!     cargo run -p rateguard-sim --release --bin accuracy -- --check   # README up to date?
//!
//! The README holds the table between `<!-- accuracy: begin -->` and
//! `<!-- accuracy: end -->`. CI runs `--check`: a change that moves the
//! numbers must come with the regenerated table.

use std::path::PathBuf;
use std::process::ExitCode;

use rateguard_sim::accuracy;

const BEGIN: &str = "<!-- accuracy: begin -->\n";
const END: &str = "<!-- accuracy: end -->";

fn readme() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../README.md")
}

// The README with `table` between the markers.
fn with_table(readme: &str, table: &str) -> Option<String> {
    let begin = readme.find(BEGIN)? + BEGIN.len();
    let end = readme[begin..].find(END)? + begin;
    Some(format!("{}{table}{}", &readme[..begin], &readme[end..]))
}

fn main() -> ExitCode {
    let table = accuracy::markdown(&accuracy::rows());
    let mode = std::env::args().nth(1);
    if mode.is_none() {
        print!("{table}");
        return ExitCode::SUCCESS;
    }
    let path = readme();
    let current = std::fs::read_to_string(&path).expect("README.md next to the workspace");
    let Some(updated) = with_table(&current, &table) else {
        eprintln!("README.md has no {} … {} section", BEGIN.trim(), END);
        return ExitCode::FAILURE;
    };
    match mode.as_deref() {
        Some("--write") => {
            std::fs::write(&path, updated).expect("README.md is writable");
            ExitCode::SUCCESS
        }
        Some("--check") if updated == current => ExitCode::SUCCESS,
        Some("--check") => {
            eprintln!(
                "The accuracy report in README.md is out of date. Regenerate it:\n\
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
