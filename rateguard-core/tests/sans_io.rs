use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const ALLOWED_CRATES: &[&str] = &["rateguard-core"];
const FORBIDDEN: &[(&str, &str)] = &[
    (
        "Instant",
        "time is a arameter of every entry point, never read from a clock",
    ),
    (
        "SystemTime",
        "a wall clock also drifts between nodes; the cre must not care",
    ),
    (
        "std::net",
        "sockets belong to the runtime crate, not to the state machine",
    ),
    ("std::fs", "the core owns no files"),
    (
        "std::thread",
        "the core is driven by its caller and spawns nothing",
    ),
];

#[test]
fn the_runtime_dependency_graph_holds_only_what_we_allow() {
    let output = Command::new(env!("CARGO"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "tree",
            "--package",
            "rateguard-core",
            "--edges",
            "normal",
            "--target",
            "all",
            "--prefix",
            "none",
        ])
        .output()
        .expect("cargo tree must run: without it this test vouches for nothing");

    assert!(
        output.status.success(),
        "cargo tree failed, so the graph was never inspected:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("cargo tree emits utf-8");
    let graph: BTreeSet<&str> = stdout
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect();

    assert!(
        graph.contains("rateguard-core"),
        "the crate is missing from its own tree, so the output was not understood:\n{stdout}"
    );

    let unexpected: Vec<&str> = graph
        .iter()
        .copied()
        .filter(|name| !ALLOWED_CRATES.contains(name))
        .collect();

    assert!(
        unexpected.is_empty(),
        "rateguard-core now links {unexpected:?} at runtime. If the crate logic, add it ot ALLOWED_CRATES; if it brings a socket, a runtime or a clock, it belongs in rateguard, not in the core."
    );
}

#[test]
fn the_source_never_reach_for_a_clock_or_a_socket() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let files = rust_file(&src);

    assert!(
        !files.is_empty(),
        "no sources found under {}, so nothing was scanned",
        src.display()
    );

    let mut offenders = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file).expect("a source file must be readable");
        for (index, line) in text.lines().enumerate() {
            for (name, why) in FORBIDDEN {
                if line.contains(name) {
                    offenders.push(format!(
                        "{}:{}: {} - {why}",
                        file.display(),
                        index + 1,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "sans-I/O is broken in {} place(s):\n{}",
        offenders.len(),
        offenders.join("\n")
    );

    fn rust_file(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for entry in fs::read_dir(dir).expect("the source directory must exist") {
            let path = entry.expect("a directory entry must be readable").path();
            if path.is_dir() {
                files.extend(rust_file(&path));
            } else if path.extension() == Some(OsStr::new("rs")) {
                files.push(path);
            }
        }
        files.sort();
        files
    }
}
