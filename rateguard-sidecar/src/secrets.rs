//! The file of gossip secrets: one per line, in base64, the first for
//! signing. Base64, so that two secrets fit in one value of a Kubernetes
//! Secret with no binary separator between them.

use std::fmt;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rateguard::Secret;

/// Why a file of secrets was refused. Never carries secret bytes: a bad
/// line is named by its number only.
#[derive(Debug)]
#[non_exhaustive]
pub enum SecretsError {
    /// The file could not be read.
    Unreadable(std::io::Error),
    /// The line of this number, from 1, is not base64.
    NotBase64(usize),
    /// The line of this number, from 1, holds a secret of this many bytes,
    /// fewer than 32.
    Short(usize, usize),
    /// This many secrets, more than 2.
    TooMany(usize),
}

impl fmt::Display for SecretsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretsError::Unreadable(error) => write!(f, "cannot read it: {error}"),
            SecretsError::NotBase64(line) => write!(f, "line {line} is not base64"),
            SecretsError::Short(line, n) => write!(
                f,
                "line {line} holds a secret of {n} bytes, at least 32 are needed"
            ),
            SecretsError::TooMany(n) => write!(f, "{n} secrets, at most 2 are held"),
        }
    }
}
impl std::error::Error for SecretsError {}

/// The secrets in the file at `path`, in order: one per line in standard
/// base64 with padding, the first for signing. Whitespace around a line is
/// dropped, so a trailing newline and the `\r` of CRLF are; blank lines are
/// skipped.
pub fn read_secrets(path: &Path) -> Result<Vec<Secret>, SecretsError> {
    let content = std::fs::read(path).map_err(SecretsError::Unreadable)?;
    let mut secrets = Vec::new();
    for (i, line) in content.split(|&b| b == b'\n').enumerate() {
        let line = line.trim_ascii();
        if line.is_empty() {
            continue;
        }
        // base64's own error names the offending byte: only the line's
        // number may go into a message that ends up in logs.
        let bytes = STANDARD
            .decode(line)
            .map_err(|_| SecretsError::NotBase64(i + 1))?;
        let n = bytes.len();
        secrets.push(Secret::new(bytes).map_err(|_| SecretsError::Short(i + 1, n))?);
    }
    if secrets.len() > 2 {
        return Err(SecretsError::TooMany(secrets.len()));
    }
    Ok(secrets)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A file of its own per test, removed when dropped.
    struct File(std::path::PathBuf);
    impl File {
        fn with(name: &str, content: &[u8]) -> File {
            let path = std::env::temp_dir().join(format!(
                "rateguard-sidecar-secrets-{}-{name}",
                std::process::id()
            ));
            std::fs::write(&path, content).unwrap();
            File(path)
        }
    }
    impl Drop for File {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // 32 bytes of 0xA1 and of 0xB2, as `openssl rand -base64 32` would
    // print them.
    const A: &str = "oaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaE=";
    const B: &str = "srKysrKysrKysrKysrKysrKysrKysrKysrKysrKysrI=";

    #[test]
    fn a_file_with_two_secrets_and_a_trailing_newline_reads() {
        let file = File::with("two", format!("{A}\n{B}\n").as_bytes());
        assert_eq!(read_secrets(&file.0).unwrap().len(), 2);
    }

    #[test]
    fn crlf_and_blank_lines_are_tolerated() {
        let file = File::with("crlf", format!("\r\n  {A}\r\n\r\n\t{B} \r\n\n").as_bytes());
        assert_eq!(read_secrets(&file.0).unwrap().len(), 2);
    }

    #[test]
    fn a_bad_line_names_its_number_not_its_content() {
        let file = File::with("bad", format!("{A}\nqzx*!\n").as_bytes());
        let error = read_secrets(&file.0).unwrap_err();
        assert!(matches!(error, SecretsError::NotBase64(2)), "{error:?}");
        let shown = error.to_string();
        assert!(shown.contains("line 2"), "{shown}");
        for c in "qzx*!".chars() {
            assert!(!shown.contains(c), "{shown}");
        }
    }

    #[test]
    fn three_secrets_are_refused() {
        let file = File::with("three", format!("{A}\n{B}\n{A}\n").as_bytes());
        assert!(matches!(
            read_secrets(&file.0),
            Err(SecretsError::TooMany(3))
        ));
    }

    #[test]
    fn a_short_secret_is_refused() {
        // 31 bytes.
        let short = "oaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoaGhoQ==";
        let file = File::with("short", format!("{A}\n{short}\n").as_bytes());
        assert!(matches!(
            read_secrets(&file.0),
            Err(SecretsError::Short(2, 31))
        ));
    }

    #[test]
    fn a_missing_file_is_unreadable() {
        let path = std::env::temp_dir().join("rateguard-sidecar-secrets-none-such");
        assert!(matches!(
            read_secrets(&path),
            Err(SecretsError::Unreadable(_))
        ));
    }
}
