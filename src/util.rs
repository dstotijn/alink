use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::proto::Attachment;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Sortable, prefixed IDs such as `msg_01K...`.
pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", ulid::Ulid::generate())
}

pub fn format_time(ms: u64) -> String {
    humantime::format_rfc3339_seconds(UNIX_EPOCH + Duration::from_millis(ms)).to_string()
}

pub fn short_id(id: &str) -> &str {
    &id[..id.len().min(10)]
}

/// Where the attachments of a received message are stored. Names are sanitized and made
/// unique, so the mapping is deterministic for a given message.
pub fn attachment_paths(
    files_dir: &Path,
    message_id: &str,
    attachments: &[Attachment],
) -> Vec<PathBuf> {
    let dir = files_dir.join(message_id);
    let mut used = std::collections::HashSet::new();
    attachments
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let mut name: String = a
                .name
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || "._-".contains(c) {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            name = name.trim_start_matches('.').to_string();
            if name.is_empty() {
                name = format!("attachment-{}", i + 1);
            }
            if !used.insert(name.clone()) {
                name = format!("{}-{name}", i + 1);
                used.insert(name.clone());
            }
            dir.join(name)
        })
        .collect()
}

pub fn save_attachments(
    files_dir: &Path,
    message_id: &str,
    attachments: &[Attachment],
) -> Result<Vec<PathBuf>> {
    let paths = attachment_paths(files_dir, message_id, attachments);
    if let Some(first) = paths.first() {
        let dir = first.parent().expect("attachment path has a parent");
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    for (path, attachment) in paths.iter().zip(attachments) {
        std::fs::write(path, &attachment.data)
            .with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(paths)
}

pub fn guess_media_type(name: &str, data: &[u8]) -> String {
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let known = match ext.as_str() {
        "patch" | "diff" => "text/x-diff",
        "md" | "markdown" => "text/markdown",
        "json" => "application/json",
        "txt" | "log" => "text/plain",
        "html" => "text/html",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "pdf" => "application/pdf",
        _ if std::str::from_utf8(data).is_ok() => "text/plain",
        _ => "application/octet-stream",
    };
    known.to_string()
}

pub fn is_text(media_type: &str) -> bool {
    media_type.starts_with("text/") || media_type == "application/json"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_names_are_sanitized_and_unique() {
        let a = |name: &str| Attachment {
            name: name.into(),
            media_type: "text/plain".into(),
            data: vec![],
        };
        let paths = attachment_paths(
            Path::new("/f"),
            "msg_1",
            &[a("../x.txt"), a("x.txt"), a("")],
        );
        assert_eq!(paths[0], Path::new("/f/msg_1/_x.txt"));
        assert_eq!(paths[1], Path::new("/f/msg_1/x.txt"));
        assert_eq!(paths[2], Path::new("/f/msg_1/attachment-3"));
    }
}
