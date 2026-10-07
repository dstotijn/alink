use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use iroh::SecretKey;
use serde::{Deserialize, Serialize};

/// Location of all local alink state: identity key, config, database, lock and socket.
#[derive(Debug, Clone)]
pub struct Home {
    pub dir: PathBuf,
}

impl Home {
    /// Resolves `$ALINK_HOME`, falling back to `$XDG_CONFIG_HOME/alink` and `~/.config/alink`.
    pub fn resolve() -> Result<Self> {
        if let Some(dir) = std::env::var_os("ALINK_HOME") {
            return Ok(Self { dir: dir.into() });
        }
        let base = match std::env::var_os("XDG_CONFIG_HOME") {
            Some(dir) => PathBuf::from(dir),
            None => {
                PathBuf::from(std::env::var_os("HOME").context("HOME is not set")?).join(".config")
            }
        };
        Ok(Self {
            dir: base.join("alink"),
        })
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.join("config.toml")
    }

    pub fn key_path(&self) -> PathBuf {
        self.dir.join("secret.key")
    }

    pub fn db_path(&self) -> PathBuf {
        self.dir.join("alink.db")
    }

    pub fn lock_path(&self) -> PathBuf {
        self.dir.join("node.lock")
    }

    /// Unix socket paths are limited to about 100 bytes, so deep homes use a per-user
    /// directory under /tmp, keyed by a hash of the home path.
    pub fn socket_path(&self) -> PathBuf {
        let path = self.dir.join("node.sock");
        if path.as_os_str().len() < 100 {
            return path;
        }
        let hash = self
            .dir
            .as_os_str()
            .as_encoded_bytes()
            .iter()
            .fold(0xcbf29ce484222325u64, |h, b| {
                (h ^ *b as u64).wrapping_mul(0x100000001b3)
            });
        let user = std::env::var("USER").unwrap_or_else(|_| "user".into());
        PathBuf::from(format!("/tmp/alink-{user}/{hash:016x}.sock"))
    }

    pub fn files_dir(&self) -> PathBuf {
        self.dir.join("files")
    }

    pub fn is_initialized(&self) -> bool {
        self.key_path().exists() && self.config_path().exists()
    }

    pub fn ensure_initialized(&self) -> Result<()> {
        if !self.is_initialized() {
            bail!(
                "alink is not initialized in {}; run `alink init` first",
                self.dir.display()
            );
        }
        Ok(())
    }

    pub fn load_secret_key(&self) -> Result<SecretKey> {
        let text = std::fs::read_to_string(self.key_path())
            .with_context(|| format!("reading {}", self.key_path().display()))?;
        let bytes = data_encoding::HEXLOWER
            .decode(text.trim().as_bytes())
            .context("secret key is not valid hex")?;
        SecretKey::try_from(bytes.as_slice()).context("secret key has the wrong length")
    }

    pub fn write_secret_key(&self, key: &SecretKey) -> Result<()> {
        let path = self.key_path();
        write_private(
            &path,
            data_encoding::HEXLOWER.encode(&key.to_bytes()).as_bytes(),
        )
    }

    pub fn load_config(&self) -> Result<Config> {
        let path = self.config_path();
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let config: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }
}

fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(contents)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Name advertised to peers in this endpoint's PeerCard.
    pub name: String,
    /// Maximum number of handler processes running at once.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    #[serde(default)]
    pub network: NetworkConfig,
    /// Locally configured capabilities that paired peers may invoke by name.
    #[serde(default, rename = "handler")]
    pub handlers: BTreeMap<String, HandlerConfig>,
}

fn default_max_concurrent() -> usize {
    2
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    /// `n0` uses iroh's public relays and DNS address lookup. `local` disables relays and
    /// address lookup, so peers are only reachable on addresses learned from invites.
    #[serde(default)]
    pub mode: NetworkMode,
    /// Custom iroh relay URLs, replacing the n0 relays in `n0` mode.
    #[serde(default)]
    pub relays: Vec<String>,
    /// Fixed UDP port to bind (default: random).
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NetworkMode {
    #[default]
    N0,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandlerConfig {
    /// Shown to peers in the PeerCard.
    pub description: Option<String>,
    /// Program and arguments. The request is written to its stdin; stdout becomes the response.
    pub command: Vec<String>,
    pub working_directory: Option<PathBuf>,
    /// For example "15m". Defaults to 30 minutes.
    pub timeout: Option<String>,
    /// For example "2MiB". Defaults to 1 MiB.
    pub max_output: Option<String>,
    /// Peer names or endpoint IDs allowed to invoke this handler; "*" allows every paired peer.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Extra environment variables for the handler process.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl HandlerConfig {
    pub fn timeout(&self) -> Result<Duration> {
        match &self.timeout {
            Some(text) => humantime::parse_duration(text)
                .with_context(|| format!("invalid handler timeout {text:?}")),
            None => Ok(Duration::from_secs(30 * 60)),
        }
    }

    pub fn max_output(&self) -> Result<usize> {
        match &self.max_output {
            Some(text) => parse_size(text),
            None => Ok(1024 * 1024),
        }
    }

    pub fn allows(&self, peer_id: &str, peer_name: &str) -> bool {
        self.peers
            .iter()
            .any(|p| p == "*" || p == peer_id || p == peer_name)
    }
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.max_concurrent == 0 {
            bail!("max_concurrent must be at least 1");
        }
        for (name, handler) in &self.handlers {
            if handler.command.is_empty() {
                bail!("handler {name:?} has an empty command");
            }
            handler
                .timeout()
                .with_context(|| format!("handler {name:?}"))?;
            handler
                .max_output()
                .with_context(|| format!("handler {name:?}"))?;
        }
        for relay in &self.network.relays {
            relay
                .parse::<iroh::RelayUrl>()
                .with_context(|| format!("invalid relay URL {relay:?}"))?;
        }
        Ok(())
    }
}

/// Parses sizes such as "512", "64KiB", "2MiB" or "1GiB".
pub fn parse_size(text: &str) -> Result<usize> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let number: usize = number
        .parse()
        .with_context(|| format!("invalid size {text:?}"))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" | "kb" | "k" => 1024,
        "mib" | "mb" | "m" => 1024 * 1024,
        "gib" | "gb" | "g" => 1024 * 1024 * 1024,
        _ => bail!("invalid size unit in {text:?}"),
    };
    Ok(number * multiplier)
}

pub fn initial_config_text(name: &str) -> String {
    format!(
        r#"# alink configuration. See `alink help` and the README for details.

# Name advertised to peers.
name = {name:?}

# Maximum number of handler processes running at once.
max_concurrent = 2

[network]
# "n0" uses iroh's public relays and DNS address lookup; "local" is for same-network testing.
mode = "n0"

# Handlers are capabilities this machine offers to paired peers. A peer can only name a
# handler; it never chooses the command, working directory or environment.
#
# [handler.review]
# description = "Read-only code review using Claude Code"
# command = ["claude", "-p", "--allowedTools", "Read,Grep,Glob"]
# working_directory = "/path/to/repo"
# timeout = "15m"
# max_output = "2MiB"
# peers = ["david"]
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sizes() {
        assert_eq!(parse_size("512").unwrap(), 512);
        assert_eq!(parse_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_size("2MiB").unwrap(), 2 * 1024 * 1024);
        assert!(parse_size("2 parsecs").is_err());
    }

    #[test]
    fn initial_config_parses() {
        let config: Config = toml::from_str(&initial_config_text("alice")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.name, "alice");
        assert!(config.handlers.is_empty());
    }

    #[test]
    fn handler_permissions() {
        let handler: HandlerConfig = toml::from_str(
            r#"
            command = ["cat"]
            peers = ["david"]
            "#,
        )
        .unwrap();
        assert!(handler.allows("abc", "david"));
        assert!(!handler.allows("abc", "mallory"));
    }
}
