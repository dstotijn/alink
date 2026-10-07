//! Invite tickets: `alink://invite/<base64url(json)>`.
//!
//! A ticket carries the inviter's iroh address plus a one-time secret. Anyone holding the
//! ticket can redeem it once, so share it like a password: over a channel you trust.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const PREFIX: &str = "alink://invite/";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket {
    pub v: u8,
    /// Inviter's display name, shown to the joiner before connecting.
    pub name: String,
    pub addr: iroh::EndpointAddr,
    pub invite_id: String,
    pub secret: String,
    pub expires_at: u64,
}

impl Ticket {
    pub fn encode(&self) -> Result<String> {
        let json = serde_json::to_vec(self)?;
        Ok(format!(
            "{PREFIX}{}",
            data_encoding::BASE64URL_NOPAD.encode(&json)
        ))
    }

    pub fn decode(text: &str) -> Result<Self> {
        let text = text.trim();
        let Some(body) = text.strip_prefix(PREFIX) else {
            bail!("not an alink invite (expected it to start with {PREFIX})");
        };
        let json = data_encoding::BASE64URL_NOPAD
            .decode(body.as_bytes())
            .context("invite is not valid base64url")?;
        let ticket: Ticket = serde_json::from_slice(&json).context("invite is malformed")?;
        if ticket.v != 1 {
            bail!("unsupported invite version {}", ticket.v);
        }
        Ok(ticket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let id = iroh::SecretKey::generate().public();
        let ticket = Ticket {
            v: 1,
            name: "alice".into(),
            addr: iroh::EndpointAddr::new(id).with_ip_addr("127.0.0.1:4433".parse().unwrap()),
            invite_id: "inv_1".into(),
            secret: "s3cret".into(),
            expires_at: 42,
        };
        let decoded = Ticket::decode(&ticket.encode().unwrap()).unwrap();
        assert_eq!(decoded.addr, ticket.addr);
        assert_eq!(decoded.secret, "s3cret");
        assert!(Ticket::decode("https://example.com").is_err());
    }
}
