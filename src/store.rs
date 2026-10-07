//! Local SQLite state. The database is the source of truth for this endpoint: the outbox is
//! durable, so messages survive restarts and are retried until the peer is reachable.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;

use crate::proto::{Envelope, PeerCard, RequestState};
use crate::util::{new_id, now_ms};

/// Messages that cannot be delivered within this window are marked as expired.
pub const DELIVERY_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS peers (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    card          TEXT NOT NULL,
    addr          TEXT,
    added_at      INTEGER NOT NULL,
    last_seen_at  INTEGER
);

CREATE TABLE IF NOT EXISTS invites (
    id          TEXT PRIMARY KEY,
    secret      TEXT NOT NULL,
    name        TEXT,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    used_at     INTEGER,
    used_by     TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id               TEXT PRIMARY KEY,
    direction        TEXT NOT NULL CHECK (direction IN ('in', 'out')),
    peer_id          TEXT NOT NULL,
    thread_id        TEXT NOT NULL,
    reply_to         TEXT,
    kind             TEXT NOT NULL,
    request_id       TEXT,
    envelope         TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    received_at      INTEGER,
    read_at          INTEGER,
    -- out: queued | delivered | failed | expired; in: received
    delivery         TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT
);

CREATE INDEX IF NOT EXISTS messages_outbox ON messages (direction, delivery, next_attempt_at);
CREATE INDEX IF NOT EXISTS messages_thread ON messages (thread_id);

CREATE TABLE IF NOT EXISTS requests (
    id          TEXT PRIMARY KEY,
    direction   TEXT NOT NULL CHECK (direction IN ('in', 'out')),
    peer_id     TEXT NOT NULL,
    thread_id   TEXT NOT NULL,
    handler     TEXT NOT NULL,
    state       TEXT NOT NULL,
    note        TEXT,
    response    TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
"#;

pub struct Store {
    conn: Connection,
}

#[derive(Debug, Clone, Serialize)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub card: PeerCard,
    #[serde(skip)]
    pub addr: Option<iroh::EndpointAddr>,
    pub added_at: u64,
    pub last_seen_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct MessageRow {
    /// Insertion order, used as a cursor by `alink listen`.
    pub rowid: i64,
    pub id: String,
    pub direction: String,
    pub peer_id: String,
    pub thread_id: String,
    pub reply_to: Option<String>,
    pub kind: String,
    pub request_id: Option<String>,
    pub envelope: Envelope,
    pub created_at: u64,
    pub read_at: Option<u64>,
    pub delivery: String,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestRow {
    pub id: String,
    pub direction: String,
    pub peer_id: String,
    pub thread_id: String,
    pub handler: String,
    pub state: String,
    pub note: Option<String>,
    pub response: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Default)]
pub struct MessageFilter<'a> {
    pub unread_only: bool,
    pub thread_id: Option<&'a str>,
    pub peer_id: Option<&'a str>,
    pub after_rowid: Option<i64>,
    pub limit: Option<usize>,
}

pub enum InviteCheck {
    Valid { name: Option<String> },
    Invalid(&'static str),
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(10))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    // Peers

    /// Adds or updates a peer, choosing a unique local name based on `preferred_name`.
    pub fn upsert_peer(
        &self,
        card: &PeerCard,
        preferred_name: &str,
        addr: Option<&iroh::EndpointAddr>,
    ) -> Result<String> {
        if let Some(existing) = self.peer_by_id(&card.id)? {
            self.conn.execute(
                "UPDATE peers SET card = ?2, addr = COALESCE(?3, addr) WHERE id = ?1",
                params![card.id, serde_json::to_string(card)?, addr_json(addr)?],
            )?;
            return Ok(existing.name);
        }
        let base = sanitize_name(preferred_name);
        let mut name = base.clone();
        let mut suffix = 2;
        while self.peer_by_name(&name)?.is_some() {
            name = format!("{base}-{suffix}");
            suffix += 1;
        }
        self.conn.execute(
            "INSERT INTO peers (id, name, card, addr, added_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                card.id,
                name,
                serde_json::to_string(card)?,
                addr_json(addr)?,
                now_ms() as i64
            ],
        )?;
        Ok(name)
    }

    pub fn update_peer_card(&self, card: &PeerCard) -> Result<()> {
        self.conn.execute(
            "UPDATE peers SET card = ?2 WHERE id = ?1",
            params![card.id, serde_json::to_string(card)?],
        )?;
        Ok(())
    }

    pub fn touch_peer(&self, id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE peers SET last_seen_at = ?2 WHERE id = ?1",
            params![id, now_ms() as i64],
        )?;
        Ok(())
    }

    pub fn peer_by_id(&self, id: &str) -> Result<Option<Peer>> {
        self.conn
            .query_row("SELECT * FROM peers WHERE id = ?1", [id], peer_from_row)
            .optional()
            .map_err(Into::into)
    }

    pub fn peer_by_name(&self, name: &str) -> Result<Option<Peer>> {
        self.conn
            .query_row("SELECT * FROM peers WHERE name = ?1", [name], peer_from_row)
            .optional()
            .map_err(Into::into)
    }

    /// Resolves a local peer name, a full endpoint ID or a unique ID prefix.
    pub fn resolve_peer(&self, needle: &str) -> Result<Peer> {
        if let Some(peer) = self.peer_by_name(needle)? {
            return Ok(peer);
        }
        let matches: Vec<Peer> = self
            .list_peers()?
            .into_iter()
            .filter(|p| p.id.starts_with(needle))
            .collect();
        match matches.len() {
            1 => Ok(matches.into_iter().next().unwrap()),
            0 => bail!("unknown peer {needle:?}; see `alink peers`"),
            _ => bail!("peer {needle:?} is ambiguous"),
        }
    }

    pub fn list_peers(&self) -> Result<Vec<Peer>> {
        let mut stmt = self.conn.prepare("SELECT * FROM peers ORDER BY name")?;
        let rows = stmt.query_map([], peer_from_row)?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    pub fn rename_peer(&self, id: &str, new_name: &str) -> Result<()> {
        let new_name = sanitize_name(new_name);
        if self.peer_by_name(&new_name)?.is_some() {
            bail!("a peer named {new_name:?} already exists");
        }
        self.conn.execute(
            "UPDATE peers SET name = ?2 WHERE id = ?1",
            params![id, new_name],
        )?;
        Ok(())
    }

    pub fn remove_peer(&self, id: &str) -> Result<()> {
        self.conn.execute("DELETE FROM peers WHERE id = ?1", [id])?;
        self.conn.execute(
            "UPDATE messages SET delivery = 'failed', last_error = 'peer removed'
             WHERE peer_id = ?1 AND direction = 'out' AND delivery = 'queued'",
            [id],
        )?;
        Ok(())
    }

    // Invites

    pub fn create_invite(&self, name: Option<&str>, ttl: Duration) -> Result<(String, String)> {
        let id = new_id("inv");
        let secret = data_encoding::BASE32_NOPAD
            .encode(&rand::random::<[u8; 20]>())
            .to_ascii_lowercase();
        let now = now_ms();
        self.conn.execute(
            "INSERT INTO invites (id, secret, name, created_at, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, secret, name, now as i64, (now + ttl.as_millis() as u64) as i64],
        )?;
        Ok((id, secret))
    }

    pub fn check_invite(&self, id: &str, secret: &str) -> Result<InviteCheck> {
        let row: Option<(String, Option<String>, i64, Option<i64>)> = self
            .conn
            .query_row(
                "SELECT secret, name, expires_at, used_at FROM invites WHERE id = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((expected, name, expires_at, used_at)) = row else {
            return Ok(InviteCheck::Invalid("unknown invite"));
        };
        if !constant_time_eq(expected.as_bytes(), secret.as_bytes()) {
            return Ok(InviteCheck::Invalid("invalid invite secret"));
        }
        if used_at.is_some() {
            return Ok(InviteCheck::Invalid("invite was already used"));
        }
        if (expires_at as u64) < now_ms() {
            return Ok(InviteCheck::Invalid("invite expired"));
        }
        Ok(InviteCheck::Valid { name })
    }

    pub fn consume_invite(&self, id: &str, peer_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE invites SET used_at = ?2, used_by = ?3 WHERE id = ?1",
            params![id, now_ms() as i64, peer_id],
        )?;
        Ok(())
    }

    pub fn invite_used_by(&self, id: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT used_by FROM invites WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()?
            .flatten())
    }

    // Messages

    /// Queues an outgoing envelope for delivery.
    pub fn enqueue(&self, peer_id: &str, envelope: &Envelope) -> Result<()> {
        self.conn.execute(
            "INSERT INTO messages (id, direction, peer_id, thread_id, reply_to, kind, request_id,
                                   envelope, created_at, read_at, delivery)
             VALUES (?1, 'out', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, 'queued')",
            params![
                envelope.id,
                peer_id,
                envelope.thread_id,
                envelope.reply_to,
                envelope.payload.kind(),
                envelope.payload.request_id(),
                serde_json::to_string(envelope)?,
                envelope.created_at as i64,
            ],
        )?;
        Ok(())
    }

    /// Stores an incoming envelope. Returns false if it was already received.
    pub fn insert_incoming(&self, peer_id: &str, envelope: &Envelope, read: bool) -> Result<bool> {
        let now = now_ms() as i64;
        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO messages (id, direction, peer_id, thread_id, reply_to, kind,
                                             request_id, envelope, created_at, received_at,
                                             read_at, delivery)
             VALUES (?1, 'in', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'received')",
            params![
                envelope.id,
                peer_id,
                envelope.thread_id,
                envelope.reply_to,
                envelope.payload.kind(),
                envelope.payload.request_id(),
                serde_json::to_string(envelope)?,
                envelope.created_at as i64,
                now,
                read.then_some(now),
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn message(&self, id: &str) -> Result<Option<MessageRow>> {
        self.conn
            .query_row(
                "SELECT rowid, * FROM messages WHERE id = ?1",
                [id],
                message_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Outgoing messages that are due for a delivery attempt, oldest first.
    pub fn due_outgoing(&self, peer_id: Option<&str>) -> Result<Vec<MessageRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, * FROM messages
             WHERE direction = 'out' AND delivery = 'queued' AND next_attempt_at <= ?1
               AND (?2 IS NULL OR peer_id = ?2)
             ORDER BY rowid",
        )?;
        let rows = stmt.query_map(params![now_ms() as i64, peer_id], message_from_row)?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    pub fn mark_delivered(&self, id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE messages SET delivery = 'delivered', received_at = ?2, last_error = NULL
             WHERE id = ?1",
            params![id, now_ms() as i64],
        )?;
        Ok(())
    }

    /// Marks an outgoing message as permanently undeliverable.
    pub fn mark_failed(&self, id: &str, delivery: &str, reason: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE messages SET delivery = ?2, last_error = ?3 WHERE id = ?1",
            params![id, delivery, reason],
        )?;
        Ok(())
    }

    /// Schedules the next attempt for all queued messages to a peer with exponential backoff.
    pub fn backoff_peer(&self, peer_id: &str, error: &str) -> Result<()> {
        let now = now_ms() as i64;
        self.conn.execute(
            "UPDATE messages
             SET attempts = attempts + 1,
                 last_error = ?2,
                 next_attempt_at = ?3 + MIN(300000, 5000 * (1 << MIN(attempts, 6)))
             WHERE peer_id = ?1 AND direction = 'out' AND delivery = 'queued'",
            params![peer_id, error, now],
        )?;
        Ok(())
    }

    /// Makes queued messages for a peer due immediately, for example when it just connected.
    pub fn reset_backoff(&self, peer_id: &str) -> Result<usize> {
        Ok(self.conn.execute(
            "UPDATE messages SET next_attempt_at = 0
             WHERE peer_id = ?1 AND direction = 'out' AND delivery = 'queued' AND next_attempt_at > 0",
            [peer_id],
        )?)
    }

    /// Expires queued messages older than [`DELIVERY_TTL`], returning their rows.
    pub fn expire_outgoing(&self) -> Result<Vec<MessageRow>> {
        let cutoff = now_ms().saturating_sub(DELIVERY_TTL.as_millis() as u64) as i64;
        let mut stmt = self.conn.prepare(
            "SELECT rowid, * FROM messages
             WHERE direction = 'out' AND delivery = 'queued' AND created_at < ?1",
        )?;
        let rows: Vec<MessageRow> = stmt
            .query_map([cutoff], message_from_row)?
            .collect::<rusqlite::Result<_>>()?;
        for row in &rows {
            self.mark_failed(&row.id, "expired", "not delivered before expiry")?;
        }
        Ok(rows)
    }

    pub fn list_incoming(&self, filter: &MessageFilter) -> Result<Vec<MessageRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, * FROM messages
             WHERE direction = 'in' AND kind IN ('message', 'response')
               AND (?1 = 0 OR read_at IS NULL)
               AND (?2 IS NULL OR thread_id = ?2)
               AND (?3 IS NULL OR peer_id = ?3)
               AND rowid > ?4
             ORDER BY rowid
             LIMIT ?5",
        )?;
        let rows = stmt.query_map(
            params![
                filter.unread_only,
                filter.thread_id,
                filter.peer_id,
                filter.after_rowid.unwrap_or(0),
                filter.limit.map(|l| l as i64).unwrap_or(-1),
            ],
            message_from_row,
        )?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    /// Unread messages and responses, optionally excluding one thread.
    pub fn count_unread(&self, exclude_thread: Option<&str>) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM messages
             WHERE direction = 'in' AND kind IN ('message', 'response') AND read_at IS NULL
               AND (?1 IS NULL OR thread_id != ?1)",
            [exclude_thread],
            |r| r.get(0),
        )?)
    }

    /// The response a peer sent for one of our requests, if any.
    pub fn response_message(&self, request_id: &str) -> Result<Option<MessageRow>> {
        self.conn
            .query_row(
                "SELECT rowid, * FROM messages
                 WHERE direction = 'in' AND kind = 'response' AND request_id = ?1
                 ORDER BY rowid DESC LIMIT 1",
                [request_id],
                message_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Outgoing messages, newest first. Without `all`, only those not yet delivered.
    pub fn list_outgoing(&self, all: bool, limit: usize) -> Result<Vec<MessageRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, * FROM messages
             WHERE direction = 'out' AND (?1 OR delivery != 'delivered')
             ORDER BY rowid DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![all, limit as i64], message_from_row)?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    pub fn mark_read(&self, ids: &[String]) -> Result<()> {
        let now = now_ms() as i64;
        let mut stmt = self
            .conn
            .prepare("UPDATE messages SET read_at = ?2 WHERE id = ?1 AND read_at IS NULL")?;
        for id in ids {
            stmt.execute(params![id, now])?;
        }
        Ok(())
    }

    // Requests

    #[allow(clippy::too_many_arguments)]
    pub fn insert_request(
        &self,
        id: &str,
        direction: &str,
        peer_id: &str,
        thread_id: &str,
        handler: &str,
        state: RequestState,
        note: Option<&str>,
    ) -> Result<bool> {
        let now = now_ms() as i64;
        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO requests (id, direction, peer_id, thread_id, handler, state, note,
                                             created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![id, direction, peer_id, thread_id, handler, state.as_str(), note, now],
        )?;
        Ok(inserted > 0)
    }

    pub fn request(&self, id: &str) -> Result<Option<RequestRow>> {
        self.conn
            .query_row(
                "SELECT * FROM requests WHERE id = ?1",
                [id],
                request_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Updates a request's state unless it already reached a terminal state.
    pub fn update_request(
        &self,
        id: &str,
        state: RequestState,
        note: Option<&str>,
        response: Option<&str>,
    ) -> Result<bool> {
        let updated = self.conn.execute(
            "UPDATE requests
             SET state = ?2, note = COALESCE(?3, note), response = COALESCE(?4, response),
                 updated_at = ?5
             WHERE id = ?1 AND state IN ('submitted', 'working')",
            params![id, state.as_str(), note, response, now_ms() as i64],
        )?;
        Ok(updated > 0)
    }

    pub fn list_requests(&self, direction: Option<&str>, limit: usize) -> Result<Vec<RequestRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT * FROM requests WHERE (?1 IS NULL OR direction = ?1)
             ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![direction, limit as i64], request_from_row)?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    /// Atomically moves the oldest submitted incoming request to `working`.
    pub fn claim_next_request(&mut self) -> Result<Option<(RequestRow, Envelope)>> {
        let tx = self.conn.transaction()?;
        let row = tx
            .query_row(
                "SELECT * FROM requests WHERE direction = 'in' AND state = 'submitted'
                 ORDER BY created_at LIMIT 1",
                [],
                request_from_row,
            )
            .optional()?;
        let Some(mut row) = row else {
            return Ok(None);
        };
        tx.execute(
            "UPDATE requests SET state = 'working', updated_at = ?2 WHERE id = ?1",
            params![row.id, now_ms() as i64],
        )?;
        let envelope: String = tx.query_row(
            "SELECT envelope FROM messages WHERE id = ?1",
            [&row.id],
            |r| r.get(0),
        )?;
        tx.commit()?;
        row.state = "working".into();
        Ok(Some((row, serde_json::from_str(&envelope)?)))
    }

    /// Incoming requests left in `working` by a previous process that stopped mid-run.
    pub fn interrupted_requests(&self) -> Result<Vec<RequestRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM requests WHERE direction = 'in' AND state = 'working'")?;
        let rows = stmt.query_map([], request_from_row)?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }
}

fn addr_json(addr: Option<&iroh::EndpointAddr>) -> Result<Option<String>> {
    addr.map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

fn json_col<T: serde::de::DeserializeOwned>(row: &Row, name: &str) -> rusqlite::Result<T> {
    let text: String = row.get(name)?;
    serde_json::from_str(&text).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}

fn opt_u64(row: &Row, name: &str) -> rusqlite::Result<Option<u64>> {
    Ok(row.get::<_, Option<i64>>(name)?.map(|v| v as u64))
}

fn peer_from_row(row: &Row) -> rusqlite::Result<Peer> {
    let addr: Option<String> = row.get("addr")?;
    Ok(Peer {
        id: row.get("id")?,
        name: row.get("name")?,
        card: json_col(row, "card")?,
        addr: addr.and_then(|a| serde_json::from_str(&a).ok()),
        added_at: row.get::<_, i64>("added_at")? as u64,
        last_seen_at: opt_u64(row, "last_seen_at")?,
    })
}

fn message_from_row(row: &Row) -> rusqlite::Result<MessageRow> {
    Ok(MessageRow {
        rowid: row.get("rowid")?,
        id: row.get("id")?,
        direction: row.get("direction")?,
        peer_id: row.get("peer_id")?,
        thread_id: row.get("thread_id")?,
        reply_to: row.get("reply_to")?,
        kind: row.get("kind")?,
        request_id: row.get("request_id")?,
        envelope: json_col(row, "envelope")?,
        created_at: row.get::<_, i64>("created_at")? as u64,
        read_at: opt_u64(row, "read_at")?,
        delivery: row.get("delivery")?,
        attempts: row.get("attempts")?,
        last_error: row.get("last_error")?,
    })
}

fn request_from_row(row: &Row) -> rusqlite::Result<RequestRow> {
    Ok(RequestRow {
        id: row.get("id")?,
        direction: row.get("direction")?,
        peer_id: row.get("peer_id")?,
        thread_id: row.get("thread_id")?,
        handler: row.get("handler")?,
        state: row.get("state")?,
        note: row.get("note")?,
        response: row.get("response")?,
        created_at: row.get::<_, i64>("created_at")? as u64,
        updated_at: row.get::<_, i64>("updated_at")? as u64,
    })
}

/// Peer names are used on the command line, so keep them shell-friendly.
pub fn sanitize_name(name: &str) -> String {
    let cleaned: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches('-').to_ascii_lowercase();
    if cleaned.is_empty() {
        "peer".into()
    } else {
        cleaned
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl RequestRow {
    pub fn state(&self) -> Result<RequestState> {
        RequestState::parse(&self.state)
            .ok_or_else(|| anyhow!("unknown request state {}", self.state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ENVELOPE_VERSION, Payload};

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn card(id: &str, name: &str) -> PeerCard {
        PeerCard {
            v: 1,
            id: id.into(),
            name: name.into(),
            handlers: vec![],
        }
    }

    fn message(id: &str) -> Envelope {
        Envelope {
            v: ENVELOPE_VERSION,
            id: id.into(),
            thread_id: "thr_1".into(),
            reply_to: None,
            created_at: now_ms(),
            payload: Payload::Message {
                text: "hi".into(),
                attachments: vec![],
            },
        }
    }

    #[test]
    fn peer_names_are_unique() {
        let s = store();
        assert_eq!(
            s.upsert_peer(&card("a1", "Alice Claude"), "Alice Claude", None)
                .unwrap(),
            "alice-claude"
        );
        assert_eq!(
            s.upsert_peer(&card("b2", "alice-claude"), "alice-claude", None)
                .unwrap(),
            "alice-claude-2"
        );
        assert_eq!(s.resolve_peer("b").unwrap().name, "alice-claude-2");
    }

    #[test]
    fn invites_are_single_use() {
        let s = store();
        let (id, secret) = s.create_invite(None, Duration::from_secs(60)).unwrap();
        assert!(matches!(
            s.check_invite(&id, "wrong").unwrap(),
            InviteCheck::Invalid(_)
        ));
        assert!(matches!(
            s.check_invite(&id, &secret).unwrap(),
            InviteCheck::Valid { .. }
        ));
        s.consume_invite(&id, "peer").unwrap();
        assert!(matches!(
            s.check_invite(&id, &secret).unwrap(),
            InviteCheck::Invalid(_)
        ));
    }

    #[test]
    fn incoming_is_idempotent_and_outbox_backs_off() {
        let s = store();
        assert!(s.insert_incoming("p", &message("msg_1"), false).unwrap());
        assert!(!s.insert_incoming("p", &message("msg_1"), false).unwrap());

        s.enqueue("p", &message("msg_2")).unwrap();
        assert_eq!(s.due_outgoing(None).unwrap().len(), 1);
        s.backoff_peer("p", "offline").unwrap();
        assert!(s.due_outgoing(None).unwrap().is_empty());
        s.reset_backoff("p").unwrap();
        assert_eq!(s.due_outgoing(Some("p")).unwrap().len(), 1);
    }

    #[test]
    fn requests_do_not_leave_terminal_states() {
        let mut s = store();
        let envelope = message("req_1");
        s.insert_incoming("p", &envelope, true).unwrap();
        s.insert_request(
            "req_1",
            "in",
            "p",
            "thr_1",
            "review",
            RequestState::Submitted,
            None,
        )
        .unwrap();
        let (row, _) = s.claim_next_request().unwrap().unwrap();
        assert_eq!(row.state, "working");
        assert!(s.claim_next_request().unwrap().is_none());
        assert!(
            s.update_request("req_1", RequestState::Completed, None, Some("ok"))
                .unwrap()
        );
        assert!(
            !s.update_request("req_1", RequestState::Working, None, None)
                .unwrap()
        );
    }
}
