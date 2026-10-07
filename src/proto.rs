//! Wire types exchanged between alink nodes over the `alink/1` ALPN.
//!
//! Each frame travels on its own bidirectional QUIC stream: the client writes one JSON
//! [`Frame`] and finishes the stream, the server answers with one JSON [`Reply`]. iroh's
//! QUIC handshake authenticates both endpoint IDs, so the sender of a frame is always the
//! connection's remote ID and never a field inside the frame.
//!
//! The payload model borrows A2A's concepts (messages, tasks with a lifecycle, artifacts,
//! contexts) without implementing the A2A protocol itself.

use serde::{Deserialize, Serialize};

pub const ALPN: &[u8] = b"alink/1";

/// Upper bound for a single encoded frame or reply.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Upper bound for the raw attachment bytes in one envelope.
pub const MAX_ATTACHMENTS: usize = 8 * 1024 * 1024;

pub const ENVELOPE_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// Deliver one envelope. Only accepted from paired peers.
    Deliver { envelope: Envelope },
    /// Redeem a one-time invite and introduce the joining endpoint.
    Join {
        invite_id: String,
        secret: String,
        card: PeerCard,
        addr: Option<iroh::EndpointAddr>,
    },
    /// Ask for the PeerCard the remote offers to us.
    GetCard,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    /// The envelope is durably stored by the receiver.
    Ack {
        id: String,
    },
    /// The receiver will never accept this frame; do not retry.
    Rejected {
        reason: String,
    },
    Joined {
        card: PeerCard,
    },
    Card {
        card: PeerCard,
    },
}

/// One immutable unit of communication. Envelope IDs are unique and make delivery idempotent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u8,
    pub id: String,
    /// Groups related envelopes, like A2A's `contextId`.
    pub thread_id: String,
    pub reply_to: Option<String>,
    /// Unix milliseconds, as claimed by the sender.
    pub created_at: u64,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Payload {
    /// Free-form message for the agent on the other side.
    Message {
        text: String,
        #[serde(default)]
        attachments: Vec<Attachment>,
    },
    /// Invoke a named handler on the receiver. The envelope ID is the request ID.
    Request {
        handler: String,
        prompt: String,
        #[serde(default)]
        attachments: Vec<Attachment>,
    },
    /// Progress for a request (for example `working`).
    Status {
        request_id: String,
        state: RequestState,
        note: Option<String>,
    },
    /// Final result of a request.
    Response {
        request_id: String,
        state: RequestState,
        text: String,
        #[serde(default)]
        attachments: Vec<Attachment>,
    },
    /// Ask the receiver to stop working on a request.
    Cancel { request_id: String },
}

impl Payload {
    pub fn kind(&self) -> &'static str {
        match self {
            Payload::Message { .. } => "message",
            Payload::Request { .. } => "request",
            Payload::Status { .. } => "status",
            Payload::Response { .. } => "response",
            Payload::Cancel { .. } => "cancel",
        }
    }

    pub fn text(&self) -> Option<&str> {
        match self {
            Payload::Message { text, .. } | Payload::Response { text, .. } => Some(text),
            Payload::Request { prompt, .. } => Some(prompt),
            Payload::Status { note, .. } => note.as_deref(),
            Payload::Cancel { .. } => None,
        }
    }

    pub fn attachments(&self) -> &[Attachment] {
        match self {
            Payload::Message { attachments, .. }
            | Payload::Request { attachments, .. }
            | Payload::Response { attachments, .. } => attachments,
            _ => &[],
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        match self {
            Payload::Status { request_id, .. }
            | Payload::Response { request_id, .. }
            | Payload::Cancel { request_id } => Some(request_id),
            _ => None,
        }
    }
}

/// Request lifecycle, modelled on A2A's TaskState.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Submitted,
    Working,
    Completed,
    Failed,
    Canceled,
    Rejected,
}

impl RequestState {
    pub fn as_str(self) -> &'static str {
        match self {
            RequestState::Submitted => "submitted",
            RequestState::Working => "working",
            RequestState::Completed => "completed",
            RequestState::Failed => "failed",
            RequestState::Canceled => "canceled",
            RequestState::Rejected => "rejected",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "submitted" => RequestState::Submitted,
            "working" => RequestState::Working,
            "completed" => RequestState::Completed,
            "failed" => RequestState::Failed,
            "canceled" => RequestState::Canceled,
            "rejected" => RequestState::Rejected,
            _ => return None,
        })
    }

    pub fn is_terminal(self) -> bool {
        !matches!(self, RequestState::Submitted | RequestState::Working)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub name: String,
    pub media_type: String,
    #[serde(with = "base64_bytes")]
    pub data: Vec<u8>,
}

/// What a peer advertises about itself, a reduced A2A Agent Card. The handler list is
/// filtered per requesting peer, so peers only see capabilities they may invoke.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerCard {
    pub v: u8,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub handlers: Vec<HandlerInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerInfo {
    pub name: String,
    pub description: Option<String>,
}

mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&data_encoding::BASE64.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(d)?;
        data_encoding::BASE64
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_with_flattened_payload() {
        let envelope = Envelope {
            v: ENVELOPE_VERSION,
            id: "req_1".into(),
            thread_id: "thr_1".into(),
            reply_to: None,
            created_at: 1,
            payload: Payload::Request {
                handler: "review".into(),
                prompt: "look".into(),
                attachments: vec![Attachment {
                    name: "a.patch".into(),
                    media_type: "text/x-diff".into(),
                    data: b"diff".to_vec(),
                }],
            },
        };
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["kind"], "request");
        assert_eq!(json["attachments"][0]["data"], "ZGlmZg==");
        let back: Envelope = serde_json::from_value(json).unwrap();
        assert_eq!(back.payload.attachments()[0].data, b"diff");
    }
}
