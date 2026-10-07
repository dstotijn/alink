//! Local control socket. When a node is already running (normally `alink serve`), CLI
//! commands that need the network ask it over this Unix socket instead of binding a second
//! endpoint with the same identity.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::warn;

use crate::node::Node;
use crate::proto::PeerCard;
use crate::ticket::Ticket;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Flush {
        peer_id: Option<String>,
    },
    Join {
        ticket: String,
        alias: Option<String>,
    },
    FetchCard {
        peer_id: String,
    },
    Addr,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Response {
    Ok { value: serde_json::Value },
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Joined {
    pub name: String,
    pub card: PeerCard,
}

pub async fn serve(node: Arc<Node>) {
    let path = node.home.socket_path();
    if let Err(e) = prepare_socket_dir(&node.home.dir, &path) {
        warn!("control socket unavailable: {e:#}");
        return;
    }
    // We hold the node lock, so any existing socket file is stale.
    let _ = std::fs::remove_file(&path);
    let listener = match UnixListener::bind(&path) {
        Ok(listener) => listener,
        Err(e) => {
            warn!("binding control socket {}: {e}", path.display());
            return;
        }
    };
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(node, stream).await {
                warn!("control connection: {e:#}");
            }
        });
    }
}

/// Creates the socket's directory if needed. A directory outside the home (see
/// `Home::socket_path`) must be private and owned by the same user as the home.
fn prepare_socket_dir(home: &Path, socket: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let dir = socket.parent().context("socket path has no parent")?;
    if dir == home {
        return Ok(());
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    let meta = std::fs::metadata(dir)?;
    if meta.uid() != std::fs::metadata(home)?.uid() || meta.permissions().mode() & 0o077 != 0 {
        anyhow::bail!("{} must be private and owned by you", dir.display());
    }
    Ok(())
}

async fn handle(node: Arc<Node>, stream: UnixStream) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    let response = match dispatch(&node, serde_json::from_str(&line)?).await {
        Ok(value) => Response::Ok { value },
        Err(e) => Response::Error {
            message: format!("{e:#}"),
        },
    };
    let mut body = serde_json::to_vec(&response)?;
    body.push(b'\n');
    write.write_all(&body).await?;
    Ok(())
}

async fn dispatch(node: &Arc<Node>, request: Request) -> Result<serde_json::Value> {
    Ok(match request {
        Request::Flush { peer_id } => {
            node.deliver_pending(peer_id.as_deref()).await?;
            serde_json::Value::Null
        }
        Request::Join { ticket, alias } => {
            let (name, card) = node
                .join(&Ticket::decode(&ticket)?, alias.as_deref())
                .await?;
            serde_json::to_value(Joined { name, card })?
        }
        Request::FetchCard { peer_id } => serde_json::to_value(node.fetch_card(&peer_id).await?)?,
        Request::Addr => serde_json::to_value(node.addr().await)?,
    })
}

pub async fn call<T: serde::de::DeserializeOwned>(socket: &Path, request: &Request) -> Result<T> {
    let mut stream = UnixStream::connect(socket).await.with_context(|| {
        format!(
            "a node is running but its control socket {} is unreachable",
            socket.display()
        )
    })?;
    let mut body = serde_json::to_vec(request)?;
    body.push(b'\n');
    stream.write_all(&body).await?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await?;
    match serde_json::from_str(&line).context("invalid control response")? {
        Response::Ok { value } => Ok(serde_json::from_value(value)?),
        Response::Error { message } => Err(anyhow!(message)),
    }
}
