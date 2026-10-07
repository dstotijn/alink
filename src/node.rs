//! The iroh endpoint: accepts frames from peers, delivers the durable outbox, and handles
//! invites. Exactly one node runs per alink home at a time, guarded by a file lock: either
//! the long-running `alink serve`, or a temporary node started by a CLI command.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, presets};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode};
use tokio::sync::{Notify, oneshot};
use tracing::{debug, info, warn};

use crate::config::{Config, Home, NetworkMode};
use crate::proto::{
    ALPN, ENVELOPE_VERSION, Envelope, Frame, HandlerInfo, MAX_ATTACHMENTS, MAX_FRAME, Payload,
    PeerCard, Reply, RequestState,
};
use crate::store::{InviteCheck, MessageRow, Peer, Store};
use crate::ticket::Ticket;
use crate::util::{new_id, now_ms, save_attachments};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

pub struct Node {
    pub home: Home,
    pub config: Config,
    pub endpoint: Endpoint,
    store: Mutex<Store>,
    lookup: MemoryLookup,
    /// Wakes the outbox flusher.
    pub outbox: Notify,
    /// Wakes the request runner (only used by `alink serve`).
    pub requests: Notify,
    deliver_lock: tokio::sync::Mutex<()>,
    /// Cancellation senders for handler processes that are currently running.
    pub running: Mutex<HashMap<String, oneshot::Sender<()>>>,
    _lock: std::fs::File,
}

impl Node {
    /// Binds the iroh endpoint and starts accepting connections and flushing the outbox.
    /// `lock` must be the exclusively locked node lock file.
    pub async fn start(home: Home, config: Config, lock: std::fs::File) -> Result<Arc<Self>> {
        let store = Store::open(&home.db_path())?;
        let lookup = MemoryLookup::new();
        let endpoint = bind(&home, &config, &lookup).await?;

        for peer in store.list_peers()? {
            if let Some(addr) = peer.addr {
                lookup.add_endpoint_info(addr);
            }
        }

        let node = Arc::new(Self {
            home,
            config,
            endpoint,
            store: Mutex::new(store),
            lookup,
            outbox: Notify::new(),
            requests: Notify::new(),
            deliver_lock: tokio::sync::Mutex::new(()),
            running: Mutex::new(HashMap::new()),
            _lock: lock,
        });
        tokio::spawn(node.clone().accept_loop());
        tokio::spawn(node.clone().flush_loop());
        tokio::spawn(crate::control::serve(node.clone()));
        debug!(id = %node.endpoint.id(), "node started");
        Ok(node)
    }

    pub fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn id(&self) -> String {
        self.endpoint.id().to_string()
    }

    pub async fn shutdown(&self) {
        self.endpoint.close().await;
        let _ = std::fs::remove_file(self.home.socket_path());
    }

    /// Our current address, for invites and joins.
    pub async fn addr(&self) -> EndpointAddr {
        if self.config.network.mode == NetworkMode::N0 {
            let _ = tokio::time::timeout(Duration::from_secs(10), self.endpoint.online()).await;
        }
        let mut addr = self.endpoint.addr();
        if self.config.network.mode == NetworkMode::Local || !addr.addrs.iter().any(|a| a.is_ip()) {
            for socket in self.endpoint.bound_sockets() {
                let ip = match socket.ip() {
                    IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
                    IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
                    ip => ip,
                };
                addr = addr.with_ip_addr(SocketAddr::new(ip, socket.port()));
            }
        }
        addr
    }

    /// The card we show to `peer`, listing only the handlers that peer may invoke.
    pub fn card_for(&self, peer_id: &str, peer_name: &str) -> PeerCard {
        let handlers = self
            .config
            .handlers
            .iter()
            .filter(|(_, h)| h.allows(peer_id, peer_name))
            .map(|(name, h)| HandlerInfo {
                name: name.clone(),
                description: h.description.clone(),
            })
            .collect();
        PeerCard {
            v: 1,
            id: self.id(),
            name: self.config.name.clone(),
            handlers,
        }
    }

    // Accepting side

    async fn accept_loop(self: Arc<Self>) {
        while let Some(incoming) = self.endpoint.accept().await {
            let node = self.clone();
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => node.serve_connection(conn).await,
                    Err(e) => debug!("incoming connection failed: {e}"),
                }
            });
        }
    }

    async fn serve_connection(self: Arc<Self>, conn: Connection) {
        let remote = conn.remote_id().to_string();
        {
            let store = self.store();
            if store.peer_by_id(&remote).ok().flatten().is_some() {
                let _ = store.touch_peer(&remote);
                // The peer is evidently reachable now, so retry anything queued for it.
                if store.reset_backoff(&remote).unwrap_or(0) > 0 {
                    self.outbox.notify_one();
                }
            }
        }
        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };
            let bytes = match recv.read_to_end(MAX_FRAME).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    debug!("reading frame from {remote}: {e}");
                    break;
                }
            };
            let reply = match serde_json::from_slice::<Frame>(&bytes) {
                Ok(frame) => self.handle_frame(&remote, frame),
                Err(e) => Ok(Reply::Rejected {
                    reason: format!("malformed frame: {e}"),
                }),
            };
            match reply {
                Ok(reply) => {
                    let body = serde_json::to_vec(&reply).expect("reply serializes");
                    if send.write_all(&body).await.is_err() || send.finish().is_err() {
                        break;
                    }
                }
                Err(e) => {
                    // Not acknowledging makes the sender retry later.
                    warn!("handling frame from {remote}: {e:#}");
                    let _ = send.reset(1u32.into());
                }
            }
        }
    }

    fn handle_frame(&self, remote: &str, frame: Frame) -> Result<Reply> {
        match frame {
            Frame::Deliver { envelope } => {
                let Some(peer) = self.store().peer_by_id(remote)? else {
                    return Ok(Reply::Rejected {
                        reason: "not paired with this endpoint".into(),
                    });
                };
                self.receive(&peer, envelope)
            }
            Frame::Join {
                invite_id,
                secret,
                card,
                addr,
            } => self.accept_join(remote, &invite_id, &secret, card, addr),
            Frame::GetCard => match self.store().peer_by_id(remote)? {
                Some(peer) => Ok(Reply::Card {
                    card: self.card_for(&peer.id, &peer.name),
                }),
                None => Ok(Reply::Rejected {
                    reason: "not paired with this endpoint".into(),
                }),
            },
        }
    }

    fn accept_join(
        &self,
        remote: &str,
        invite_id: &str,
        secret: &str,
        card: PeerCard,
        addr: Option<EndpointAddr>,
    ) -> Result<Reply> {
        if card.id != remote {
            return Ok(Reply::Rejected {
                reason: "card does not match the connection identity".into(),
            });
        }
        let addr = addr.filter(|a| a.id.to_string() == remote);
        let store = self.store();
        let name = match store.check_invite(invite_id, secret)? {
            InviteCheck::Valid { name } => name.unwrap_or_else(|| card.name.clone()),
            InviteCheck::Invalid(reason) => {
                return Ok(Reply::Rejected {
                    reason: reason.into(),
                });
            }
        };
        let name = store.upsert_peer(&card, &name, addr.as_ref())?;
        store.consume_invite(invite_id, remote)?;
        if let Some(addr) = addr {
            self.lookup.add_endpoint_info(addr);
        }
        info!(peer = %name, "peer joined via invite {invite_id}");
        Ok(Reply::Joined {
            card: self.card_for(remote, &name),
        })
    }

    fn receive(&self, peer: &Peer, envelope: Envelope) -> Result<Reply> {
        if envelope.v != ENVELOPE_VERSION {
            return Ok(Reply::Rejected {
                reason: format!("unsupported envelope version {}", envelope.v),
            });
        }
        let attachment_bytes: usize = envelope
            .payload
            .attachments()
            .iter()
            .map(|a| a.data.len())
            .sum();
        if attachment_bytes > MAX_ATTACHMENTS {
            return Ok(Reply::Rejected {
                reason: "attachments too large".into(),
            });
        }
        let id = envelope.id.clone();
        let store = self.store();
        match &envelope.payload {
            Payload::Message { attachments, .. } => {
                if store.insert_incoming(&peer.id, &envelope, false)? {
                    save_attachments(&self.home.files_dir(), &id, attachments)?;
                    info!(peer = %peer.name, "received message {id}");
                }
            }
            Payload::Request {
                handler,
                attachments,
                ..
            } => {
                if !store.insert_incoming(&peer.id, &envelope, true)? {
                    return Ok(Reply::Ack { id });
                }
                save_attachments(&self.home.files_dir(), &id, attachments)?;
                let allowed = self
                    .config
                    .handlers
                    .get(handler)
                    .filter(|h| h.allows(&peer.id, &peer.name));
                if allowed.is_some() {
                    store.insert_request(
                        &id,
                        "in",
                        &peer.id,
                        &envelope.thread_id,
                        handler,
                        RequestState::Submitted,
                        None,
                    )?;
                    info!(peer = %peer.name, handler = %handler, "received request {id}");
                    self.requests.notify_one();
                } else {
                    let reason = format!("handler {handler:?} is not available to you");
                    store.insert_request(
                        &id,
                        "in",
                        &peer.id,
                        &envelope.thread_id,
                        handler,
                        RequestState::Rejected,
                        Some(&reason),
                    )?;
                    store.enqueue(
                        &peer.id,
                        &response(&envelope, RequestState::Rejected, reason),
                    )?;
                    self.outbox.notify_one();
                    info!(peer = %peer.name, handler = %handler, "rejected request {id}");
                }
            }
            Payload::Status {
                request_id,
                state,
                note,
            } => {
                if store.insert_incoming(&peer.id, &envelope, true)?
                    && self.owns(&store, request_id, "out", &peer.id)?
                {
                    store.update_request(request_id, *state, note.as_deref(), None)?;
                }
            }
            Payload::Response {
                request_id,
                state,
                text,
                attachments,
            } => {
                if store.insert_incoming(&peer.id, &envelope, false)? {
                    save_attachments(&self.home.files_dir(), &id, attachments)?;
                    if self.owns(&store, request_id, "out", &peer.id)? {
                        let note = (*state != RequestState::Completed)
                            .then(|| text.lines().next().unwrap_or(""));
                        store.update_request(request_id, *state, note, Some(text))?;
                    }
                    info!(peer = %peer.name, "received response to {request_id}");
                }
            }
            Payload::Cancel { request_id } => {
                if store.insert_incoming(&peer.id, &envelope, true)?
                    && self.owns(&store, request_id, "in", &peer.id)?
                {
                    let running = self
                        .running
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(request_id);
                    if let Some(cancel) = running {
                        let _ = cancel.send(());
                    } else if store.update_request(
                        request_id,
                        RequestState::Canceled,
                        Some("canceled by peer"),
                        None,
                    )? {
                        let request = store
                            .message(request_id)?
                            .context("request message missing")?;
                        store.enqueue(
                            &peer.id,
                            &response(
                                &request.envelope,
                                RequestState::Canceled,
                                "Canceled.".into(),
                            ),
                        )?;
                        self.outbox.notify_one();
                    }
                }
            }
        }
        Ok(Reply::Ack { id })
    }

    /// Whether `request_id` exists with the given direction and belongs to `peer_id`, so a
    /// peer can only update or cancel its own requests.
    fn owns(
        &self,
        store: &Store,
        request_id: &str,
        direction: &str,
        peer_id: &str,
    ) -> Result<bool> {
        Ok(store
            .request(request_id)?
            .is_some_and(|r| r.direction == direction && r.peer_id == peer_id))
    }

    // Sending side

    async fn flush_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.outbox.notified() => {}
                _ = tokio::time::sleep(FLUSH_INTERVAL) => {}
            }
            if let Err(e) = self.deliver_pending(None).await {
                warn!("delivering outbox: {e:#}");
            }
        }
    }

    /// Attempts delivery of every due outgoing message, optionally only to one peer.
    pub async fn deliver_pending(self: &Arc<Self>, peer_id: Option<&str>) -> Result<()> {
        let _guard = self.deliver_lock.lock().await;
        let due = {
            let store = self.store();
            for expired in store.expire_outgoing()? {
                if expired.kind == "request" {
                    store.update_request(
                        &expired.id,
                        RequestState::Failed,
                        Some("not delivered before expiry"),
                        None,
                    )?;
                }
            }
            store.due_outgoing(peer_id)?
        };
        let mut by_peer: Vec<(String, Vec<MessageRow>)> = Vec::new();
        for row in due {
            match by_peer.iter_mut().find(|(p, _)| *p == row.peer_id) {
                Some((_, rows)) => rows.push(row),
                None => by_peer.push((row.peer_id.clone(), vec![row])),
            }
        }
        let mut tasks = tokio::task::JoinSet::new();
        for (peer_id, rows) in by_peer {
            let node = self.clone();
            tasks.spawn(async move { node.deliver_to(&peer_id, rows).await });
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result? {
                warn!("delivery: {e:#}");
            }
        }
        Ok(())
    }

    async fn deliver_to(&self, peer_id: &str, rows: Vec<MessageRow>) -> Result<()> {
        let conn = match self.connect(peer_id).await {
            Ok(conn) => conn,
            Err(e) => {
                debug!("peer {peer_id} unreachable: {e:#}");
                self.store().backoff_peer(peer_id, &format!("{e:#}"))?;
                return Ok(());
            }
        };
        for row in rows {
            let frame = Frame::Deliver {
                envelope: row.envelope.clone(),
            };
            match call(&conn, &frame).await {
                Ok(Reply::Ack { .. }) => {
                    self.store().mark_delivered(&row.id)?;
                    debug!("delivered {} to {peer_id}", row.id);
                }
                Ok(Reply::Rejected { reason }) => {
                    let store = self.store();
                    store.mark_failed(&row.id, "failed", &reason)?;
                    if row.kind == "request" {
                        store.update_request(
                            &row.id,
                            RequestState::Rejected,
                            Some(&reason),
                            None,
                        )?;
                    }
                    warn!("{} rejected by {peer_id}: {reason}", row.id);
                }
                Ok(other) => {
                    self.store()
                        .backoff_peer(peer_id, &format!("unexpected reply {other:?}"))?;
                    break;
                }
                Err(e) => {
                    self.store().backoff_peer(peer_id, &format!("{e:#}"))?;
                    break;
                }
            }
        }
        self.store().touch_peer(peer_id)?;
        conn.close(0u32.into(), b"done");
        Ok(())
    }

    async fn connect(&self, peer_id: &str) -> Result<Connection> {
        let id: EndpointId = peer_id
            .parse()
            .with_context(|| format!("invalid endpoint id {peer_id}"))?;
        self.connect_addr(EndpointAddr::new(id)).await
    }

    async fn connect_addr(&self, addr: EndpointAddr) -> Result<Connection> {
        tokio::time::timeout(CONNECT_TIMEOUT, self.endpoint.connect(addr, ALPN))
            .await
            .context("connection timed out")?
            .map_err(|e| anyhow!("{e}"))
    }

    /// Redeems an invite ticket and stores the inviter as a peer.
    pub async fn join(&self, ticket: &Ticket, alias: Option<&str>) -> Result<(String, PeerCard)> {
        if ticket.addr.id == self.endpoint.id() {
            bail!("this invite was created by this endpoint");
        }
        if ticket.expires_at < now_ms() {
            bail!("invite expired");
        }
        let inviter = ticket.addr.id.to_string();
        let name = alias.unwrap_or(&ticket.name);
        let frame = Frame::Join {
            invite_id: ticket.invite_id.clone(),
            secret: ticket.secret.clone(),
            card: self.card_for(&inviter, name),
            addr: Some(self.addr().await),
        };
        let conn = self
            .connect_addr(ticket.addr.clone())
            .await
            .context("connecting to inviter")?;
        let reply = call(&conn, &frame).await;
        conn.close(0u32.into(), b"done");
        match reply? {
            Reply::Joined { card } if card.id == inviter => {
                let name = self.store().upsert_peer(
                    &card,
                    alias.unwrap_or(&card.name),
                    Some(&ticket.addr),
                )?;
                self.lookup.add_endpoint_info(ticket.addr.clone());
                self.store().touch_peer(&inviter)?;
                Ok((name, card))
            }
            Reply::Rejected { reason } => bail!("invite rejected: {reason}"),
            other => bail!("unexpected reply {other:?}"),
        }
    }

    /// Fetches and stores the current card of a paired peer.
    pub async fn fetch_card(&self, peer_id: &str) -> Result<PeerCard> {
        let conn = self.connect(peer_id).await?;
        let reply = call(&conn, &Frame::GetCard).await;
        conn.close(0u32.into(), b"done");
        match reply? {
            Reply::Card { card } if card.id == peer_id => {
                let store = self.store();
                store.update_peer_card(&card)?;
                store.touch_peer(peer_id)?;
                Ok(card)
            }
            Reply::Rejected { reason } => bail!("{reason}"),
            other => bail!("unexpected reply {other:?}"),
        }
    }

    /// Contacts every peer once. Refreshes their cards and, because an inbound connection
    /// resets their retry backoff towards us, prompts them to deliver anything queued for us.
    pub fn greet_peers(self: &Arc<Self>) {
        let peers = self.store().list_peers().unwrap_or_default();
        for peer in peers {
            let node = self.clone();
            tokio::spawn(async move {
                if let Err(e) = node.fetch_card(&peer.id).await {
                    debug!(peer = %peer.name, "greeting failed: {e:#}");
                }
            });
        }
    }
}

/// Binds the iroh endpoint. Without a configured port, the node reuses the port from its
/// previous run, so the direct addresses peers learned earlier stay valid. That matters most
/// in `local` mode, where no relay or address lookup can tell peers about a new port.
async fn bind(home: &Home, config: &Config, lookup: &MemoryLookup) -> Result<Endpoint> {
    let port_file = home.dir.join("port");
    let remembered = std::fs::read_to_string(&port_file)
        .ok()
        .and_then(|p| p.trim().parse::<u16>().ok());
    let preferred = config.network.port.or(remembered);
    let endpoint = match build(home, config, lookup, preferred).await {
        Ok(endpoint) => endpoint,
        // A remembered port may have been taken by something else since; pick a new one.
        Err(e) if config.network.port.is_none() && preferred.is_some() => {
            debug!("port {preferred:?} unavailable ({e:#}); binding a random port");
            build(home, config, lookup, None).await?
        }
        Err(e) => return Err(e),
    };
    if config.network.port.is_none()
        && let Some(socket) = endpoint.bound_sockets().first()
    {
        let _ = std::fs::write(&port_file, socket.port().to_string());
    }
    Ok(endpoint)
}

async fn build(
    home: &Home,
    config: &Config,
    lookup: &MemoryLookup,
    port: Option<u16>,
) -> Result<Endpoint> {
    let mut builder = match config.network.mode {
        NetworkMode::N0 => Endpoint::builder(presets::N0),
        NetworkMode::Local => Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled),
    };
    if !config.network.relays.is_empty() {
        let urls = config
            .network
            .relays
            .iter()
            .map(|r| r.parse())
            .collect::<Result<Vec<_>, _>>()?;
        builder = builder.relay_mode(RelayMode::custom(urls));
    }
    if let Some(port) = port {
        builder = builder
            .bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))
            .map_err(|e| anyhow!("invalid bind address: {e}"))?;
    }
    builder
        .secret_key(home.load_secret_key()?)
        .alpns(vec![ALPN.to_vec()])
        .address_lookup(lookup.clone())
        .bind()
        .await
        .map_err(|e| anyhow!("binding iroh endpoint: {e}"))
}

/// Sends one frame on a fresh bidirectional stream and reads the reply.
async fn call(conn: &Connection, frame: &Frame) -> Result<Reply> {
    let exchange = async {
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&serde_json::to_vec(frame)?).await?;
        send.finish()?;
        let bytes = recv.read_to_end(MAX_FRAME).await?;
        anyhow::Ok(serde_json::from_slice(&bytes)?)
    };
    tokio::time::timeout(CALL_TIMEOUT, exchange)
        .await
        .context("peer did not reply in time")?
}

/// Builds the response envelope for a request.
pub fn response(request: &Envelope, state: RequestState, text: String) -> Envelope {
    Envelope {
        v: ENVELOPE_VERSION,
        id: new_id("msg"),
        thread_id: request.thread_id.clone(),
        reply_to: Some(request.id.clone()),
        created_at: now_ms(),
        payload: Payload::Response {
            request_id: request.id.clone(),
            state,
            text,
            attachments: vec![],
        },
    }
}
