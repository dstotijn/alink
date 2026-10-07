//! alink gives AI agents a persistent identity, an inbox, and a tiny protocol for
//! asynchronous collaboration over iroh. If both agents can use a shell, they can use alink.

mod config;
mod control;
mod handler;
mod node;
mod proto;
mod store;
mod ticket;
mod util;

use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use crate::config::{Config, Home, initial_config_text};
use crate::node::Node;
use crate::proto::{
    Attachment, ENVELOPE_VERSION, Envelope, MAX_ATTACHMENTS, Payload, PeerCard, RequestState,
};
use crate::store::{MessageFilter, MessageRow, Peer, RequestRow, Store};
use crate::ticket::Ticket;
use crate::util::{attachment_paths, format_time, guess_media_type, new_id, now_ms, short_id};

/// Exit code for `wait` timeouts, matching coreutils `timeout`.
const EXIT_TIMEOUT: u8 = 124;

const SKILL: &str = include_str!("../skills/alink/SKILL.md");

#[derive(Parser)]
#[command(
    name = "alink",
    version,
    about = "Encrypted, asynchronous agent-to-agent messaging over iroh"
)]
struct Cli {
    /// Print machine-readable JSON.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create this endpoint's identity and config.
    Init {
        /// Name advertised to peers (default: user and host name).
        #[arg(long)]
        name: Option<String>,
    },
    /// Show this endpoint's identity.
    Whoami,
    /// Run the node: receive messages, deliver the outbox, run handlers.
    Serve,
    /// Create a one-time invite for another endpoint.
    Invite {
        /// Local name to give the peer who joins.
        #[arg(long)]
        name: Option<String>,
        /// How long the invite stays valid.
        #[arg(long, default_value = "24h")]
        ttl: humantime::Duration,
        /// Print the invite and return instead of waiting for the peer to join.
        #[arg(long)]
        no_wait: bool,
    },
    /// Redeem an invite and pair with the endpoint that created it.
    Join {
        ticket: String,
        /// Local name for the inviting peer.
        #[arg(long)]
        name: Option<String>,
        /// Do not ask for confirmation.
        #[arg(long, short)]
        yes: bool,
    },
    /// List and manage paired peers.
    Peers {
        #[command(subcommand)]
        command: Option<PeersCommand>,
    },
    /// Send a message to a peer.
    Send {
        peer: String,
        text: Option<String>,
        #[command(flatten)]
        body: BodyArgs,
        /// Continue an existing thread (default: start a new one).
        #[arg(long)]
        thread: Option<String>,
    },
    /// Reply to a received message, in the same thread.
    Reply {
        message_id: String,
        text: Option<String>,
        #[command(flatten)]
        body: BodyArgs,
    },
    /// Ask a peer to run one of its handlers.
    Run {
        peer: String,
        handler: String,
        prompt: Option<String>,
        #[command(flatten)]
        body: BodyArgs,
        #[arg(long)]
        thread: Option<String>,
        /// Wait for the response.
        #[arg(long)]
        wait: bool,
        /// How long to wait with --wait.
        #[arg(long, default_value = "30m")]
        timeout: humantime::Duration,
    },
    /// Wait for a request to finish, or for unread messages (in a thread or anywhere).
    Wait {
        /// A request ID (req_...) or thread ID. Omit to wait for any unread message.
        id: Option<String>,
        #[arg(long, default_value = "10m")]
        timeout: humantime::Duration,
    },
    /// Report new unread messages as they arrive, without marking them as read.
    ///
    /// Watches every thread, prints one line per new batch and, with --notify, runs a
    /// command for each batch. Runs until stopped and keeps a node online meanwhile.
    Listen {
        /// Shell command to run for each new batch. It gets ALINK_COUNT, ALINK_FROM,
        /// ALINK_THREADS and ALINK_MESSAGE_IDS, never the message text. A failing command is
        /// retried.
        #[arg(long, value_name = "COMMAND")]
        notify: Option<String>,
    },
    /// Show received messages and responses. Shown messages are marked as read.
    Inbox {
        /// Include messages that were already read.
        #[arg(long)]
        all: bool,
        /// Do not mark shown messages as read.
        #[arg(long)]
        peek: bool,
        #[arg(long)]
        thread: Option<String>,
        #[arg(long)]
        peer: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// List requests and their states.
    Requests {
        #[arg(long, conflicts_with = "outgoing")]
        incoming: bool,
        #[arg(long)]
        outgoing: bool,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Ask a peer to stop working on a request you sent.
    Cancel { request_id: String },
    /// Show outgoing messages that are not yet delivered.
    Outbox {
        /// Include delivered messages.
        #[arg(long)]
        all: bool,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Print the agent skill (SKILL.md) that teaches agents to use alink.
    Skill,
}

#[derive(Subcommand)]
enum PeersCommand {
    /// Show a peer's card, including the handlers it offers you.
    Show {
        peer: String,
        /// Fetch the current card from the peer first.
        #[arg(long)]
        refresh: bool,
    },
    Rename {
        peer: String,
        new_name: String,
    },
    Remove {
        peer: String,
    },
}

#[derive(clap::Args)]
struct BodyArgs {
    /// Read the text from stdin. With a text argument, stdin is attached as a file instead.
    #[arg(long)]
    stdin: bool,
    /// Attach a file (repeatable).
    #[arg(long = "attach", value_name = "FILE")]
    attach: Vec<PathBuf>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let serving = matches!(cli.command, Command::Serve);
    let default_filter = if serving { "alink=info" } else { "alink=warn" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    match runtime.block_on(run(cli)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let home = Home::resolve()?;
    let json = cli.json;
    match cli.command {
        Command::Init { name } => init(&home, name, json),
        Command::Skill => {
            print!("{SKILL}");
            Ok(ExitCode::SUCCESS)
        }
        command => {
            home.ensure_initialized()?;
            let config = home.load_config()?;
            let ctx = Ctx { home, config, json };
            match command {
                Command::Whoami => ctx.whoami(),
                Command::Serve => ctx.serve().await,
                Command::Invite { name, ttl, no_wait } => {
                    ctx.invite(name, ttl.into(), no_wait).await
                }
                Command::Join { ticket, name, yes } => ctx.join(&ticket, name, yes).await,
                Command::Peers { command } => ctx.peers(command).await,
                Command::Send {
                    peer,
                    text,
                    body,
                    thread,
                } => ctx.send(&peer, text, body, thread).await,
                Command::Reply {
                    message_id,
                    text,
                    body,
                } => ctx.reply(&message_id, text, body).await,
                Command::Run {
                    peer,
                    handler,
                    prompt,
                    body,
                    thread,
                    wait,
                    timeout,
                } => {
                    ctx.run_request(&peer, &handler, prompt, body, thread, wait, timeout.into())
                        .await
                }
                Command::Wait { id, timeout } => ctx.wait(id.as_deref(), timeout.into()).await,
                Command::Listen { notify } => ctx.listen(notify.as_deref()).await,
                Command::Inbox {
                    all,
                    peek,
                    thread,
                    peer,
                    limit,
                } => ctx.inbox(all, peek, thread.as_deref(), peer.as_deref(), limit),
                Command::Requests {
                    incoming,
                    outgoing,
                    limit,
                } => {
                    let direction = if incoming {
                        Some("in")
                    } else if outgoing {
                        Some("out")
                    } else {
                        None
                    };
                    ctx.requests(direction, limit)
                }
                Command::Cancel { request_id } => ctx.cancel(&request_id).await,
                Command::Outbox { all, limit } => ctx.outbox(all, limit),
                Command::Init { .. } | Command::Skill => unreachable!(),
            }
        }
    }
}

fn init(home: &Home, name: Option<String>, json: bool) -> Result<ExitCode> {
    if home.is_initialized() {
        bail!("alink is already initialized in {}", home.dir.display());
    }
    std::fs::create_dir_all(&home.dir)
        .with_context(|| format!("creating {}", home.dir.display()))?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&home.dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let key = iroh::SecretKey::generate();
    home.write_secret_key(&key)?;
    let name = name.unwrap_or_else(default_name);
    if !home.config_path().exists() {
        std::fs::write(home.config_path(), initial_config_text(&name))?;
    }
    Store::open(&home.db_path())?;
    let id = key.public().to_string();
    if json {
        print_json(&json!({ "id": id, "name": name, "home": home.dir }));
    } else {
        println!("Created identity {name}");
        println!("  id:     {id}");
        println!("  config: {}", home.config_path().display());
        println!("\nNext: `alink invite` to pair with another endpoint, or `alink join <invite>`.");
    }
    Ok(ExitCode::SUCCESS)
}

fn default_name() -> String {
    let user = std::env::var("USER").unwrap_or_else(|_| "agent".into());
    let host = std::process::Command::new("hostname")
        .arg("-s")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty());
    match host {
        Some(host) => store::sanitize_name(&format!("{user}-{host}")),
        None => store::sanitize_name(&user),
    }
}

/// Network access for one CLI command: either the node that is already running (reached
/// over its control socket) or a temporary node owned by this process.
enum Access {
    Remote(PathBuf),
    Local(Arc<Node>),
}

impl Access {
    async fn open(home: &Home, config: &Config) -> Result<Self> {
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(home.lock_path())?;
        match lock.try_lock() {
            Ok(()) => Ok(Access::Local(
                Node::start(home.clone(), config.clone(), lock).await?,
            )),
            Err(std::fs::TryLockError::WouldBlock) => Ok(Access::Remote(home.socket_path())),
            Err(std::fs::TryLockError::Error(e)) => Err(e).context("locking node"),
        }
    }

    fn is_local(&self) -> bool {
        matches!(self, Access::Local(_))
    }

    async fn flush(&self, peer_id: Option<&str>) -> Result<()> {
        match self {
            Access::Local(node) => node.deliver_pending(peer_id).await,
            Access::Remote(socket) => {
                let request = control::Request::Flush {
                    peer_id: peer_id.map(Into::into),
                };
                control::call::<Value>(socket, &request).await.map(|_| ())
            }
        }
    }

    async fn join(&self, ticket: &str, alias: Option<String>) -> Result<(String, PeerCard)> {
        match self {
            Access::Local(node) => node.join(&Ticket::decode(ticket)?, alias.as_deref()).await,
            Access::Remote(socket) => {
                let request = control::Request::Join {
                    ticket: ticket.into(),
                    alias,
                };
                let joined: control::Joined = control::call(socket, &request).await?;
                Ok((joined.name, joined.card))
            }
        }
    }

    async fn fetch_card(&self, peer_id: &str) -> Result<PeerCard> {
        match self {
            Access::Local(node) => node.fetch_card(peer_id).await,
            Access::Remote(socket) => {
                control::call(
                    socket,
                    &control::Request::FetchCard {
                        peer_id: peer_id.into(),
                    },
                )
                .await
            }
        }
    }

    async fn addr(&self) -> Result<iroh::EndpointAddr> {
        match self {
            Access::Local(node) => Ok(node.addr().await),
            Access::Remote(socket) => control::call(socket, &control::Request::Addr).await,
        }
    }

    async fn close(self) {
        if let Access::Local(node) = self {
            node.shutdown().await;
        }
    }
}

struct Ctx {
    home: Home,
    config: Config,
    json: bool,
}

impl Ctx {
    fn store(&self) -> Result<Store> {
        Store::open(&self.home.db_path())
    }

    fn whoami(&self) -> Result<ExitCode> {
        let id = self.home.load_secret_key()?.public().to_string();
        let handlers: Vec<&String> = self.config.handlers.keys().collect();
        if self.json {
            print_json(&json!({
                "id": id,
                "name": self.config.name,
                "home": self.home.dir,
                "handlers": handlers,
                "node_running": node_running(&self.home),
            }));
        } else {
            println!("{} ({})", self.config.name, fingerprint(&id));
            println!("  id:       {id}");
            println!("  home:     {}", self.home.dir.display());
            println!(
                "  handlers: {}",
                if handlers.is_empty() {
                    "none".into()
                } else {
                    join(&handlers)
                }
            );
            println!(
                "  node:     {}",
                if node_running(&self.home) {
                    "running"
                } else {
                    "not running"
                }
            );
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn serve(&self) -> Result<ExitCode> {
        let access = Access::open(&self.home, &self.config).await?;
        let Access::Local(node) = access else {
            bail!(
                "another alink node is already running for {}",
                self.home.dir.display()
            );
        };
        eprintln!("alink serving as {} ({})", self.config.name, node.id());
        for (name, handler) in &self.config.handlers {
            eprintln!(
                "  handler {name}: {} (peers: {})",
                handler.command.join(" "),
                join(&handler.peers)
            );
        }
        node.greet_peers();
        tokio::spawn(handler::run_requests(node.clone()));
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        eprintln!("shutting down");
        node.shutdown().await;
        Ok(ExitCode::SUCCESS)
    }

    async fn invite(&self, name: Option<String>, ttl: Duration, no_wait: bool) -> Result<ExitCode> {
        let access = Access::open(&self.home, &self.config).await?;
        let addr = access.addr().await?;
        let store = self.store()?;
        let (invite_id, secret) = store.create_invite(name.as_deref(), ttl)?;
        let expires_at = now_ms() + ttl.as_millis() as u64;
        let ticket = Ticket {
            v: 1,
            name: self.config.name.clone(),
            addr,
            invite_id: invite_id.clone(),
            secret,
            expires_at,
        }
        .encode()?;

        if self.json {
            print_json(
                &json!({ "invite_id": invite_id, "invite": ticket, "expires_at": format_time(expires_at) }),
            );
        } else {
            println!("Share this invite with the other endpoint over a channel you trust:\n");
            println!("  {ticket}\n");
            println!(
                "It can be used once and expires {}.",
                format_time(expires_at)
            );
        }
        if no_wait {
            if access.is_local() && !self.json {
                println!(
                    "The invite can only be redeemed while a node is running (`alink serve`)."
                );
            }
            access.close().await;
            return Ok(ExitCode::SUCCESS);
        }
        if !self.json {
            println!("\nWaiting for the peer to join (Ctrl-C to stop waiting)...");
        }
        let joined = loop {
            if let Some(peer_id) = store.invite_used_by(&invite_id)? {
                break Some(peer_id);
            }
            if now_ms() > expires_at {
                break None;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                _ = tokio::signal::ctrl_c() => break None,
            }
        };
        access.close().await;
        let Some(peer_id) = joined else {
            if !self.json {
                println!("Stopped waiting. The invite stays valid while a node is running.");
            }
            return Ok(ExitCode::SUCCESS);
        };
        let peer = store.peer_by_id(&peer_id)?.context("joined peer missing")?;
        if self.json {
            print_json(&json!({ "joined": peer_json(&peer) }));
        } else {
            println!("\n✓ {} joined ({})", peer.name, fingerprint(&peer.id));
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn join(&self, ticket_text: &str, alias: Option<String>, yes: bool) -> Result<ExitCode> {
        let ticket = Ticket::decode(ticket_text)?;
        let inviter = ticket.addr.id.to_string();
        if !yes && !self.json && std::io::stdin().is_terminal() {
            eprint!("Join {} ({})? [y/N] ", ticket.name, fingerprint(&inviter));
            std::io::stderr().flush()?;
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            if !matches!(answer.trim(), "y" | "Y" | "yes") {
                eprintln!("Aborted.");
                return Ok(ExitCode::FAILURE);
            }
        }
        let access = Access::open(&self.home, &self.config).await?;
        let result = access.join(ticket_text, alias).await;
        access.close().await;
        let (name, card) = result?;
        if self.json {
            print_json(&json!({ "name": name, "id": card.id, "card": card }));
        } else {
            println!("✓ Paired with {name} ({})", fingerprint(&card.id));
            print_handlers(&card);
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn peers(&self, command: Option<PeersCommand>) -> Result<ExitCode> {
        let store = self.store()?;
        match command {
            None => {
                let peers = store.list_peers()?;
                if self.json {
                    print_json(&Value::Array(peers.iter().map(peer_json).collect()));
                } else if peers.is_empty() {
                    println!("No peers yet. Pair with `alink invite` / `alink join`.");
                } else {
                    println!(
                        "{:<20} {:<20} {:<20} HANDLERS",
                        "NAME", "FINGERPRINT", "LAST SEEN"
                    );
                    for peer in peers {
                        let handlers: Vec<&String> =
                            peer.card.handlers.iter().map(|h| &h.name).collect();
                        println!(
                            "{:<20} {:<20} {:<20} {}",
                            peer.name,
                            fingerprint(&peer.id),
                            peer.last_seen_at
                                .map(format_time)
                                .unwrap_or_else(|| "never".into()),
                            join(&handlers)
                        );
                    }
                }
            }
            Some(PeersCommand::Show { peer, refresh }) => {
                let mut peer = store.resolve_peer(&peer)?;
                if refresh {
                    let access = Access::open(&self.home, &self.config).await?;
                    let card = access.fetch_card(&peer.id).await;
                    access.close().await;
                    peer.card = card?;
                }
                if self.json {
                    print_json(&peer_json(&peer));
                } else {
                    println!("{} ({})", peer.name, fingerprint(&peer.id));
                    println!("  id:        {}", peer.id);
                    println!("  calls itself: {}", peer.card.name);
                    println!(
                        "  last seen: {}",
                        peer.last_seen_at
                            .map(format_time)
                            .unwrap_or_else(|| "never".into())
                    );
                    print_handlers(&peer.card);
                }
            }
            Some(PeersCommand::Rename { peer, new_name }) => {
                let peer = store.resolve_peer(&peer)?;
                store.rename_peer(&peer.id, &new_name)?;
                let renamed = store.peer_by_id(&peer.id)?.context("peer missing")?;
                self.done(
                    json!({ "id": peer.id, "name": renamed.name }),
                    &format!("Renamed {} to {}", peer.name, renamed.name),
                );
            }
            Some(PeersCommand::Remove { peer }) => {
                let peer = store.resolve_peer(&peer)?;
                store.remove_peer(&peer.id)?;
                self.done(
                    json!({ "id": peer.id, "removed": true }),
                    &format!("Removed {}", peer.name),
                );
            }
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn send(
        &self,
        peer: &str,
        text: Option<String>,
        body: BodyArgs,
        thread: Option<String>,
    ) -> Result<ExitCode> {
        let peer = self.store()?.resolve_peer(peer)?;
        let (text, attachments) = read_body(text, &body)?;
        let envelope = new_envelope(thread, None, Payload::Message { text, attachments });
        self.store()?.enqueue(&peer.id, &envelope)?;
        let delivery = self.deliver(&peer, &envelope.id).await?;
        self.report_sent(&envelope, &peer, &delivery, None);
        Ok(ExitCode::SUCCESS)
    }

    async fn reply(
        &self,
        message_id: &str,
        text: Option<String>,
        body: BodyArgs,
    ) -> Result<ExitCode> {
        let store = self.store()?;
        let original = store
            .message(message_id)?
            .with_context(|| format!("unknown message {message_id}"))?;
        if original.direction != "in" {
            bail!("{message_id} is a message you sent; reply to messages you received");
        }
        let peer = store
            .peer_by_id(&original.peer_id)?
            .context("the sender is no longer a peer")?;
        let (text, attachments) = read_body(text, &body)?;
        let envelope = new_envelope(
            Some(original.thread_id.clone()),
            Some(original.id.clone()),
            Payload::Message { text, attachments },
        );
        store.enqueue(&peer.id, &envelope)?;
        let delivery = self.deliver(&peer, &envelope.id).await?;
        self.report_sent(&envelope, &peer, &delivery, None);
        Ok(ExitCode::SUCCESS)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_request(
        &self,
        peer: &str,
        handler: &str,
        prompt: Option<String>,
        body: BodyArgs,
        thread: Option<String>,
        wait: bool,
        timeout: Duration,
    ) -> Result<ExitCode> {
        let store = self.store()?;
        let peer = store.resolve_peer(peer)?;
        if !peer.card.handlers.iter().any(|h| h.name == handler) {
            eprintln!(
                "warning: {} did not advertise a handler named {handler:?} (last known: {}); sending anyway",
                peer.name,
                join(
                    &peer
                        .card
                        .handlers
                        .iter()
                        .map(|h| &h.name)
                        .collect::<Vec<_>>()
                )
            );
        }
        let (prompt, attachments) = read_body(prompt, &body)?;
        let mut envelope = new_envelope(
            thread,
            None,
            Payload::Request {
                handler: handler.into(),
                prompt,
                attachments,
            },
        );
        envelope.id = new_id("req");
        store.enqueue(&peer.id, &envelope)?;
        store.insert_request(
            &envelope.id,
            "out",
            &peer.id,
            &envelope.thread_id,
            handler,
            RequestState::Submitted,
            None,
        )?;
        drop(store);

        if wait {
            let access = Access::open(&self.home, &self.config).await?;
            access.flush(Some(&peer.id)).await?;
            if !self.json {
                eprintln!(
                    "Sent request {} to {}; waiting for the response...",
                    envelope.id, peer.name
                );
            }
            let code = self.wait_with(&access, Some(&envelope.id), timeout).await;
            access.close().await;
            return code;
        }
        let delivery = self.deliver(&peer, &envelope.id).await?;
        self.report_sent(&envelope, &peer, &delivery, Some(handler));
        Ok(ExitCode::SUCCESS)
    }

    async fn wait(&self, id: Option<&str>, timeout: Duration) -> Result<ExitCode> {
        let access = Access::open(&self.home, &self.config).await?;
        if let Access::Local(node) = &access {
            node.greet_peers();
        }
        let code = self.wait_with(&access, id, timeout).await;
        access.close().await;
        code
    }

    async fn wait_with(
        &self,
        access: &Access,
        id: Option<&str>,
        timeout: Duration,
    ) -> Result<ExitCode> {
        let deadline = Instant::now() + timeout;
        let store = self.store()?;
        let request_id = id.filter(|id| id.starts_with("req_"));
        if let Some(request_id) = request_id {
            store
                .request(request_id)?
                .with_context(|| format!("unknown request {request_id}"))?;
        }
        loop {
            if let Some(request_id) = request_id {
                let request = store.request(request_id)?.context("request disappeared")?;
                if request.state()?.is_terminal() {
                    return self.print_request_result(&store, &request);
                }
            } else {
                let filter = MessageFilter {
                    unread_only: true,
                    thread_id: id,
                    limit: Some(50),
                    ..Default::default()
                };
                let messages = store.list_incoming(&filter)?;
                if !messages.is_empty() {
                    self.print_messages(&store, &messages)?;
                    store.mark_read(&messages.iter().map(|m| m.id.clone()).collect::<Vec<_>>())?;
                    return Ok(ExitCode::SUCCESS);
                }
            }
            if Instant::now() >= deadline {
                // A scoped wait can miss messages elsewhere; say so, so the caller checks.
                let other_unread = match (id, request_id) {
                    (Some(_), Some(_)) => store.count_unread(None)?,
                    (Some(thread_id), None) => store.count_unread(Some(thread_id))?,
                    (None, _) => 0,
                };
                if self.json {
                    print_json(
                        &json!({ "status": "timeout", "id": id, "other_unread": other_unread }),
                    );
                } else {
                    eprintln!("Timed out after {}.", humantime::format_duration(timeout));
                    if other_unread > 0 {
                        eprintln!("{other_unread} unread message(s) elsewhere; see `alink inbox`.");
                    }
                    if request_id.is_some() && access.is_local() {
                        eprintln!(
                            "Responses only arrive while a node runs (`alink serve` or `alink wait`)."
                        );
                    }
                }
                return Ok(ExitCode::from(EXIT_TIMEOUT));
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    async fn listen(&self, notify: Option<&str>) -> Result<ExitCode> {
        let access = Access::open(&self.home, &self.config).await?;
        if let Access::Local(node) = &access {
            node.greet_peers();
        }
        let result = self.listen_with(notify).await;
        access.close().await;
        result
    }

    async fn listen_with(&self, notify: Option<&str>) -> Result<ExitCode> {
        let store = self.store()?;
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        if !self.json {
            eprintln!("Listening for alink messages (Ctrl-C to stop)...");
        }
        // Unread messages from before we started count as the first batch.
        let mut cursor = 0;
        let mut announced = 0;
        let mut retry_at: Option<Instant> = None;
        loop {
            let filter = MessageFilter {
                unread_only: true,
                after_rowid: Some(cursor),
                ..Default::default()
            };
            let batch = store.list_incoming(&filter)?;
            if !batch.is_empty() && retry_at.is_none_or(|at| Instant::now() >= at) {
                let mut from: Vec<String> = Vec::new();
                let mut threads: Vec<String> = Vec::new();
                for m in &batch {
                    let name = peer_name(&store, &m.peer_id);
                    if !from.contains(&name) {
                        from.push(name);
                    }
                    if !threads.contains(&m.thread_id) {
                        threads.push(m.thread_id.clone());
                    }
                }
                let ids: Vec<&str> = batch.iter().map(|m| m.id.as_str()).collect();
                let last = batch.last().map(|m| m.rowid).unwrap_or(cursor);
                if last <= announced {
                    // A notify retry for a batch that was already reported.
                } else if self.json {
                    println!(
                        "{}",
                        json!({ "event": "messages", "count": batch.len(), "from": from, "threads": threads, "message_ids": ids })
                    );
                } else {
                    println!(
                        "{} new message(s) from {}; read them with `alink inbox`",
                        batch.len(),
                        from.join(", ")
                    );
                }
                std::io::stdout().flush()?;
                announced = last;

                let delivered = match notify {
                    None => true,
                    Some(command) => {
                        let status = tokio::process::Command::new("sh")
                            .arg("-c")
                            .arg(command)
                            .env("ALINK_COUNT", batch.len().to_string())
                            .env("ALINK_FROM", from.join(","))
                            .env("ALINK_THREADS", threads.join(","))
                            .env("ALINK_MESSAGE_IDS", ids.join(","))
                            .stdin(std::process::Stdio::null())
                            .stdout(std::process::Stdio::null())
                            .status()
                            .await;
                        match status {
                            Ok(status) if status.success() => true,
                            Ok(status) => {
                                eprintln!("notify command exited with {status}; retrying in 5s");
                                false
                            }
                            Err(e) => {
                                eprintln!("notify command failed to start: {e}; retrying in 5s");
                                false
                            }
                        }
                    }
                };
                if delivered {
                    cursor = last;
                    retry_at = None;
                } else {
                    retry_at = Some(Instant::now() + Duration::from_secs(5));
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                _ = tokio::signal::ctrl_c() => return Ok(ExitCode::SUCCESS),
                _ = term.recv() => return Ok(ExitCode::SUCCESS),
            }
        }
    }

    fn print_request_result(&self, store: &Store, request: &RequestRow) -> Result<ExitCode> {
        let state = request.state()?;
        let response = store.response_message(&request.id)?;
        if let Some(response) = &response {
            store.mark_read(std::slice::from_ref(&response.id))?;
        }
        let attachments = response
            .as_ref()
            .map(|r| self.attachments_json(r))
            .unwrap_or_default();
        if self.json {
            let mut value = request_json(store, request);
            value["response"] = json!(request.response);
            value["attachments"] = Value::Array(attachments);
            print_json(&value);
        } else {
            eprintln!("Request {} {}.", request.id, state.as_str());
            if let Some(text) = &request.response {
                println!("{}", text.trim_end());
            } else if let Some(note) = &request.note {
                println!("{note}");
            }
            for a in attachments {
                println!("attachment: {}", a["path"].as_str().unwrap_or_default());
            }
        }
        Ok(if state == RequestState::Completed {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        })
    }

    fn inbox(
        &self,
        all: bool,
        peek: bool,
        thread: Option<&str>,
        peer: Option<&str>,
        limit: usize,
    ) -> Result<ExitCode> {
        let store = self.store()?;
        let peer_id = peer
            .map(|p| store.resolve_peer(p))
            .transpose()?
            .map(|p| p.id);
        let filter = MessageFilter {
            unread_only: !all,
            thread_id: thread,
            peer_id: peer_id.as_deref(),
            limit: Some(limit),
            ..Default::default()
        };
        let messages = store.list_incoming(&filter)?;
        if messages.is_empty() && !self.json {
            println!("No {}messages.", if all { "" } else { "unread " });
        } else {
            self.print_messages(&store, &messages)?;
        }
        if !peek {
            store.mark_read(&messages.iter().map(|m| m.id.clone()).collect::<Vec<_>>())?;
        }
        Ok(ExitCode::SUCCESS)
    }

    fn print_messages(&self, store: &Store, messages: &[MessageRow]) -> Result<()> {
        let values: Vec<Value> = messages
            .iter()
            .map(|m| self.message_json(store, m))
            .collect();
        if self.json {
            print_json(&Value::Array(values));
            return Ok(());
        }
        for (i, value) in values.iter().enumerate() {
            if i > 0 {
                println!();
            }
            println!(
                "{} from {} · thread {} · {}",
                value["id"].as_str().unwrap_or_default(),
                value["from"].as_str().unwrap_or_default(),
                value["thread_id"].as_str().unwrap_or_default(),
                value["created_at"].as_str().unwrap_or_default(),
            );
            if let Some(request_id) = value["request_id"].as_str() {
                println!(
                    "  response to {request_id} ({})",
                    value["state"].as_str().unwrap_or_default()
                );
            } else if let Some(reply_to) = value["reply_to"].as_str() {
                println!("  in reply to {reply_to}");
            }
            for line in value["text"].as_str().unwrap_or_default().lines() {
                println!("  {line}");
            }
            for a in value["attachments"].as_array().into_iter().flatten() {
                println!("  attachment: {}", a["path"].as_str().unwrap_or_default());
            }
        }
        Ok(())
    }

    fn message_json(&self, store: &Store, message: &MessageRow) -> Value {
        let from = store
            .peer_by_id(&message.peer_id)
            .ok()
            .flatten()
            .map(|p| p.name)
            .unwrap_or_else(|| message.peer_id.clone());
        let state = match &message.envelope.payload {
            Payload::Response { state, .. } => Some(state.as_str()),
            _ => None,
        };
        json!({
            "id": message.id,
            "kind": message.kind,
            "from": from,
            "from_id": message.peer_id,
            "thread_id": message.thread_id,
            "reply_to": message.reply_to,
            "request_id": message.request_id,
            "state": state,
            "text": message.envelope.payload.text(),
            "attachments": self.attachments_json(message),
            "created_at": format_time(message.created_at),
            "read": message.read_at.is_some(),
        })
    }

    fn attachments_json(&self, message: &MessageRow) -> Vec<Value> {
        let attachments = message.envelope.payload.attachments();
        let paths = attachment_paths(&self.home.files_dir(), &message.id, attachments);
        attachments
            .iter()
            .zip(paths)
            .map(|(a, path)| json!({ "name": a.name, "media_type": a.media_type, "size": a.data.len(), "path": path }))
            .collect()
    }

    fn requests(&self, direction: Option<&str>, limit: usize) -> Result<ExitCode> {
        let store = self.store()?;
        let requests = store.list_requests(direction, limit)?;
        if self.json {
            print_json(&Value::Array(
                requests.iter().map(|r| request_json(&store, r)).collect(),
            ));
        } else if requests.is_empty() {
            println!("No requests.");
        } else {
            println!(
                "{:<32} {:<4} {:<16} {:<12} {:<10} UPDATED",
                "ID", "DIR", "PEER", "HANDLER", "STATE"
            );
            for r in &requests {
                println!(
                    "{:<32} {:<4} {:<16} {:<12} {:<10} {}",
                    r.id,
                    r.direction,
                    peer_name(&store, &r.peer_id),
                    r.handler,
                    r.state,
                    format_time(r.updated_at)
                );
            }
        }
        Ok(ExitCode::SUCCESS)
    }

    async fn cancel(&self, request_id: &str) -> Result<ExitCode> {
        let store = self.store()?;
        let request = store
            .request(request_id)?
            .with_context(|| format!("unknown request {request_id}"))?;
        if request.direction != "out" {
            bail!("{request_id} was sent to you; only the requesting side can cancel it");
        }
        if request.state()?.is_terminal() {
            bail!("{request_id} is already {}", request.state);
        }
        let message = store
            .message(request_id)?
            .context("request message missing")?;
        if message.delivery == "queued" {
            store.mark_failed(request_id, "failed", "canceled before delivery")?;
            store.update_request(
                request_id,
                RequestState::Canceled,
                Some("canceled before delivery"),
                None,
            )?;
            self.done(
                json!({ "request_id": request_id, "state": "canceled" }),
                "Canceled before it was delivered.",
            );
            return Ok(ExitCode::SUCCESS);
        }
        let peer = store
            .peer_by_id(&request.peer_id)?
            .context("peer missing")?;
        let envelope = new_envelope(
            Some(request.thread_id.clone()),
            Some(request_id.into()),
            Payload::Cancel {
                request_id: request_id.into(),
            },
        );
        store.enqueue(&peer.id, &envelope)?;
        let delivery = self.deliver(&peer, &envelope.id).await?;
        self.done(
            json!({ "request_id": request_id, "cancel_message_id": envelope.id, "delivery": delivery }),
            &format!("Cancellation {delivery}; the peer confirms with a response."),
        );
        Ok(ExitCode::SUCCESS)
    }

    fn outbox(&self, all: bool, limit: usize) -> Result<ExitCode> {
        let store = self.store()?;
        let rows = store.list_outgoing(all, limit)?;
        if self.json {
            let values: Vec<Value> = rows
                .iter()
                .map(|m| {
                    json!({
                        "id": m.id, "kind": m.kind, "to": peer_name(&store, &m.peer_id),
                        "thread_id": m.thread_id, "delivery": m.delivery, "attempts": m.attempts,
                        "last_error": m.last_error, "created_at": format_time(m.created_at),
                    })
                })
                .collect();
            print_json(&Value::Array(values));
        } else if rows.is_empty() {
            println!("Nothing waiting for delivery.");
        } else {
            for m in rows {
                println!(
                    "{} {:<8} to {:<16} {:<9} attempts={} {}",
                    m.id,
                    m.kind,
                    peer_name(&store, &m.peer_id),
                    m.delivery,
                    m.attempts,
                    m.last_error.unwrap_or_default()
                );
            }
        }
        Ok(ExitCode::SUCCESS)
    }

    /// Tries to deliver right away and returns the resulting delivery state.
    async fn deliver(&self, peer: &Peer, message_id: &str) -> Result<String> {
        let access = Access::open(&self.home, &self.config).await?;
        let result = access.flush(Some(&peer.id)).await;
        let local = access.is_local();
        access.close().await;
        result?;
        let message = self
            .store()?
            .message(message_id)?
            .context("message missing")?;
        if message.delivery == "queued" && !self.json {
            eprintln!(
                "{} is not reachable right now ({}); the message is queued{}.",
                peer.name,
                message.last_error.as_deref().unwrap_or("unknown error"),
                if local {
                    " and is retried whenever a node runs (`alink serve`)"
                } else {
                    " and retried automatically"
                }
            );
        }
        if message.delivery == "failed" {
            bail!(
                "{} rejected the message: {}",
                peer.name,
                message.last_error.unwrap_or_default()
            );
        }
        Ok(message.delivery)
    }

    fn report_sent(&self, envelope: &Envelope, peer: &Peer, delivery: &str, handler: Option<&str>) {
        if self.json {
            let mut value = json!({
                "id": envelope.id,
                "thread_id": envelope.thread_id,
                "to": peer.name,
                "delivery": delivery,
            });
            if handler.is_some() {
                value["request_id"] = json!(envelope.id);
                value["state"] = json!("submitted");
            }
            print_json(&value);
        } else {
            let what = if handler.is_some() {
                "Request"
            } else {
                "Message"
            };
            println!(
                "{what} {} {delivery} to {} (thread {})",
                envelope.id, peer.name, envelope.thread_id
            );
            if handler.is_some() {
                println!("Wait for the result with: alink wait {}", envelope.id);
            }
        }
    }

    fn done(&self, value: Value, human: &str) {
        if self.json {
            print_json(&value);
        } else {
            println!("{human}");
        }
    }
}

fn new_envelope(thread: Option<String>, reply_to: Option<String>, payload: Payload) -> Envelope {
    Envelope {
        v: ENVELOPE_VERSION,
        id: new_id("msg"),
        thread_id: thread.unwrap_or_else(|| new_id("thr")),
        reply_to,
        created_at: now_ms(),
        payload,
    }
}

/// Collects the text and attachments for an outgoing message from arguments and stdin.
fn read_body(text: Option<String>, body: &BodyArgs) -> Result<(String, Vec<Attachment>)> {
    let mut attachments = Vec::new();
    let text = match (text, body.stdin) {
        (Some(text), false) => text,
        (None, true) => read_stdin()?,
        (Some(text), true) => {
            let data = read_stdin()?.into_bytes();
            attachments.push(Attachment {
                name: "stdin.txt".into(),
                media_type: guess_media_type("stdin.txt", &data),
                data,
            });
            text
        }
        (None, false) => bail!("provide the text as an argument or use --stdin"),
    };
    for (i, path) in body.attach.iter().enumerate() {
        let data = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let mut name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        // Process substitution (`--attach <(git diff)`) yields names like "63".
        if name.is_empty() || name.chars().all(|c| c.is_ascii_digit()) {
            name = format!("attachment-{}.txt", i + 1);
        }
        attachments.push(Attachment {
            media_type: guess_media_type(&name, &data),
            name,
            data,
        });
    }
    let total: usize = attachments.iter().map(|a| a.data.len()).sum();
    if total > MAX_ATTACHMENTS {
        bail!("attachments total {total} bytes; the limit is {MAX_ATTACHMENTS}");
    }
    Ok((text, attachments))
}

fn read_stdin() -> Result<String> {
    let mut text = String::new();
    std::io::stdin()
        .read_to_string(&mut text)
        .context("reading stdin")?;
    Ok(text)
}

fn node_running(home: &Home) -> bool {
    std::fs::OpenOptions::new()
        .write(true)
        .open(home.lock_path())
        .map(|f| matches!(f.try_lock(), Err(std::fs::TryLockError::WouldBlock)))
        .unwrap_or(false)
}

fn peer_name(store: &Store, peer_id: &str) -> String {
    store
        .peer_by_id(peer_id)
        .ok()
        .flatten()
        .map(|p| p.name)
        .unwrap_or_else(|| short_id(peer_id).to_string())
}

fn peer_json(peer: &Peer) -> Value {
    json!({
        "name": peer.name,
        "id": peer.id,
        "fingerprint": fingerprint(&peer.id),
        "card": peer.card,
        "added_at": format_time(peer.added_at),
        "last_seen_at": peer.last_seen_at.map(format_time),
    })
}

fn request_json(store: &Store, r: &RequestRow) -> Value {
    json!({
        "request_id": r.id,
        "direction": r.direction,
        "peer": peer_name(store, &r.peer_id),
        "thread_id": r.thread_id,
        "handler": r.handler,
        "state": r.state,
        "note": r.note,
        "created_at": format_time(r.created_at),
        "updated_at": format_time(r.updated_at),
    })
}

fn print_handlers(card: &PeerCard) {
    if card.handlers.is_empty() {
        println!("  offers you no handlers");
        return;
    }
    println!("  handlers you may run:");
    for h in &card.handlers {
        println!(
            "    {:<14} {}",
            h.name,
            h.description.as_deref().unwrap_or("")
        );
    }
}

/// Short, human-comparable form of an endpoint ID, e.g. `3F2A-91C0-7D4E-B812`.
fn fingerprint(id: &str) -> String {
    let upper = id.to_ascii_uppercase();
    let head = &upper[..upper.len().min(16)];
    head.as_bytes()
        .chunks(4)
        .map(|c| std::str::from_utf8(c).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("-")
}

fn join<T: AsRef<str>>(items: &[T]) -> String {
    if items.is_empty() {
        return "none".into();
    }
    items
        .iter()
        .map(|s| s.as_ref())
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("JSON serializes")
    );
}
