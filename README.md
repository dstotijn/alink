# alink

[![CI](https://github.com/dstotijn/alink/actions/workflows/ci.yml/badge.svg)](https://github.com/dstotijn/alink/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**alink** gives AI agents a persistent identity, an inbox, and a small protocol for working
together asynchronously across machines. A Codex session on your laptop can ask a Claude Code
session on a colleague's machine a question, keep working, and pick up the answer later.

It works via CLI, so any agent that can run shell commands can use it. The included
[Agent Skill](skills/alink/SKILL.md) shows them how. Agents can reach each other across the
Internet without opening ports or running a public server, and everything they send is
encrypted end to end.

alink is early software. The wire protocol, config format and CLI may change between 0.x
releases, so paired machines should run the same version.

## Example

Bob's Codex session is building a contact center integration and hits a product decision
that only Alice can make. It asks Alice's Claude Code session and moves on to other work:

```console
$ alink send alice-claude "Calls that fail during an agent transfer: should the integration retry them, or drop them and log a missed call? I'm blocked on this, so I'll work on the reporting export meanwhile."
Message msg_01M4C162KAPVE7QHJXR9EB6NGK delivered to alice-claude (thread thr_01M4C162KA3HAXRBJ1GDQBW4SB)
```

Alice's agent raises the question with her when she is back. Hours later, her answer is in
Bob's inbox:

```console
$ alink inbox
msg_01M4C162KRDYQJ1XB576YVCSK6 from alice-claude · thread thr_01M4C162KA3HAXRBJ1GDQBW4SB · 2026-10-07T20:34:23Z
  in reply to msg_01M4C162KAPVE7QHJXR9EB6NGK
  Alice says: retry once after two seconds, then log a missed call tagged
  transfer_failed. Those customers get a callback offer, so never retry more than once.
```

In practice the agents run these commands themselves, through the skill, and can
[stay reachable](#staying-reachable) so they react as soon as a message arrives. If a machine
is offline, messages wait in the sender's outbox until both machines are online.

Peers can also offer handlers: commands such as `claude -p` that run unattended when a peer
asks, for example for a read-only code review. See [Configuration](#configuration).

## Install

```sh
curl -fsSL https://dstotijn.github.io/alink/install.sh | sh
```

The script downloads the latest [release](https://github.com/dstotijn/alink/releases) for your
platform, verifies its SHA-256 checksum and installs it to `~/.local/bin`. To pin a version or
install elsewhere, set variables for `sh`:

```sh
curl -fsSL https://dstotijn.github.io/alink/install.sh | ALINK_VERSION=v0.1.0 ALINK_INSTALL_DIR=/opt/bin sh
```

To install by hand, download `alink-<target>.tar.gz` and its `.sha256` file from the
releases page, check it with `shasum -a 256 -c alink-<target>.tar.gz.sha256`, unpack it and
put `alink` on your `PATH`. To build from source, run
`cargo install --git https://github.com/dstotijn/alink` (Rust 1.91 or later).

Release binaries are available for macOS and Linux, on arm64 and x86_64. The Linux binaries
are statically linked. alink has no runtime dependencies. Windows is currently not supported, because
alink relies on Unix sockets.

## Quick start

Create an identity, then invite the other machine:

```sh
alink init --name alice-claude    # creates ~/.config/alink, or $ALINK_HOME
alink invite                      # prints alink://invite/... and waits for the peer
```

Send the invite to the other person over a channel you trust. It works once and expires
after 24 hours by default. On their machine:

```sh
alink init --name bob-codex
alink join 'alink://invite/...'
```

Both sides now hold each other's public keys. Run `alink serve` in a separate terminal, or
[as a service](skills/alink/references/service.md), to receive messages at any time and to
run handlers. Without it, alink is only online while a command runs.

Every command takes `--json` for scripts and agents. `alink help <command>` shows a
command's options:

```text
Encrypted, asynchronous agent-to-agent messaging over iroh

Usage: alink [OPTIONS] <COMMAND>

Commands:
  init      Create this endpoint's identity and config
  whoami    Show this endpoint's identity
  serve     Run the node: receive messages, deliver the outbox, run handlers
  invite    Create a one-time invite for another endpoint
  join      Redeem an invite and pair with the endpoint that created it
  peers     List and manage paired peers
  send      Send a message to a peer
  reply     Reply to a received message, in the same thread
  run       Ask a peer to run one of its handlers
  wait      Wait for a request to finish, or for unread messages (in a thread or anywhere)
  listen    Report new unread messages as they arrive, without marking them as read
  inbox     Show received messages and responses. Shown messages are marked as read
  requests  List requests and their states
  cancel    Ask a peer to stop working on a request you sent
  outbox    Show outgoing messages that are not yet delivered
  skill     Print the agent skill (SKILL.md) that teaches agents to use alink
  help      Print this message or the help of the given subcommand(s)

Options:
      --json     Print machine-readable JSON
  -h, --help     Print help
  -V, --version  Print version
```

## Using with Claude Code and Codex

Install the agent skill with the [skills CLI](https://skills.sh). It asks which agents to
install it for:

```sh
npx skills add dstotijn/alink
```

`alink skill` prints the same `SKILL.md`. With the skill installed, ask your agent in plain
language, for example "ask alice-claude for a second opinion on this diff" or "check whether
any agent replied". The skill covers pairing, messages, requests and waiting for answers.

### Staying reachable

Handlers serve unattended requests. For a live Claude Code or Codex session that should
react when a peer writes, `alink listen` watches for unread messages in any thread. It never
marks them as read; it prints one line per new batch (`--json` for JSON) and, with
`--notify`, runs a command that receives only metadata (`ALINK_COUNT`, `ALINK_FROM`,
`ALINK_THREADS`, `ALINK_MESSAGE_IDS`). For Codex, that command can queue a fixed prompt into
the running session:

```sh
alink listen --notify 'codex queue --thread "$CODEX_THREAD_ID" --message "New alink messages from $ALINK_FROM. Run alink inbox --json."'
```

Claude Code can watch `alink listen --json` with its Monitor tool, or keep a background
`alink wait` running. The skill explains which option an agent should pick.

## How it works

```text
Codex ── skill ──▶ alink ◀──── iroh QUIC (E2E encrypted) ────▶ alink ◀── skill ── Claude Code
                    │                                            │
               SQLite outbox,                              SQLite inbox,
               inbox, peers                                handlers ──▶ claude -p
```

- **Transport: [iroh](https://iroh.computer).** Each endpoint's identity is an Ed25519 key,
  and peers dial each other by public key. iroh handles NAT traversal and hole punching, and
  falls back to relays when a direct path is impossible. Connections are QUIC with TLS that
  authenticates both endpoint keys, so traffic is end-to-end encrypted. Relays only forward
  ciphertext. No inbound ports and no server of your own are needed.
- **Asynchronous delivery: a durable outbox.** Every outgoing message is first written to
  local SQLite, then delivered and acknowledged by the receiver. If the peer is offline, the
  message stays queued and is retried with backoff (up to five minutes between attempts, kept
  for seven days). When a peer comes online it greets its peers, which triggers immediate
  retries towards it. Delivery is idempotent: message IDs are unique and duplicates are
  ignored.
- **Data model: modelled on [A2A](https://a2a-protocol.org/).** Threads correspond to A2A
  contexts, requests to tasks with a lifecycle
  (`submitted → working → completed | failed | canceled | rejected`), attachments to parts,
  and the `PeerCard` to a reduced Agent Card. The wire format is alink's own and stays small
  (see [`src/proto.rs`](src/proto.rs)).
- **Handlers are local capabilities.** A peer can only *name* a handler. The command, working
  directory, environment, timeout and output limit come from your own config. The request
  reaches the process on stdin and is never interpolated into arguments. Each handler lists
  the peers allowed to call it, and a peer's card shows only the handlers that peer may use.

### One node per identity

Network work happens in a *node*, which owns the iroh endpoint. `alink serve` runs one
permanently. Without it, commands that need the network (`send`, `run`, `wait`, `invite`,
`join`) start a temporary node for as long as they run. When a node is already running, the
CLI talks to it over a Unix socket instead, so the same identity is never bound twice.

Handlers only run under `alink serve`. Requests that arrive at a temporary node are queued
and run when `alink serve` next starts.

## Configuration

`~/.config/alink/config.toml`:

```toml
name = "alice-claude"
max_concurrent = 2

[network]
mode = "n0"          # iroh's public relays and DNS discovery; "local" disables both
# relays = ["https://relay.example.com"]   # self-hosted iroh relays
# port = 7842

[handler.review]
description = "Read-only code review using Claude Code"
command = ["claude", "-p", "--allowedTools", "Read,Grep,Glob"]
working_directory = "/src/project"
timeout = "15m"
max_output = "2MiB"
peers = ["bob-codex"]              # local peer names or endpoint IDs; "*" for all peers

[handler.second-opinion]
description = "Independent analysis by Codex"
command = ["codex", "exec", "--sandbox", "read-only", "-"]
working_directory = "/src/project"
peers = ["*"]
```

Handlers receive `ALINK_REQUEST_ID`, `ALINK_THREAD_ID`, `ALINK_PEER`, `ALINK_PEER_ID`,
`ALINK_HANDLER` and, when files are attached, `ALINK_ATTACHMENTS_DIR`. Stdout becomes the
response. A non-zero exit, timeout or cancellation is reported back as a failure or
cancellation.

## Security model

- **Treat remote text as untrusted input.** A handler gives every allowed peer the ability
  to drive that command with arbitrary prompts. Configure handlers the way you would
  configure a service exposed to those people: read-only tools, a dedicated working
  directory, never `--dangerously-skip-permissions`.
- Only paired peers can deliver messages. Unknown endpoints can only redeem a valid invite.
- An invite is a bearer secret until it is used. Share it privately and keep the TTL short.
- Messages are encrypted in transit by iroh's QUIC TLS. At rest they sit in plaintext in your
  local SQLite database and `files/` directory, protected by file permissions (`0700` home,
  `0600` key).
- There is no forward secrecy beyond what each QUIC session provides, and no protection
  against a stolen `secret.key`. Run `alink peers remove` on the other side to revoke a
  compromised peer.
- In the default `n0` network mode, iroh's relays and DNS discovery service, run by n0, see
  endpoint IDs, IP addresses and when peers connect, but never message contents. Setting
  `relays` replaces n0's relays; discovery still goes through n0.

To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Limitations and next steps

- **Both nodes must be online at the same time at some point.** iroh relays forward live
  connections but do not store messages. The sender's outbox makes this asynchronous as long
  as the two machines overlap eventually. A store-and-forward mailbox, an always-on alink
  node that holds sealed envelopes for offline peers, is the natural next step if that
  overlap is too rare.
- Handlers are one-shot: each request starts a fresh process. `ALINK_THREAD_ID` lets a
  wrapper script map threads to persistent Claude or Codex sessions, for example with
  `claude --resume`.
- Attachments travel inline (up to 8 MiB per message). Large artifacts would fit
  `iroh-blobs`.
- An A2A gateway (`alink gateway --a2a`) could expose peers as standard A2A agents later. The
  data model was chosen so this maps cleanly.

## FAQ

<details>
<summary><strong>Why not A2A?</strong></summary>

[A2A](https://a2a-protocol.org/) defines how agents talk to each other, but it expects the
remote agent to be an HTTP server you can reach at a URL. Claude Code and Codex sessions run
on laptops behind NAT, go offline, and don't host servers. That reachability gap is the
problem alink solves: an addressable identity, encrypted connections without open ports, and
delivery that waits until the other machine is online.

alink's data model follows A2A's (see [How it works](#how-it-works)), so a gateway to A2A
can be added later. Speaking A2A on the wire today would not let standard A2A clients
connect, since both sides would still need alink to reach each other, so it would add weight
without adding interoperability.

</details>

<details>
<summary><strong>Why not simple HTTP?</strong></summary>

With HTTP, at least one side needs a reachable endpoint: a port forward, a tunnel service,
or a hosted server. You then also need to decide who is allowed to call it, get TLS
certificates onto laptops, and queue messages while the other side is offline.

iroh covers most of that. Each machine is identified by a public key, connections are
mutually authenticated and end-to-end encrypted, and NAT traversal falls back to relays when
a direct path fails, so no ports or servers are needed. alink adds the durable outbox on top.

The trade-off: a hosted HTTP mailbox can hold messages while both machines are offline,
which alink cannot do yet (see [Limitations](#limitations-and-next-steps)).

</details>

<details>
<summary><strong>Isn't this a security nightmare?</strong></summary>

It can be if you configure it carelessly, so alink keeps the dangerous parts opt-in and
explicit:

- Only peers you paired with, through a one-time invite, can send you anything.
- Without handlers, peers can only leave messages. Your agent reads them when you, or its
  skill, decide to, and the [skill](skills/alink/SKILL.md) tells it to treat them as
  untrusted input and to ask you before acting beyond what you asked for.
- A handler runs only for the peers it lists. Peers choose the handler by name, never the
  command, arguments, working directory or environment, and their text reaches the process
  on stdin. Timeouts and output limits apply.
- `alink listen --notify` never passes message text to the notify command.

The remaining risk is prompt injection. A handler that runs an agent with tools gives every
allowed peer the ability to drive that agent. Give handlers read-only tools, a dedicated
working directory and a sandbox, and only list peers you would trust with that access. See
the [security model](#security-model) and [SECURITY.md](SECURITY.md).

</details>

## Development

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo deny check                 # advisories and licenses (cargo-deny is pinned in mise.toml)
cargo test                       # unit tests
cargo build && tests/e2e.sh      # two local identities: pairing, requests, offline delivery (needs jq)
```

Set `ALINK_HOME` to run several identities on one machine. `mode = "local"` in `[network]`
keeps test traffic off the public relays.

`src/` layout: `proto.rs` (wire types), `node.rs` (iroh endpoint, delivery, invites),
`handler.rs` (running requests), `store.rs` (SQLite), `control.rs` (local socket),
`ticket.rs` (invite format), `config.rs`, `main.rs` (CLI).

### Releasing

Bump `version` in `Cargo.toml`, commit, then tag and push:

```sh
git tag v0.1.0 && git push origin v0.1.0
```

The release workflow builds the binaries, checks that the tag matches `Cargo.toml` and
publishes the GitHub release. The Pages workflow publishes `install.sh` whenever it changes
on `main`; set the repository's Pages source to "GitHub Actions" once.

## License

[MIT](LICENSE)

---

© 2026 David Stotijn
