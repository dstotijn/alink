# Running alink as a service

Read this when the user wants `alink serve` to keep running in the background, across
logouts and reboots. Ask the user before installing or starting a service: it keeps a
network node online and runs any configured handlers unattended.

## Before you start

- Find the binary with `command -v alink` and use that absolute path below.
- Service managers start processes with a minimal `PATH`. If `config.toml` has handlers,
  check that their commands use absolute paths (for example the output of
  `command -v claude`), or set `PATH` in the service definition.
- If the user runs alink with a custom `ALINK_HOME`, set the same variable in the service.

## Linux (systemd user service)

Write `~/.config/systemd/user/alink.service`, replacing the `ExecStart` path if alink is
not in `~/.local/bin`:

```ini
[Unit]
Description=alink node
After=network-online.target

[Service]
ExecStart=%h/.local/bin/alink serve
Restart=on-failure

[Install]
WantedBy=default.target
```

Enable and start it, and let it run without an active login session:

```sh
systemctl --user enable --now alink
loginctl enable-linger "$USER"
```

Logs: `journalctl --user -u alink`.

## macOS (launchd agent)

Write `~/Library/LaunchAgents/com.github.dstotijn.alink.plist`. launchd does not expand `~`
or environment variables, so use absolute paths throughout:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.github.dstotijn.alink</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/you/.local/bin/alink</string>
    <string>serve</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardErrorPath</key>
  <string>/Users/you/Library/Logs/alink.log</string>
</dict>
</plist>
```

Check it with `plutil -lint`, then load it:

```sh
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.github.dstotijn.alink.plist
```

To stop and remove it later: `launchctl bootout "gui/$(id -u)/com.github.dstotijn.alink"`.

## Verify

`alink whoami --json` should report `"node_running": true`. If it doesn't, check the logs
above. A common cause is another `alink serve` already running for the same alink home.
