# Security policy

alink connects to other machines and can run locally configured commands when a paired
peer asks it to. Security reports are very welcome.

## Reporting a vulnerability

Report vulnerabilities privately through
[GitHub's private vulnerability reporting](https://github.com/dstotijn/alink/security/advisories/new).
Do not open a public issue.

Please include what you found, how to reproduce it, and the impact you expect. You can
expect an acknowledgement within a week. Fixes are released as a new version, with a GitHub
security advisory once users can upgrade.

## Supported versions

Only the latest release receives security fixes.

## Scope

In scope, for example:

- a peer or an unpaired endpoint making alink run something other than the configured
  handler command, or changing its working directory, arguments or environment;
- an unpaired endpoint delivering messages, or a peer reading or changing another peer's
  requests;
- redeeming an invite without its secret, or more than once;
- weaknesses in how alink stores keys and messages on disk.

Out of scope:

- what a handler does with a request it was configured to accept. A handler that runs an
  agent with broad permissions gives every allowed peer that power; see the security model
  in the README;
- issues in iroh or other dependencies, which should be reported to those projects. Do tell
  us if alink uses them in a way that makes the issue exploitable.
