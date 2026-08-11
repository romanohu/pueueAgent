# launchd service health status design

## Scope

Fix only the macOS launchd service health check. A successful `launchctl print`
means that the job is loaded, not necessarily that its daemon is running. The
existing systemd status behavior and all upgrade, scheduler, periodic, and
cancel logic remain unchanged.

## Behavior

`launchd_status` will inspect successful `launchctl print` stdout. It returns
`ServiceStatus::Running` only when a line, after leading whitespace is removed,
matches `state = running` case-insensitively with whitespace around the equals
sign accepted. A successful command with `state = exited`, another state, or no
state line returns `ServiceStatus::Stopped`. A failed `launchctl print` also
returns `Stopped`, preserving the existing command-error handling.

## Test seam

The output interpretation will be isolated in a testable helper. Focused fake
outputs will cover running, exited, missing state, mixed-case/indented running,
and command failure. The service integration tests will retain coverage that
enable checks daemon health after installation; systemd behavior will be
covered by its existing status semantics and remain unmodified.

