#!/usr/bin/env python3
"""Run one test child in a private process group and forward stop signals."""

import os
import signal
import subprocess
import sys
import time


GROUP_STOP_TIMEOUT = 5.0

child = None
child_pgid = None
forwarded_signal = None


def reset_child_signals():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def forward_signal(signum):
    if child_pgid is None:
        return
    try:
        os.killpg(child_pgid, signum)
    except ProcessLookupError:
        pass


def handle_signal(signum, _frame):
    global forwarded_signal
    if forwarded_signal is None:
        forwarded_signal = signum
        forward_signal(signum)


def write_child_pid():
    marker = os.environ.get("PUEUE_AGENT_SIGNAL_CHILD_PID_FILE")
    if not marker:
        return
    with open(marker, "w", encoding="ascii") as handle:
        handle.write(f"{child.pid}\n")


def stop_child_group(signum):
    if child is None:
        return
    if child.poll() is None:
        forward_signal(signum)

    deadline = time.monotonic() + GROUP_STOP_TIMEOUT
    while child.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)

    if child.poll() is None:
        forward_signal(signal.SIGKILL)
        child.wait()
    else:
        # The group leader may have exited while a descendant is still
        # running; the private PGID is the only owned group we may signal.
        forward_signal(signal.SIGTERM)


def main():
    global child, child_pgid
    if len(sys.argv) < 2:
        print("usage: signal_supervisor.py COMMAND [ARG ...]", file=sys.stderr)
        return 64

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    child = subprocess.Popen(
        sys.argv[1:],
        preexec_fn=reset_child_signals,
        start_new_session=True,
    )
    child_pgid = os.getpgid(child.pid)
    write_child_pid()
    try:
        while child.poll() is None:
            time.sleep(0.05)
    finally:
        stop_child_group(forwarded_signal or signal.SIGTERM)

    if forwarded_signal is not None:
        return 128 + forwarded_signal
    return child.returncode


if __name__ == "__main__":
    raise SystemExit(main())
