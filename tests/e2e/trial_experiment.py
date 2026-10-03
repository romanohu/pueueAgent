#!/usr/bin/env python3
"""Stdlib-only child used by the isolated real-Pueue trial supervisor."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time
from typing import NoReturn
import uuid


EXPECTED_INPUT = b"stage3 trial fixture v1\n"
RUNTIME_NAMES = (
    "PUEUE_AGENT_EXPERIMENT_ID",
    "PUEUE_AGENT_CAMPAIGN_ID",
    "PUEUE_AGENT_RESULT_PATH",
    "PUEUE_AGENT_ARTIFACT_DIR",
)


def fail(code: str) -> NoReturn:
    print(f"trial fixture failed: {code}", file=sys.stderr)
    raise SystemExit(70)


def write_private(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(payload)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=("success", "queued-timeout", "running-timeout", "extra-task"),
    )
    parser.add_argument("--expected-cwd", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--run-dir", required=True)
    arguments = parser.parse_args()

    project = Path(arguments.expected_cwd).resolve(strict=True)
    if Path.cwd().resolve(strict=True) != project:
        fail("cwd")
    input_path = Path(arguments.input).resolve(strict=True)
    if input_path.read_bytes() != EXPECTED_INPUT:
        fail("input")

    values = {name: os.environ.get(name) for name in RUNTIME_NAMES}
    if any(value is None or value == "" for value in values.values()):
        fail("runtime_environment")
    try:
        experiment_id = str(uuid.UUID(values["PUEUE_AGENT_EXPERIMENT_ID"]))
        campaign_id = str(uuid.UUID(values["PUEUE_AGENT_CAMPAIGN_ID"]))
        generation = Path(values["PUEUE_AGENT_RESULT_PATH"]).parent
        artifact_dir = Path(values["PUEUE_AGENT_ARTIFACT_DIR"])
        generation_id = str(uuid.UUID(generation.name))
    except (TypeError, ValueError, AttributeError):
        fail("runtime_identity")

    expected_generation = project / ".pueue-agent" / "trials" / generation_id
    expected_result = expected_generation / "result.json"
    expected_artifacts = expected_generation / "artifacts"
    if generation != expected_generation or Path(values["PUEUE_AGENT_RESULT_PATH"]) != expected_result:
        fail("result_path")
    if artifact_dir != expected_artifacts:
        fail("artifact_path")
    service = project / ".pueue-agent"
    trials = service / "trials"
    for path, expected_mode, reason in (
        (service, 0o755, "service_mode"),
        (trials, 0o700, "trials_mode"),
        (generation, 0o700, "generation_mode"),
    ):
        try:
            metadata = path.lstat()
        except OSError:
            fail(reason)
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
            fail(reason)
        if stat.S_IMODE(metadata.st_mode) != expected_mode:
            fail(reason)
    if expected_result.exists() or expected_result.is_symlink() or artifact_dir.exists() or artifact_dir.is_symlink():
        fail("output_not_empty")

    run_dir = Path(arguments.run_dir).resolve(strict=True)
    started = run_dir / "started"
    write_private(started, b"started\n")
    write_private(run_dir / "fixture.pid", f"{os.getpid()}\n".encode("ascii"))
    if arguments.mode != "success":
        release = run_dir / "release"
        deadline = time.monotonic() + 20.0
        while not release.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        if not release.exists():
            fail("release_timeout")

    artifact_dir.mkdir(mode=0o700)
    if stat.S_IMODE(artifact_dir.lstat().st_mode) != 0o700:
        fail("artifact_mode")
    artifact = artifact_dir / "fixture.txt"
    write_private(artifact, b"bounded trial artifact\n")
    manifest = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "metrics": {"fixture_loss": 0.25},
    }
    encoded = json.dumps(manifest, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > 16 * 1024:
        fail("manifest_size")
    write_private(expected_result, encoded)
    result_metadata = expected_result.lstat()
    if (
        not stat.S_ISREG(result_metadata.st_mode)
        or result_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(result_metadata.st_mode) != 0o600
        or result_metadata.st_nlink != 1
    ):
        fail("result_mode")
    evidence = {
        "trial_id": generation_id,
        "experiment_id": experiment_id,
        "campaign_id": campaign_id,
        "generation_id": generation_id,
        "service_mode": stat.S_IMODE(service.lstat().st_mode),
        "trials_mode": stat.S_IMODE(trials.lstat().st_mode),
        "generation_mode": stat.S_IMODE(generation.lstat().st_mode),
        "artifact_mode": stat.S_IMODE(artifact_dir.lstat().st_mode),
        "result_mode": stat.S_IMODE(result_metadata.st_mode),
        "result_path": str(expected_result),
        "artifact_dir": str(artifact_dir),
        "result_sha256": hashlib.sha256(encoded).hexdigest(),
        "artifact_sha256": hashlib.sha256(b"bounded trial artifact\n").hexdigest(),
    }
    write_private(
        run_dir / "fixture-evidence.json",
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
