import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time


LEARNING_RATE = 0.001
_TRAINING_XS = (-1.0, -0.5, 0.5, 1.0)
_HELDOUT_XS = (-0.75, -0.25, 0.25, 0.75)


def _finite(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be finite")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _step(value, name):
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def train_steps(
    weight: float, start_step: int, end_step: int, learning_rate: float
) -> tuple[float, int]:
    weight = _finite(weight, "weight")
    start_step = _step(start_step, "start_step")
    end_step = _step(end_step, "end_step")
    learning_rate = _finite(learning_rate, "learning_rate")
    if end_step < start_step:
        raise ValueError("end_step must be greater than or equal to start_step")

    for _ in range(start_step, end_step):
        gradient = sum(
            2.0 * (weight * x - 2.0 * x) * x for x in _TRAINING_XS
        ) / len(_TRAINING_XS)
        weight -= learning_rate * gradient
        weight = _finite(weight, "weight")
    return weight, end_step


def evaluate(weight):
    weight = _finite(weight, "weight")
    loss = sum((weight * x - 2.0 * x) ** 2 for x in _HELDOUT_XS) / len(_HELDOUT_XS)
    return _finite(loss, "loss")


def save_checkpoint(path, weight, step):
    path = Path(path)
    weight = _finite(weight, "weight")
    step = _step(step, "step")
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        {"schema_version": 1, "step": step, "weight": weight},
        allow_nan=False,
        sort_keys=True,
    )

    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            os.fchmod(temporary.fileno(), 0o600)
            temporary.write(encoded)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())

        # A hard link publishes the fsynced temporary inode without allowing
        # an existing per-step checkpoint to be replaced.
        os.link(temporary_path, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass


def load_checkpoint(path) -> tuple[float, int]:
    path = Path(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("checkpoint JSON is invalid") from error
    if not isinstance(value, dict):
        raise ValueError("checkpoint must be a JSON object")
    try:
        weight = value["weight"]
        step = value["step"]
    except KeyError as error:
        raise ValueError("checkpoint must contain weight and step") from error
    return _finite(weight, "checkpoint weight"), _step(step, "checkpoint step")


def checkpoint_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _checkpoint_namespace(checkpoint_dir, experiment_id):
    component = Path(experiment_id)
    if not experiment_id or experiment_id in {".", ".."} or component.name != experiment_id:
        raise ValueError("experiment id must be a single path component")
    return Path(checkpoint_dir) / component


def _write_result_manifest(path, experiment_id, loss):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        {
            "schema_version": 1,
            "experiment_id": experiment_id,
            "metrics": {"loss": loss},
        },
        allow_nan=False,
        sort_keys=True,
    )
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        result_file = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = None
        with result_file:
            result_file.write(encoded)
            result_file.write("\n")
            result_file.flush()
            os.fsync(result_file.fileno())
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _non_negative_int(value):
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _non_negative_float(value):
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be finite") from error
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be finite and non-negative")
    return parsed


def _parser():
    parser = argparse.ArgumentParser(description="Run the CPU research learner")
    parser.add_argument("--steps", type=_non_negative_int, default=100)
    parser.add_argument("--learning-rate", type=float, default=LEARNING_RATE)
    parser.add_argument("--step-delay", type=_non_negative_float, default=0.0)
    parser.add_argument("--checkpoint-dir", type=Path, required=True)
    parser.add_argument("--resume", type=Path)
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    learning_rate = _finite(args.learning_rate, "learning_rate")
    experiment_id = os.environ["PUEUE_AGENT_EXPERIMENT_ID"]
    result_path = os.environ["PUEUE_AGENT_RESULT_PATH"]
    checkpoint_dir = _checkpoint_namespace(args.checkpoint_dir, experiment_id)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    if args.resume is None:
        weight, step = 0.0, 0
        initial_path = checkpoint_dir / "step-0.json"
        save_checkpoint(initial_path, weight, step)
        print(
            f"step=0 loss={evaluate(weight)} "
            f"checkpointdigest={checkpoint_digest(initial_path)} weight={weight}",
            flush=True,
        )
    else:
        weight, step = load_checkpoint(args.resume)
        print(
            f"resume-load checkpointdigest={checkpoint_digest(args.resume)} "
            f"step={step} weight={weight}",
            flush=True,
        )

    if args.steps < step:
        raise ValueError("steps must be greater than or equal to the resume step")

    for next_step in range(step + 1, args.steps + 1):
        weight, step = train_steps(weight, step, next_step, learning_rate)
        checkpoint_path = checkpoint_dir / f"step-{step}.json"
        save_checkpoint(checkpoint_path, weight, step)
        loss = evaluate(weight)
        print(
            f"step={step} loss={loss} "
            f"checkpointdigest={checkpoint_digest(checkpoint_path)} weight={weight}",
            flush=True,
        )
        if args.step_delay:
            time.sleep(args.step_delay)

    loss = evaluate(weight)
    _write_result_manifest(result_path, experiment_id, loss)
    print(f"final loss={loss} step={step} weight={weight}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
