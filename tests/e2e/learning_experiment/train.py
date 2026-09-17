import json
import os

from model import evaluate, train


def main():
    experiment_id = os.environ["PUEUE_AGENT_EXPERIMENT_ID"]
    result_path = os.environ["PUEUE_AGENT_RESULT_PATH"]
    loss = evaluate(train())
    encoded = json.dumps(
        {
            "schema_version": 1,
            "experiment_id": experiment_id,
            "metrics": {"loss": loss},
        },
        allow_nan=False,
        sort_keys=True,
    )

    os.makedirs(os.path.dirname(result_path) or ".", exist_ok=True)
    descriptor = os.open(
        result_path,
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
    finally:
        if descriptor is not None:
            os.close(descriptor)
    print(f"loss={loss}")


if __name__ == "__main__":
    main()
