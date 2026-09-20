import hashlib
import json
import math
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

from train import load_checkpoint, save_checkpoint, train_steps


class CheckpointTests(unittest.TestCase):
    def test_one_step_matches_stage1_gradient(self):
        self.assertEqual(train_steps(0.0, 0, 1, 0.1), (0.25, 1))

    def test_resume_matches_uninterrupted(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "step-40.json"
            weight, step = train_steps(0.0, 0, 40, 0.02)
            save_checkpoint(path, weight, step)
            loaded_weight, loaded_step = load_checkpoint(path)
            self.assertEqual((loaded_weight, loaded_step), (weight, 40))
            resumed = train_steps(loaded_weight, loaded_step, 100, 0.02)
            whole = train_steps(0.0, 0, 100, 0.02)
            self.assertEqual(resumed, whole)
            self.assertNotEqual(resumed, train_steps(0.0, 0, 60, 0.02))

    def test_invalid_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "invalid.json"
            path.write_text("not json", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_checkpoint(path)

    def test_non_finite_weight_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "nan.json"
            path.write_text(
                json.dumps({"weight": math.nan, "step": 4}), encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                load_checkpoint(path)

    def test_negative_step_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "negative.json"
            path.write_text(
                json.dumps({"weight": 1.0, "step": -1}), encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                load_checkpoint(path)

    def test_boolean_step_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "boolean.json"
            path.write_text(
                json.dumps({"weight": 1.0, "step": True}), encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                load_checkpoint(path)

    def test_save_checkpoint_does_not_overwrite_existing_step(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "step-4.json"
            save_checkpoint(path, 1.25, 4)
            original = path.read_bytes()
            inode = path.stat().st_ino

            with self.assertRaises(FileExistsError):
                save_checkpoint(path, 9.5, 5)

            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(path.stat().st_ino, inode)


class TrainerContractTests(unittest.TestCase):
    fixture_root = Path(__file__).parent
    trainer = fixture_root / "train.py"

    def run_trainer(
        self,
        result_path,
        checkpoint_dir,
        *arguments,
        steps=4,
        experiment_id="experiment:fixture",
    ):
        environment = os.environ.copy()
        environment.update(
            {
                "PUEUE_AGENT_EXPERIMENT_ID": experiment_id,
                "PUEUE_AGENT_RESULT_PATH": str(result_path),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        return subprocess.run(
            [
                sys.executable,
                str(self.trainer),
                "--steps",
                str(steps),
                "--learning-rate",
                "0.02",
                "--step-delay",
                "0",
                "--checkpoint-dir",
                str(checkpoint_dir),
                *arguments,
            ],
            cwd=self.fixture_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_subprocess_writes_manifest_and_preserves_result_inode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_path = root / "result.json"
            checkpoint_dir = root / "checkpoints"
            result_path.write_text("old result", encoding="utf-8")
            result_path.chmod(0o600)
            inode_before = result_path.stat().st_ino

            completed = self.run_trainer(result_path, checkpoint_dir)

            self.assertEqual(result_path.stat().st_ino, inode_before)
            self.assertEqual(stat.S_IMODE(result_path.stat().st_mode), 0o600)
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(result["schema_version"], 1)
            self.assertEqual(result["experiment_id"], "experiment:fixture")
            final_checkpoint = json.loads(
                (checkpoint_dir / "experiment:fixture" / "step-4.json").read_text(
                    encoding="utf-8"
                )
            )
            heldout_xs = (-0.75, -0.25, 0.25, 0.75)
            expected_loss = sum(
                (final_checkpoint["weight"] * x - 2.0 * x) ** 2
                for x in heldout_xs
            ) / len(heldout_xs)
            self.assertTrue(math.isfinite(expected_loss))
            self.assertEqual(result["metrics"]["loss"], expected_loss)
            self.assertIn("step=4", completed.stdout)
            self.assertIn("checkpointdigest=", completed.stdout)
            self.assertIn("weight=", completed.stdout)

    def test_subprocess_resume_isolated_from_source_and_matches_metric(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkpoint_dir = root / "checkpoints"
            source_id = "experiment:source"
            successor_id = "experiment:successor"
            source_result = root / "source.json"
            self.run_trainer(
                source_result,
                checkpoint_dir,
                steps=4,
                experiment_id=source_id,
            )
            source_checkpoint = checkpoint_dir / source_id / "step-2.json"
            source_latest = checkpoint_dir / source_id / "step-4.json"
            source_latest_before = source_latest.read_bytes()
            checkpoint = json.loads(source_checkpoint.read_text(encoding="utf-8"))
            checkpoint_digest = hashlib.sha256(source_checkpoint.read_bytes()).hexdigest()

            resumed = self.run_trainer(
                root / "resumed.json",
                checkpoint_dir,
                "--resume",
                str(source_checkpoint),
                steps=4,
                experiment_id=successor_id,
            )
            source_manifest = json.loads(source_result.read_text(encoding="utf-8"))
            resumed_manifest = json.loads(
                (root / "resumed.json").read_text(encoding="utf-8")
            )

            self.assertIn(
                f"resume-load checkpointdigest={checkpoint_digest} "
                f"step=2 weight={checkpoint['weight']}",
                resumed.stdout,
            )
            self.assertEqual(
                resumed_manifest["metrics"], source_manifest["metrics"]
            )
            self.assertEqual(source_latest.read_bytes(), source_latest_before)
            self.assertTrue(
                (checkpoint_dir / successor_id / "step-4.json").exists()
            )
            self.assertNotIn("step=0 loss=", resumed.stdout)
            self.assertNotEqual(checkpoint["weight"], 0.0)

            cold = self.run_trainer(
                root / "cold.json",
                checkpoint_dir,
                steps=2,
                experiment_id="experiment:cold",
            )
            cold_manifest = json.loads(
                (root / "cold.json").read_text(encoding="utf-8")
            )
            self.assertIn("step=0 loss=", cold.stdout)
            self.assertNotEqual(
                cold_manifest["metrics"], resumed_manifest["metrics"]
            )

    def test_subprocess_creates_private_manifest_and_checkpoints(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_path = root / "result.json"
            checkpoint_dir = root / "checkpoints"

            self.run_trainer(result_path, checkpoint_dir)

            self.assertEqual(stat.S_IMODE(result_path.stat().st_mode), 0o600)
            self.assertEqual(
                sorted(
                    path.relative_to(checkpoint_dir).as_posix()
                    for path in checkpoint_dir.rglob("step-*.json")
                ),
                [
                    "experiment:fixture/step-0.json",
                    "experiment:fixture/step-1.json",
                    "experiment:fixture/step-2.json",
                    "experiment:fixture/step-3.json",
                    "experiment:fixture/step-4.json",
                ],
            )


if __name__ == "__main__":
    unittest.main()
