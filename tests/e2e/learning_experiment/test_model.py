import math
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

from model import LEARNING_RATE, evaluate, train


class LearningModelTests(unittest.TestCase):
    def test_one_gradient_step_uses_the_fixed_training_data(self):
        self.assertAlmostEqual(train(learning_rate=0.1, steps=1), 0.25)

    def test_zero_steps_preserves_the_zero_initial_weight(self):
        self.assertEqual(train(learning_rate=0.1, steps=0), 0.0)

    def test_candidate_learning_rate_improves_held_out_mse(self):
        baseline_weight = train(learning_rate=LEARNING_RATE, steps=100)
        candidate_weight = train(learning_rate=0.05, steps=100)

        baseline_loss = evaluate(baseline_weight)
        candidate_loss = evaluate(candidate_weight)

        self.assertTrue(math.isfinite(baseline_loss))
        self.assertTrue(math.isfinite(candidate_loss))
        self.assertLess(candidate_loss, baseline_loss)

    def test_non_finite_training_inputs_are_rejected(self):
        with self.assertRaises(ValueError):
            train(learning_rate=math.nan)
        with self.assertRaises(ValueError):
            evaluate(math.inf)


class TrainerContractTests(unittest.TestCase):
    fixture_root = Path(__file__).parent
    trainer = fixture_root / "train.py"

    def run_trainer(self, result_path):
        environment = os.environ.copy()
        environment.update(
            {
                "PUEUE_AGENT_EXPERIMENT_ID": "experiment:fixture",
                "PUEUE_AGENT_RESULT_PATH": str(result_path),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        return subprocess.run(
            [sys.executable, str(self.trainer)],
            cwd=self.fixture_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_subprocess_writes_a_bounded_manifest_and_preserves_result_inode(self):
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "result.json"
            result_path.write_text("old result", encoding="utf-8")
            result_path.chmod(0o600)
            inode_before = result_path.stat().st_ino
            source_before = {
                path: path.read_bytes() for path in (self.fixture_root / "model.py",)
            }

            completed = self.run_trainer(result_path)

            self.assertIn("loss=", completed.stdout)
            self.assertEqual(result_path.stat().st_ino, inode_before)
            self.assertEqual(stat.S_IMODE(result_path.stat().st_mode), 0o600)
            result = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(result["schema_version"], 1)
            self.assertEqual(result["experiment_id"], "experiment:fixture")
            self.assertTrue(math.isfinite(result["metrics"]["loss"]))
            for path, contents in source_before.items():
                self.assertEqual(path.read_bytes(), contents)

    def test_subprocess_creates_private_result_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            result_path = Path(temporary) / "new-result.json"

            self.run_trainer(result_path)

            self.assertEqual(stat.S_IMODE(result_path.stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
