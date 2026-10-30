"""Offline rejection tests: never bless incomplete or stale comparison inputs."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ComparisonInputs(unittest.TestCase):
    def fixture(self, root):
        raw = root / "baseline" / "raw"
        raw.mkdir(parents=True)
        for backend in ("redis", "postgres"):
            for case in range(6):
                name = f"{backend}-case{case}-run1"
                data = json.dumps({"Type": "summary"})
                (raw / f"{name}.summary.json").write_text(data)
                (raw / f"{name}.jsonl").write_text(data + "\n")
        return raw

    def check_rejected(self, root, message):
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("compare_report.py")),
                                 str(root), "--repeats", "1"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(message, result.stderr)
        self.assertFalse((root / "comparison.csv").exists())

    def test_missing_raw_with_saved_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = self.fixture(root)
            (raw / "redis-case0-run1.jsonl").unlink()
            self.check_rejected(root, "raw trials")

    def test_stale_sidecar_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = self.fixture(root)
            (raw / "redis-case0-run1.summary.json").write_text('{"Type":"stale"}')
            self.check_rejected(root, "raw/sidecar summary mismatch")


if __name__ == "__main__":
    unittest.main()
