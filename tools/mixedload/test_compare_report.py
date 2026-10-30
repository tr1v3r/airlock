"""Offline rejection tests: never bless incomplete or stale comparison inputs."""
import json
import csv
import gzip
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

    def test_uncompressed_trial_preserves_renew_metrics(self):
        source = Path(__file__).resolve().parents[2] / "docs/validation/terminal-history/runs"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for variant in ("baseline", "candidate"):
                raw = root / variant / "raw"
                raw.mkdir(parents=True)
                for path in (source / variant / "raw").iterdir():
                    (raw / path.name).symlink_to(path)
            compressed = root / "baseline/raw/redis-case0-run1.jsonl.gz"
            contents = gzip.decompress(compressed.read_bytes())
            compressed.unlink()
            compressed.with_suffix("").write_bytes(contents)
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("compare_report.py")),
                                     str(root)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            with (root / "comparison.csv").open() as f:
                row = next(r for r in csv.DictReader(f)
                           if r["trial"] == "redis-case0-run1" and r["variant"] == "baseline")
            self.assertGreater(int(row["renew_n"]), 0)
            self.assertTrue(row["renew_p95_ms"])


if __name__ == "__main__":
    unittest.main()
