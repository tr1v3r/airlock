"""Offline regression checks; never connect to a backend or modify saved data."""
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
RAW = HERE.parent.parent / 'docs/validation/mixed-capacity/raw'


class WorkflowTest(unittest.TestCase):
    def test_runner_preserves_nonempty_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            sentinel = out / 'existing.summary.json'
            sentinel.write_text('original')
            env = dict(os.environ, OUTPUT_DIR=tmp, REDIS_ADDR='unused',
                       POSTGRES_DSN='unused', MIXEDLOAD_BINARY='/does-not-exist')
            result = subprocess.run(['sh', str(HERE / 'run.sh')], env=env,
                                    capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('must be empty', result.stderr)
            self.assertEqual(sentinel.read_text(), 'original')
            self.assertEqual(list(out.iterdir()), [sentinel])

    def analyze(self, mutate=None):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'raw').mkdir()
            source = RAW / 'redis-case0-run1.jsonl.gz'
            with gzip.open(source, 'rt') as stream:
                data = [json.loads(line) for line in stream]
            if mutate:
                mutate(data)
            content = ''.join(json.dumps(row) + '\n' for row in data)
            (root / 'raw/trial.jsonl').write_text(content)
            if mutate is duplicate:
                with gzip.open(root / 'raw/trial.jsonl.gz', 'wt') as stream:
                    stream.write(content)
            result = subprocess.run([sys.executable, str(HERE / 'analyze.py'), tmp],
                                    capture_output=True, text=True, check=False)
            if mutate:
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'summary.json').exists())
            else:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue((root / 'latency.csv').exists())

    def test_valid_trial(self):
        self.analyze()

    def test_duplicate_trial(self):
        self.analyze(duplicate)

    def test_missing_sample(self):
        self.analyze(lambda data: data.pop(next(i for i, s in enumerate(data) if s.get('Phase') == 'measured')))

    def test_summary_without_samples(self):
        self.analyze(lambda data: data.__delitem__(slice(None, -1)))


def duplicate(data):
    pass


if __name__ == '__main__':
    unittest.main()
