"""Regression checks for source-bound benchmark evidence."""
import contextlib
import io
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import bench_shared_report as report


class SharedBenchEvidenceTest(unittest.TestCase):
    def test_resume_requires_source_iterations_and_completed_run(self):
        source = pathlib.Path(__file__).with_name('bench-shared.sh').read_text()
        function = re.search(r'^cell_valid\(\) \{.*?^\}', source, re.M | re.S).group()
        valid = ('# fingerprint: fresh\n# benchtime: 100x\n'
                 'BenchmarkSharedUncontended/W=4/H=0-8 100 123 ns/op\n'
                 'PASS\nok\tmodule\t0.1s\n')
        cases = [(valid, True),
                 (valid.replace('fresh', 'old'), False),
                 (valid.replace('100x', '99x'), False),
                 (valid.replace('PASS\nok\tmodule\t0.1s', 'FAIL'), False),
                 ('# fingerprint: fresh\n# benchtime: 100x\nPASS\nok\tmodule\t0.1s\n', False)]
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'raw.txt'
            for body, expected in cases:
                with self.subTest(body=body):
                    path.write_text(body)
                    result = subprocess.run(
                        ['bash', '-c', function + '\nBENCH_FINGERPRINT=fresh\ncell_valid "$1" 100x',
                         'test', str(path)], capture_output=True)
                    self.assertEqual(result.returncode == 0, expected)

    def test_report_excludes_old_and_unidentified_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            raw = root / 'raw'
            raw.mkdir()
            for worker, fingerprint in [(4, 'fresh'), (16, 'old'), (64, None)]:
                header = f'# fingerprint: {fingerprint}\n' if fingerprint else ''
                (raw / f'shared-uncontended_W{worker}_H0_c1.txt').write_text(
                    header + '# benchtime: 100x\n'
                    f'BenchmarkSharedUncontended/W={worker}/H=0-8 100 123 ns/op 8000 cycles_per_s\nPASS\nok\tmodule\t0.1s\n')
            (raw / 'shared-uncontended_W256_H0_c1.txt').write_text(
                '# fingerprint: fresh\n# benchtime: 100x\n'
                'BenchmarkSharedUncontended/W=256/H=0-8 100 123 ns/op\nFAIL\n')
            (root / 'env.txt').write_text('- source fingerprint: fresh\n')
            args = ['report', '--raw-dir', str(raw), '--plan', str(root / 'plan.tsv'),
                    '--env-file', str(root / 'env.txt'), '--out-csv', str(root / 'out.csv'),
                    '--out-md', str(root / 'out.md')]
            with patch.object(sys, 'argv', args), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(report.main(), 0)
            csv = (root / 'out.csv').read_text()
            self.assertIn('/W=4/', csv)
            self.assertNotIn('/W=16/', csv)
            self.assertNotIn('/W=64/', csv)
            self.assertNotIn('/W=256/', csv)
            self.assertIn('INCOMPLETE-RUN', (root / 'out.md').read_text())
            self.assertEqual((root / 'out.md').read_text().count('STALE-SOURCE'), 2)
