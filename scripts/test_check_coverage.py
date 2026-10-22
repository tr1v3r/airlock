"""Regression tests for fail-closed coverage enforcement."""
import contextlib
import io
import pathlib
import tempfile
import unittest

from check_coverage import check


class CoverageGateTest(unittest.TestCase):
    def result(self, body):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "coverage.out"
            path.write_text(body)
            with contextlib.redirect_stdout(io.StringIO()):
                return check(path)

    def test_boundary_and_rounding(self):
        for covered, expected in [(95, 0), (94, 1)]:
            self.assertEqual(self.result(
                f"mode: atomic\na.go:1.1,2.1 {covered} 1\na.go:3.1,4.1 {100-covered} 0\n"
            ), expected)
        self.assertEqual(self.result(
            "mode: atomic\na.go:1.1,2.1 94999 1\na.go:3.1,4.1 5001 0\n"
        ), 1)

    def test_each_file_must_pass(self):
        self.assertEqual(self.result(
            "mode: atomic\na.go:1.1,2.1 1000 1\nb.go:1.1,2.1 1 0\n"
        ), 1)

    def test_duplicate_blocks_merge_across_test_binaries(self):
        self.assertEqual(self.result(
            "mode: atomic\na.go:1.1,2.1 1 0\na.go:1.1,2.1 1 2\n"
        ), 0)

    def test_empty_or_zero_statement_profiles_fail(self):
        for profile in ["mode: atomic\n", "mode: atomic\na.go:1.1,2.1 0 1\n"]:
            self.assertEqual(self.result(profile), 1)

    def test_malformed_profiles_fail(self):
        for profile in ["", "mode: set\n", "mode: atomic\nbroken\n",
                        "mode: atomic\na.go:1.1,2.1 -1 0\n",
                        "mode: atomic\na.go:1.1,2.1 1 0\na.go:1.1,2.1 2 0\n"]:
            with self.assertRaises(ValueError):
                self.result(profile)


if __name__ == "__main__":
    unittest.main()
