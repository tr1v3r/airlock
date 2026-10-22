"""Regression-test the workflow gate and installation wrapper without network I/O."""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class GateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        cls.script = textwrap.dedent(workflow.split("python3 - <<'PYTHON'\n", 1)[1].split("          PYTHON", 1)[0])

    def gate(self, event="pull_request", fork=False, **statuses):
        results = {name: {"result": status} for name, status in statuses.items()}
        env = dict(os.environ, RESULTS=json.dumps(results), EVENT_NAME=event,
                   IS_FORK=str(fork).lower())
        return subprocess.run([sys.executable, "-c", self.script], env=env,
                              capture_output=True, text=True, timeout=5).returncode

    def test_only_exact_event_specific_skips_are_allowed(self):
        for event in ("pull_request", "push", "merge_group", "schedule", "workflow_dispatch"):
            for fork in (False, True):
                for job in ("quality", "security", "drivers", "installation", "stress", "extended"):
                    for status in ("success", "skipped", "failure", "cancelled"):
                        allowed = status == "success" or (status == "skipped" and (
                            (job == "extended" and event not in ("schedule", "workflow_dispatch")) or
                            (job == "installation" and (event == "merge_group" or (event == "pull_request" and fork)))))
                        with self.subTest(event=event, fork=fork, job=job, status=status):
                            self.assertEqual(self.gate(event, fork, **{job: status}) == 0, allowed)


FAKE_GO = r'''
import json, os, pathlib, sys
args = sys.argv[1:]
log = pathlib.Path(os.environ["FAKE_GO_LOG"])
with log.open("a") as out:
    out.write(json.dumps({"args": args, "cwd": os.getcwd(), "work": os.environ.get("GOWORK")}) + "\n")
if args[:2] == ["mod", "init"]:
    pathlib.Path("go.mod").write_text("module example.com/smoke\n")
elif args and args[0] == "get":
    pathlib.Path("versions.json").write_text(json.dumps(dict(arg.rsplit("@", 1) for arg in args[1:])))
elif args[:2] == ["list", "-m"]:
    module = args[-1]
    if "@" in module:
        version = module.rsplit("@", 1)[1]
    else:
        version = json.loads(pathlib.Path("versions.json").read_text())[module]
        if os.environ.get("FAKE_DRIFT"):
            version = "wrong-mvs-version"
    print("v0.0.0-" + version)
elif args == ["run", "."]:
    assert pathlib.Path("main.go").exists()
    assert pathlib.Path("candidate.go").exists() == (pathlib.Path.cwd().name == "candidate")
    assert "replace" not in pathlib.Path("go.mod").read_text()
elif args != ["mod", "verify"]:
    sys.exit(87)
'''


class InstallWrapperTests(unittest.TestCase):
    def invoke(self, bad_readme=None, drift=False):
        with tempfile.TemporaryDirectory(prefix="airlock-install-wrapper-") as temp:
            root = pathlib.Path(temp)
            (root / "scripts").mkdir()
            shutil.copyfile(ROOT / "scripts/install-smoke.sh", root / "scripts/install-smoke.sh")
            for name in ("README.md", "README.zh-CN.md"):
                command = "go get github.com/tr1v3r/airlock@dev"
                if name == bad_readme:
                    command = "go get github.com/tr1v3r/airlock"
                (root / name).write_text(command + "\n")
            go = root / "go"
            go.write_text(f"#!{sys.executable}\n" + FAKE_GO)
            go.chmod(0o755)
            log = root / "go.jsonl"
            env = dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}",
                       TMPDIR=temp, FAKE_GO_LOG=str(log))
            if drift:
                env["FAKE_DRIFT"] = "1"
            result = subprocess.run(["bash", str(root / "scripts/install-smoke.sh"), "candidate-sha"],
                                    env=env, text=True, capture_output=True, timeout=15)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            self.assertFalse(list(root.glob("airlock-install-smoke.*")))
            return result, calls

    def test_two_external_lanes_and_no_workspace(self):
        result, calls = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        requests = [call for call in calls if call["args"][0] == "get"]
        self.assertEqual(len(requests), 2)
        for request, version in zip(requests, ("dev", "candidate-sha")):
            self.assertEqual(len(request["args"]), 5)
            self.assertTrue(all(arg.endswith("@" + version) for arg in request["args"][1:]))
        self.assertTrue(all(call["work"] == "off" for call in calls))

    def test_each_language_must_match_the_documented_install_lane(self):
        for readme in ("README.md", "README.zh-CN.md"):
            with self.subTest(readme=readme):
                result, calls = self.invoke(bad_readme=readme)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])

    def test_mvs_drift_is_not_silently_accepted(self):
        result, _ = self.invoke(drift=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unexpected selected version", result.stderr)


if __name__ == "__main__":
    unittest.main()
