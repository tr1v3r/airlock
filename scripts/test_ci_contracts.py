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
    versions_file = pathlib.Path("versions.json")
    versions = json.loads(versions_file.read_text()) if versions_file.exists() else {}
    requested = dict(arg.rsplit("@", 1) for arg in args[1:])
    versions.update(requested)
    if os.environ.get("FAKE_BRANCH_SKEW") and pathlib.Path.cwd().name == "documented":
        core = "github.com/tr1v3r/airlock"
        if core in requested:
            assert len(requested) == 1, "combining stale core@dev and fresh driver@dev must fail"
            versions[core] = "cached-core"
        else:
            # A separate driver get upgrades core via Go's MVS requirements.
            versions[core] = "required-core"
            versions.update({module: "fresh-driver" for module in requested})
    versions_file.write_text(json.dumps(versions))
elif args == ["list", "-m", "-json", "all"]:
    versions = json.loads(pathlib.Path("versions.json").read_text())
    if os.environ.get("FAKE_BRANCH_SKEW") and pathlib.Path.cwd().name == "documented":
        assert versions["github.com/tr1v3r/airlock"] == "required-core"
    for module, version in versions.items():
        print(json.dumps({"Path": module, "Version": "v0.0.0-" + version}))
elif args[:2] == ["list", "-m"]:
    module = args[-1]
    if "@" in module:
        assert not module.endswith("@dev"), "documented lane must not re-resolve a mutable branch ref"
        version = module.rsplit("@", 1)[1]
    else:
        version = json.loads(pathlib.Path("versions.json").read_text())[module]
        if os.environ.get("FAKE_DRIFT") and pathlib.Path.cwd().name == "candidate":
            version = "wrong-mvs-version"
    print("v0.0.0-" + version)
elif args == ["run", "-mod=readonly", "."]:
    assert pathlib.Path("main.go").exists()
    assert pathlib.Path("candidate.go").exists() == (pathlib.Path.cwd().name == "candidate")
    assert "replace" not in pathlib.Path("go.mod").read_text()
    if os.environ.get("FAKE_BUILD_FAIL"):
        sys.exit(1)
elif args != ["mod", "verify"]:
    sys.exit(87)
'''


class InstallWrapperTests(unittest.TestCase):
    def invoke(self, bad_readme=None, drift=False, branch_skew=False, build_fail=False):
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
            if branch_skew:
                env["FAKE_BRANCH_SKEW"] = "1"
            if build_fail:
                env["FAKE_BUILD_FAIL"] = "1"
            result = subprocess.run(["bash", str(root / "scripts/install-smoke.sh"), "candidate-sha"],
                                    env=env, text=True, capture_output=True, timeout=15)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            self.assertFalse(list(root.glob("airlock-install-smoke.*")))
            return result, calls

    def test_two_external_lanes_and_no_workspace(self):
        result, calls = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        requests = [call for call in calls if call["args"][0] == "get"]
        self.assertEqual(len(requests), 5)
        expected_modules = ["github.com/tr1v3r/airlock" + suffix
                            for suffix in ("", "/driver/redis", "/driver/postgres", "/driver/memory")]
        self.assertEqual([request["args"] for request in requests[:4]],
                         [["get", module + "@dev"] for module in expected_modules])
        self.assertEqual(requests[4]["args"], ["get", *[module + "@candidate-sha" for module in expected_modules]])
        self.assertTrue(all(call["work"] == "off" for call in calls))
        graphs = [call for call in calls if call["args"] == ["list", "-m", "-json", "all"]]
        self.assertEqual(len(graphs), 2)

    def test_mutable_branch_cache_skew_can_upgrade_core_via_mvs(self):
        result, _ = self.invoke(branch_skew=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("required-core", result.stdout)
        self.assertIn("branch-ref proxy caches may lag", result.stdout)

    def test_build_failure_still_fails_documented_lane(self):
        result, calls = self.invoke(build_fail=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(all(pathlib.Path(call["cwd"]).name == "documented" for call in calls))

    def test_each_language_must_match_the_documented_install_lane(self):
        for readme in ("README.md", "README.zh-CN.md"):
            with self.subTest(readme=readme):
                result, calls = self.invoke(bad_readme=readme)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])

    def test_candidate_mvs_drift_is_not_silently_accepted(self):
        result, _ = self.invoke(drift=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unexpected selected version", result.stderr)


class CompatibilityMatrixTests(unittest.TestCase):
    def test_every_module_runs_on_every_toolchain(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        step = workflow.split("      - name: Test independent modules on each supported toolchain and OS\n", 1)[1].split("\n\n", 1)[0]
        self.assertNotIn("MATRIX_GO", step)
        self.assertNotIn("Skipping driver/postgres", step)
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory(prefix="airlock-compatibility-") as temp:
            root = pathlib.Path(temp)
            for module in ("memory", "postgres", "redis"):
                directory = root / "driver" / module
                directory.mkdir(parents=True)
                (directory / "go.mod").write_text("module example.com/" + module + "\n")
            log = root / "go.jsonl"
            fake = root / "go"
            fake.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''
                import json, os, pathlib, sys
                with open(os.environ["FAKE_GO_LOG"], "a") as out:
                    out.write(json.dumps({"module": pathlib.Path.cwd().name, "command": sys.argv[1]}) + "\\n")
            '''))
            fake.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}", FAKE_GO_LOG=str(log))
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", script],
                                    cwd=root, env=env, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual(calls, [{"module": module, "command": command}
                                    for module in ("memory", "postgres", "redis") for command in ("test", "vet")])


class StressBudgetTests(unittest.TestCase):
    def test_model_and_heavy_shared_are_not_multiplied_by_local_schedule(self):
        for lane, count, cpus, fuzztime, stress in (
            ("quick", "1", "1,4", "5s", "0"),
            ("extended", "3", "1,2,4", "30s", "1"),
        ):
            with self.subTest(lane=lane), tempfile.TemporaryDirectory(prefix="airlock-stress-wrapper-") as temp:
                directory = pathlib.Path(temp)
                log = directory / "go.jsonl"
                fake = directory / "go"
                fake.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''
                    import json, os, sys
                    with open(os.environ["FAKE_GO_LOG"], "a") as out:
                        out.write(json.dumps({"args": sys.argv[1:], "stress": os.environ["AIRLOCK_STRESS"]}) + "\\n")
                '''))
                fake.chmod(0o755)
                env = dict(os.environ, PATH=f"{directory}{os.pathsep}{os.environ['PATH']}", FAKE_GO_LOG=str(log))
                result = subprocess.run(["bash", str(ROOT / "scripts/stress.sh"), lane],
                                        env=env, text=True, capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                calls = [json.loads(line) for line in log.read_text().splitlines()]
                self.assertEqual(len(calls), 6 if lane == "quick" else 10)
                concurrent = calls[0]["args"]
                self.assertIn("-count=" + count, concurrent)
                self.assertIn("-cpu=" + cpus, concurrent)
                expected_pattern = "^Test(Local|SharedConcurrent|SharedClock)" if lane == "quick" else "^Test(Local|SharedClock)"
                self.assertIn(expected_pattern, concurrent)
                self.assertFalse(any("TransitionModel" in arg for arg in concurrent))
                model_index = 1
                if lane == "extended":
                    shared = calls[1]["args"]
                    self.assertIn("^TestSharedConcurrent", shared)
                    self.assertIn("-count=1", shared)
                    self.assertIn("-cpu=1", shared)
                    self.assertIn("-timeout=20m", shared)
                    self.assertFalse(any("SharedConcurrent" in arg for arg in concurrent))
                    model_index = 2
                model = calls[model_index]["args"]
                self.assertIn("^TestTransitionModelTraces$", model)
                self.assertIn("-count=1", model)
                self.assertIn("-timeout=3m", model)
                self.assertFalse(any(arg.startswith("-cpu") for arg in model))
                for call in calls[model_index + 1:model_index + 5]:
                    self.assertIn("-fuzztime=" + fuzztime, call["args"])
                    self.assertIn("-fuzzminimizetime=1s", call["args"])
                    self.assertIn("-parallel=2", call["args"])
                if lane == "extended":
                    for call in calls[-3:]:
                        self.assertIn("-count=3", call["args"])
                        self.assertIn("-cpu=1,2,4", call["args"])
                        self.assertIn("-timeout=5m", call["args"])
                self.assertTrue(all(call["stress"] == stress for call in calls))


if __name__ == "__main__":
    unittest.main()
