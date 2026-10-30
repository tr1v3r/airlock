"""Regression-test the workflow gate and installation wrapper without network I/O."""
import json
import os
import pathlib
import re
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
    def invoke(self, lane, fail_at=0):
        with tempfile.TemporaryDirectory(prefix="airlock-stress-wrapper-") as temp:
            directory = pathlib.Path(temp)
            log = directory / "go.jsonl"
            fake = directory / "go"
            fake.write_text(f"#!{sys.executable}\n" + textwrap.dedent('''
                import json, os, pathlib, sys
                log = pathlib.Path(os.environ["FAKE_GO_LOG"])
                calls = log.read_text().splitlines() if log.exists() else []
                with log.open("a") as out:
                    out.write(json.dumps({"args": sys.argv[1:], "stress": os.environ["AIRLOCK_STRESS"]}) + "\\n")
                if len(calls) + 1 == int(os.environ["FAKE_GO_FAIL_AT"]):
                    sys.exit(23)
            '''))
            fake.chmod(0o755)
            env = dict(os.environ, PATH=f"{directory}{os.pathsep}{os.environ['PATH']}",
                       FAKE_GO_LOG=str(log), FAKE_GO_FAIL_AT=str(fail_at), AIRLOCK_STRESS="1")
            result = subprocess.run(["bash", str(ROOT / "scripts/stress.sh"), lane],
                                    env=env, text=True, capture_output=True, timeout=10)
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return result, calls

    def test_quick_and_extended_keep_repetition_separate_from_size_expansion(self):
        for lane, count, fuzztime in (("quick", "1", "5s"), ("extended", "10", "30s")):
            with self.subTest(lane=lane):
                result, calls = self.invoke(lane)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(len(calls), 5 if lane == "quick" else 10)
                concurrent = calls[0]
                self.assertEqual(concurrent["stress"], "0")
                for arg in ("-race", "-v", "-shuffle=on", "-count=" + count, "-cpu=1,2,4"):
                    self.assertIn(arg, concurrent["args"])
                fuzz_index = 1
                if lane == "extended":
                    expanded = calls[1]
                    self.assertEqual(expanded["stress"], "1")
                    for arg in ("-count=1", "-cpu=1", "-timeout=20m"):
                        self.assertIn(arg, expanded["args"])
                    model = calls[2]["args"]
                    self.assertIn("^TestTransitionModelTraces$", model)
                    self.assertIn("-count=1", model)
                    self.assertFalse(any(arg.startswith("-cpu") for arg in model))
                    fuzz_index = 3
                    for call in calls[-3:]:
                        self.assertIn("-count=3", call["args"])
                        self.assertIn("-cpu=1,2,4", call["args"])
                for call in calls[fuzz_index:fuzz_index + 4]:
                    self.assertIn("-fuzztime=" + fuzztime, call["args"])
                    self.assertIn("-fuzzminimizetime=1s", call["args"])
                    self.assertIn("-parallel=2", call["args"])

    def test_repeat_selection_covers_regressions_and_excludes_serial_work(self):
        result, calls = self.invoke("quick")
        self.assertEqual(result.returncode, 0, result.stderr)
        args = calls[0]["args"]
        pattern = re.compile(args[args.index("-run") + 1])
        # Representative regression names are independent of the wrapper's regex.
        for name in (
            "TestCancelGrantRaceAndDuplicateRelease", "TestAirlockMultipleSlotsAndChurn",
            "TestWeightedCancelGrantRaceAndConcurrentRelease", "TestPriorityMixedWeightConcurrentCapacity",
            "TestResizeCancellationGrantRace", "TestResizeZeroReleaseCancelRace",
            "TestResizeZeroPriorityProtectedHead",
            "TestTryAcquireMixedWithWaitingAndConcurrentRelease", "TestLocalDynamicModel",
            "TestSharedConcurrentWeightedLifecycle", "TestSharedConcurrentLeaseOperations",
            "TestSharedReleaseCancelsGateWaitAndInvalidatesInFlightRenew", "TestSharedLeaseGateWaitingIsBounded",
            "TestSharedRenewCannotResurrectExpiredLease", "TestSharedClockAllowanceOffsets",
        ):
            self.assertIsNotNone(pattern.fullmatch(name), name)
        for name in (
            "TestTransitionModelTraces", "TestTransitionPartialMatchesCompleteState",
            "TestTransitionScaleFixtures", "TestSharedBenchmarkSmoke", "TestLocalModelMutationSensitivity",
        ):
            self.assertIsNone(pattern.fullmatch(name), name)
        inventory = [name for path in ROOT.glob("*_test.go")
                     for name in re.findall(r"^func (Test\w+)\(", path.read_text(), re.MULTILINE)]
        self.assertGreater(sum(bool(pattern.fullmatch(name)) for name in inventory), 40)
        # A stale/misspelled explicit entry must not silently become a no-op.
        script = (ROOT / "scripts/stress.sh").read_text()
        groups = re.findall(r"^  '([^']+)'$", script, re.MULTILINE)
        self.assertTrue(groups)
        for group in groups:
            for alternative in group.split("|"):
                selection = re.compile("Test(?:" + alternative + ")")
                self.assertTrue(any(selection.fullmatch(name) for name in inventory), alternative)

    def test_failure_stops_later_test_phases(self):
        for lane, phases in (("quick", 5), ("extended", 10)):
            for fail_at in range(1, phases + 1):
                with self.subTest(lane=lane, phase=fail_at):
                    result, calls = self.invoke(lane, fail_at)
                    self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
                    self.assertEqual(len(calls), fail_at)

    def test_unknown_lane_runs_nothing(self):
        result, calls = self.invoke("unknown")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
