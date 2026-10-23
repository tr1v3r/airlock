"""Exercise fixture cleanup with a fake docker; never contact a Docker daemon."""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ("postgres-failover.sh", "redis-cluster.sh", "redis-failover.sh")

# Both executables are controlled fakes. Even the Redis precompiled test binary
# is synthesized, so these tests cannot accidentally run Go or connect to a DB.
FAKE = r'''
import json, os, pathlib, signal, sys
args = sys.argv[1:]
state_path = pathlib.Path(os.environ["FAKE_STATE"])
state = json.loads(state_path.read_text()) if state_path.exists() else {"runs": 0, "owned": [], "calls": []}
kind = pathlib.Path(sys.argv[0]).name
state["calls"].append([kind, *args])
mode = os.environ["FAKE_MODE"]
def save():
    state_path.write_text(json.dumps(state))
def fail():
    # Failed creation can have stdout. It must never become an owned ID.
    print("foreign-id")
    save()
    sys.exit(23)
if kind == "go":
    if mode == "test-failure":
        fail()
    if "-o" in args:
        binary = pathlib.Path(args[args.index("-o") + 1])
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
    save()
    sys.exit(0)
if args[:2] == ["network", "create"]:
    if mode == "network-conflict":
        fail()
    state["owned"].append("network-id")
    print("network-id")
elif args and args[0] == "run":
    state["runs"] += 1
    if mode == "container-conflict-" + str(state["runs"]):
        fail()
    ident = "container-id-" + str(state["runs"])
    state["owned"].append(ident)
    print(ident)
elif args[:2] == ["network", "rm"]:
    if args[2:] != ["network-id"] or "network-id" not in state["owned"]:
        state["unsafe"] = args
        save()
        sys.exit(99)
elif args and args[0] in ("rm", "unpause", "pause", "stop"):
    # All destructive operations must target IDs obtained from successful run.
    targets = args[2:] if args[0] == "rm" else args[-1:]
    if any(t not in state["owned"] or not t.startswith("container-id-") for t in targets):
        state["unsafe"] = args
        save()
        sys.exit(99)
elif args and args[0] == "exec":
    if mode == "interrupt" and not state.get("interrupted"):
        state["interrupted"] = True
        save()
        os.kill(os.getppid(), signal.SIGTERM)
        sys.exit(0)
    if "ping" in args:
        print("PONG")
    elif "cluster" in args and "info" in args:
        print("cluster_state:ok")
    elif "replication" in args and "info" in args:
        print("master_link_status:up\nconnected_slaves:0")
    elif any("SELECT sync_state" in a for a in args):
        print("sync")
elif args and args[0] == "port":
    if mode == "partial-failure" and state["runs"] >= 2:
        fail()
    print("127.0.0.1:54321")
elif args and args[0] == "inspect":
    print("172.30.0.2")
elif args and args[0] == "logs":
    pass
else:
    state["unexpected"] = args
    save()
    sys.exit(86)
save()
'''


class FixtureOwnershipTests(unittest.TestCase):
    def check_scenario(self, script, mode):
        with tempfile.TemporaryDirectory(prefix="airlock-fixture-test-") as temp:
            directory = pathlib.Path(temp)
            for name in ("docker", "go"):
                fake = directory / name
                fake.write_text(f"#!{sys.executable}\n" + FAKE)
                fake.chmod(0o755)
            state_path = directory / "state.json"
            env = dict(os.environ, PATH=f"{directory}{os.pathsep}{os.environ['PATH']}",
                       FAKE_MODE=mode, FAKE_STATE=str(state_path), TMPDIR=temp)
            result = subprocess.run(["bash", str(ROOT / "scripts" / script)],
                                    env=env, text=True, capture_output=True, timeout=15)
            state = json.loads(state_path.read_text())
            self.assertNotIn("unsafe", state, (script, mode, state))
            self.assertNotIn("unexpected", state, (script, mode, state))
            if mode == "success":
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            else:
                self.assertNotEqual(result.returncode, 0, (script, mode))
            removed = []
            for call in state["calls"]:
                if call[:3] == ["docker", "rm", "-f"]:
                    removed.extend(call[3:])
                elif call[:3] == ["docker", "network", "rm"]:
                    removed.extend(call[3:])
            self.assertCountEqual(removed, state["owned"], (script, mode, state))
            self.assertFalse(list(directory.glob("airlock-*-failover.*")),
                             "owned temporary fixture was not removed")
            if mode == "interrupt":
                self.assertTrue(state.get("interrupted"))

    def test_cleanup_ownership_matrix(self):
        modes = ("network-conflict", "container-conflict-1", "container-conflict-2",
                 "partial-failure", "test-failure", "interrupt", "success")
        for script in SCRIPTS:
            for mode in modes:
                with self.subTest(script=script, mode=mode):
                    self.check_scenario(script, mode)
        # Cluster has one more creation boundary than primary/standby fixtures.
        self.check_scenario("redis-cluster.sh", "container-conflict-3")


if __name__ == "__main__":
    unittest.main()
