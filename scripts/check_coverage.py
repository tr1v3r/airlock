"""Enforce statement coverage for every production file and the whole SDK."""
import collections
import pathlib
import sys

THRESHOLD = 95


def check(path):
    lines = pathlib.Path(path).read_text().splitlines()
    if not lines or lines[0] != "mode: atomic":
        raise ValueError("expected an atomic Go coverage profile")
    files = collections.defaultdict(lambda: [0, 0])
    blocks = {}
    for line in lines[1:]:
        location, statements, count = line.split()
        statements, count = int(statements), int(count)
        if statements < 0 or count < 0:
            raise ValueError("negative coverage values")
        # -coverpkg can repeat blocks for different test binaries. Count each
        # statement once, covered if any test binary executes it.
        if location in blocks and blocks[location][0] != statements:
            raise ValueError("inconsistent statement count")
        blocks[location] = (statements, count > 0 or blocks.get(location, (0, False))[1])
    for location, (statements, covered) in blocks.items():
        name = location.rsplit(":", 1)[0]
        files[name][0] += statements
        files[name][1] += statements if covered else 0
    total = [sum(v[i] for v in files.values()) for i in (0, 1)]
    failed = not files or total[0] == 0
    for name, (statements, covered) in [*sorted(files.items()), ("TOTAL", total)]:
        if statements == 0:
            continue
        passed = covered * 100 >= statements * THRESHOLD
        print(f"{name}: {100 * covered / statements:.2f}% (required {THRESHOLD}%)")
        failed |= not passed
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(check(sys.argv[1]))
    except (IndexError, OSError, ValueError) as exc:
        print(f"Invalid coverage profile: {exc}", file=sys.stderr)
        sys.exit(1)
